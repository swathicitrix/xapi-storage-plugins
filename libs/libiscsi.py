#!/usr/bin/env python

import uuid
import sqlite3
import os
import time
import urlparse
import stat
from xapi.storage.common import call
from xapi.storage import log
import xapi.storage.libs.poolhelper
import xcp.environ
import XenAPI

def login(dbg, target, iqn, usechap=False, username=None, password=None):
    cmd = ["/usr/sbin/iscsiadm", "-m", "discovery", "-t", "st", "-p", target]
    output = call(dbg, cmd).split('\n')[0] 
    # FIXME: only take the first one returned. 
    # This might not always be the one we want.
    log.debug("%s: output = %s" % (dbg, output))
    portal = output.split(' ')[0]
    # FIXME: error handling

    # Provide authentication details if necessary
    if usechap:
        cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, "--portal", 
               portal, "--op", "update", "-n", "node.session.auth.authmethod", 
               "-v", "CHAP"]
        output = call(dbg, cmd)
        log.debug("%s: output = %s" % (dbg, output))
        cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, "--portal", 
               portal, "--op", "update", "-n", "node.session.auth.username", 
               "-v", username]
        output = call(dbg, cmd)
        log.debug("%s: output = %s" % (dbg, output))
        cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, "--portal", 
               portal, "--op", "update", "-n", "node.session.auth.password", 
               "-v", password]
        output = call(dbg, cmd)
        log.debug("%s: output = %s" % (dbg, output))

    # Log in
    cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, 
           "--portal", portal, "-l"]
    output = call(dbg, cmd)
    log.debug("%s: output = %s" % (dbg, output))
    # FIXME: check for success

def waitForDevice(dbg):
    # Wait for new device(s) to appear
    cmd = ["/usr/sbin/udevadm", "settle"]
    call(dbg, cmd)

    # FIXME: For some reason, udevadm settle isn't sufficient 
    # to ensure the device is present. Why not?
    time.sleep(10)

def listSessions(dbg):
    '''Return a list of (sessionid, portal, targetIQN) pairs 
       representing logged-in iSCSI sessions.'''
    cmd = ["/usr/sbin/iscsiadm", "-m", "session"]
    output = call(dbg, cmd, error=False)  
    # if there are none, this command exits with rc 21
    # e.g. "tcp: [1] 10.71.153.28:3260,1 iqn.2009-01.xenrt.test:iscsi6da966ca 
    # (non-flash)"
    return [tuple([int(x.split(' ')[1].strip('[]')), x.split(' ')[2], 
            x.split(' ')[3]]) for x in output.split('\n') if x <> '']

def findMatchingSession(dbg, target, iqn, sessions):
    for (sessionid, portal, targetiqn) in sessions:
        # FIXME: only match on target IP address and IQN for now 
        # (not target port number)
        if portal.startswith(target + ":") and targetiqn == iqn:
            return sessionid
    return None

def rescanSession(dbg, sessionid):
    cmd = ["/usr/sbin/iscsiadm", "-m", "session", "-r", str(sessionid), 
           "--rescan"]
    output = call(dbg, cmd)
    log.debug("%s: output = '%s'" % (dbg, output))
    # FIXME: check for success

def getDesiredInitiatorName(dbg):
    # FIXME: for now, get this from xapi. In future, xapi will 
    # write this to a file we can read from.
    inventory = xcp.environ.readInventory()
    session = XenAPI.xapi_local()
    session.xenapi.login_with_password("root", "")
    this_host = session.xenapi.host.get_by_uuid(
                inventory.get("INSTALLATION_UUID"))
    return session.xenapi.host.get_other_config(this_host)['iscsi_iqn']

def setInitiatorName(dbg, iqn):
    with open('/etc/iscsi/initiatorname.iscsi', "w") as fd:
        fd.write('InitiatorName=%s\n' % (iqn))

def getCurrentInitiatorName(dbg):
    with open('/etc/iscsi/initiatorname.iscsi', "r") as fd:
        lines = fd.readlines()
        for line in lines:
            if not line.strip().startswith("#") and "InitiatorName" in line:
               return line.split('=')[1].strip()


def restartISCSIDaemon(dbg):
    cmd = ["/usr/bin/systemctl", "restart", "iscsid"]
    call(dbg, cmd)
 
def isISCSIDaemonRunning(dbg):
    cmd = ["/usr/bin/systemctl", "status", "iscsid"]
    (stdout, stderr, rc) = call(dbg, cmd, error=False, simple=False)
    return rc == 0
 
def configureISCSIDaemon(dbg):
    # Find out what the user wants the IQN to be
    iqn = getDesiredInitiatorName(dbg)
 
    # Make that the IQN, if possible
    if not isISCSIDaemonRunning(dbg):
        setInitiatorName(dbg, iqn)
        restartISCSIDaemon(dbg)
    else:
        cur_iqn = getCurrentInitiatorName(dbg)
        if iqn != cur_iqn:
            if len(listSessions(dbg)) > 0:
                raise xapi.storage.api.volume.Unimplemented(
                      "Daemon running with sessions from IQN '%s', "
                      "desired IQN '%s'" % (cur_iqn, iqn))
            else:
                setInitiatorName(dbg, iqn)
                restartISCSIDaemon(dbg)

def decomposeISCSIuri(dbg, uri):
    if (uri.scheme != "iscsi" or not uri.netloc or not uri.path):
        raise xapi.storage.api.volume.SR_does_not_exist(
              "The SR URI is invalid; please use \
               iscsi://<target>/<targetIQN>/<lun>")

    target = uri.netloc
    [null, iqn, lunid] = uri.path.split("/")
    return (target, iqn, lunid)

def zoneInLUN(dbg, uri):
    log.debug("%s: zoneInLUN uri=%s" % (dbg, uri))

    u = urlparse.urlparse(uri)
    if u.scheme == 'iscsi':
        log.debug("%s: u = %s" % (dbg, u))
        (target, iqn, lunid) = decomposeISCSIuri(dbg, u)
        log.debug("%s: target = '%s', iqn = '%s', lunid = '%s'" % 
                  (dbg, target, iqn, lunid))

        # If there's authentication required, the target will be i
        # of the form 'username%password@12.34.56.78'
        atindex = target.find('@')
        usechap = False
        username = None
        password = None
        if atindex >= 0:
            usechap = True
            [username, password] = target[0:atindex].split('%')
            target = target[atindex+1:]

        configureISCSIDaemon(dbg)

        current_sessions = listSessions(dbg)
        log.debug("%s: current iSCSI sessions are %s" % (dbg, current_sessions))
        sessionid = findMatchingSession(dbg, target, iqn, current_sessions)
        if sessionid:
            # If there's an existing session, rescan it 
            # in case new LUNs have appeared in it
            log.debug("%s: rescanning session %d for %s on %s" % 
                      (dbg, sessionid, iqn, target))
            # FIXME: should do refcounting to avoid logging out on 
            # first SR.detach
            rescanSession(dbg, sessionid)
        else:
            # Otherwise, perform a fresh login
            log.debug("%s: logging into %s on %s" % (dbg, iqn, target))
            login(dbg, target, iqn, usechap, username, password)

        waitForDevice(dbg)
        dev_path = "/dev/iscsi/%s/%s:3260/LUN%s" % (iqn, target, lunid)
    else:
        # FIXME: raise some sort of exception
        raise xapi.storage.api.volume.Unimplemented(
            "Not an iSCSI LUN: %s" % uri)

    # Verify it's a block device
    if not stat.S_ISBLK(os.stat(dev_path).st_mode):
        raise xapi.storage.api.volume.Unimplemented(
            "Not a block device: %s" % dev_path)

    # Switch to 'noop' scheduler
    sched_file = "/sys/block/%s/queue/scheduler" % (
                 os.path.basename(os.readlink(dev_path)))
    with open(sched_file, "w") as fd:
        fd.write("noop\n")

    return dev_path

def zoneOutLUN(dbg, uri):
    log.debug("%s: zoneOutLUN uri=%s" % (dbg, uri))

    u = urlparse.urlparse(uri)
    log.debug("%s: u = %s" % (dbg, u))
    if u.scheme == 'iscsi':
        (target, iqn, lunid) = decomposeISCSIuri(dbg, u)
        log.debug("%s: iqn = %s" % (dbg, iqn))

        cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, "-u"]
        call(dbg, cmd)
