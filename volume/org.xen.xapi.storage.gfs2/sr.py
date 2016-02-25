#!/usr/bin/env python

import os
import os.path
import sys
import errno
import uuid
import urlparse
import xapi.storage.api.volume
from xapi.storage.common import call
from xapi.storage import log
import time
import stat
import XenAPI
import fcntl
import json
import xcp.environ

# For a block device /a/b/c, we will mount it at <mountpoint_root>/a/b/c
mountpoint_root = "/var/run/sr-mount/"

def getSRpath(dbg, sr, check=True):
    uri = urlparse.urlparse(sr)

    if uri.scheme == 'iscsi':
        (target, iqn, lunid) = decomposeISCSIuri(dbg, uri)
        sr_path = "/dev/iscsi/%s/%s:3260/LUN%s" % (iqn, target, lunid)
    else:
        sr_path = uri.path

    if check:
        if not(os.path.isdir(sr_path)) or not(os.path.ismount(sr_path)):
            raise xapi.storage.api.volume.Sr_not_attached(sr_path)
    return sr_path

def getVOLpath(dbg, sr_path, key, check=True):
    vol_dir  = os.path.join(sr_path, key)
    vol_path = os.path.join(vol_dir, key)
    if check:
        if not(os.path.exists(vol_path)):
            raise xapi.storage.api.volume.Volume_does_not_exist(vol_path)
    return vol_path

def getFromSRMetadata(dbg, sr, key):
    value = None
    u = urlparse.urlparse(sr)
    if u.scheme == 'file':
        # Get the device path
        metapath = "%s/meta.json" % (u.path)
        log.debug("%s: metapath = %s" % (dbg, metapath))
        if os.path.exists(metapath):
            with open(metapath, "r") as fd:
                meta = json.load(fd)
                value = meta[key]
    log.debug("%s: SR metadata says '%s' -> '%s'" % (dbg, key, value))
    return value

def sanitise_name(dbg, name):
    sanitised = ""
    for c in name:
        if c == os.sep or c in [
                "<",
                ">",
                ":",
                "\"",
                "/",
                "|",
                "?",
                "*"]:
            sanitised = sanitised + "_"
        else:
            sanitised = sanitised + c
    if sanitised == "":
        sanitised = "unknown"
    return sanitised

def mount(dbg, dev_path):
    # Ensure corosync+dlm are configured and running
    inventory = xcp.environ.readInventory()
    session = XenAPI.xapi_local()
    session.xenapi.login_with_password("root", "")
    this_host = session.xenapi.host.get_by_uuid(
        inventory.get("INSTALLATION_UUID"))
    log.debug("%s: setting up corosync and dlm on this host" % (dbg))
    session.xenapi.host.call_plugin(
        this_host, "gfs2setup", "gfs2Setup", {})

    mnt_path = os.path.abspath(mountpoint_root + dev_path)
    try:
        os.makedirs(mnt_path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(mnt_path):
            pass
        else:
            raise
    if not os.path.ismount(mnt_path):
        cmd = ["/usr/sbin/modprobe", "gfs2"]
        call(dbg, cmd)

        cmd = ["/usr/bin/mount", "-t", "gfs2", "-o",
               "noatime,nodiratime", dev_path, mnt_path]
        call(dbg, cmd)
    return mnt_path

def mount_local(dbg, dev_path):
    mnt_path = os.path.abspath(mountpoint_root + dev_path)
    try:
        os.makedirs(mnt_path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(mnt_path):
            pass
        else:
            raise
    if not os.path.ismount(mnt_path):
        cmd = ["/usr/bin/mount", "-t", "gfs2", "-o",
               "noatime,nodiratime,lockproto=lock_nolock", dev_path, mnt_path]
        call(dbg, cmd)
    return mnt_path

def umount(dbg, mnt_path):
    cmd = ["/usr/bin/umount", mnt_path]
    call(dbg, cmd)

def login(dbg, target, iqn, usechap=False, username=None, password=None):
    cmd = ["/usr/sbin/iscsiadm", "-m", "discovery", "-t", "st", "-p", target]
    output = call(dbg, cmd).split('\n')[0] # FIXME: only take the first one returned. This might not always be the one we want.
    log.debug("%s: output = %s" % (dbg, output))
    portal = output.split(' ')[0]
    # FIXME: error handling

    # Provide authentication details if necessary
    if usechap:
        cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, "--portal", portal, "--op", "update", "-n", "node.session.auth.authmethod", "-v", "CHAP"]
        output = call(dbg, cmd)
        log.debug("%s: output = %s" % (dbg, output))
        cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, "--portal", portal, "--op", "update", "-n", "node.session.auth.username", "-v", username]
        output = call(dbg, cmd)
        log.debug("%s: output = %s" % (dbg, output))
        cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, "--portal", portal, "--op", "update", "-n", "node.session.auth.password", "-v", password]
        output = call(dbg, cmd)
        log.debug("%s: output = %s" % (dbg, output))

    # Log in
    cmd = ["/usr/sbin/iscsiadm", "-m", "node", "-T", iqn, "--portal", portal, "-l"]
    output = call(dbg, cmd)
    log.debug("%s: output = %s" % (dbg, output))
    # FIXME: check for success

def waitForDevice(dbg):
    # Wait for new device(s) to appear
    cmd = ["/usr/sbin/udevadm", "settle"]
    call(dbg, cmd)

    # FIXME: For some reason, udevadm settle isn't sufficient to ensure the device is present. Why not?
    time.sleep(10)

def listSessions(dbg):
    '''Return a list of (sessionid, portal, targetIQN) pairs representing logged-in iSCSI sessions.'''
    cmd = ["/usr/sbin/iscsiadm", "-m", "session"]
    output = call(dbg, cmd, error=False)  # if there are none, this command exits with rc 21
    # e.g. "tcp: [1] 10.71.153.28:3260,1 iqn.2009-01.xenrt.test:iscsi6da966ca (non-flash)"
    return [tuple([int(x.split(' ')[1].strip('[]')), x.split(' ')[2], x.split(' ')[3]]) for x in output.split('\n') if x <> '']

def findMatchingSession(dbg, target, iqn, sessions):
    for (sessionid, portal, targetiqn) in sessions:
        # FIXME: only match on target IP address and IQN for now (not target port number)
        if portal.startswith(target + ":") and targetiqn == iqn:
            return sessionid
    return None

def rescanSession(dbg, sessionid):
    cmd = ["/usr/sbin/iscsiadm", "-m", "session", "-r", str(sessionid), "--rescan"]
    output = call(dbg, cmd)
    log.debug("%s: output = '%s'" % (dbg, output))
    # FIXME: check for success

def zoneInLUN(dbg, uri):
    log.debug("%s: zoneInLUN uri=%s" % (dbg, uri))

    u = urlparse.urlparse(uri)
    if u.scheme == 'iscsi':
        log.debug("%s: u = %s" % (dbg, u))
        (target, iqn, lunid) = decomposeISCSIuri(dbg, u)
        log.debug("%s: target = '%s', iqn = '%s', lunid = '%s'" % (dbg, target, iqn, lunid))

        # If there's authentication required, the target will be of the form 'username%password@12.34.56.78'
        atindex = target.find('@')
        usechap = False
        username = None
        password = None
        if atindex >= 0:
            usechap = True
            [username, password] = target[0:atindex].split('%')
            target = target[atindex+1:]
        
        current_sessions = listSessions(dbg)
        log.debug("%s: current iSCSI sessions are %s" % (dbg, current_sessions))
        sessionid = findMatchingSession(dbg, target, iqn, current_sessions)
        if sessionid:
            # If there's an existing session, rescan it in case new LUNs have appeared in it
            log.debug("%s: rescanning session %d for %s on %s" % (dbg, sessionid, iqn, target))
            # FIXME: should do refcounting to avoid logging out on first SR.detach
            rescanSession(dbg, sessionid)
        else:
            # Otherwise, perform a fresh login
            log.debug("%s: logging into %s on %s" % (dbg, iqn, target))
            login(dbg, target, iqn, usechap, username, password)

        waitForDevice(dbg)
        dev_path = "/dev/iscsi/%s/%s:3260/LUN%s" % (iqn, target, lunid)
    else:
        # Assume it's a local block device
        dev_path = u.path

        if not(os.path.isdir(dev_path)) or not(os.path.ismount(dev_path)):
            raise xapi.storage.api.volume.Sr_not_attached(dev_path)

    # Verify it's a block device
    if not stat.S_ISBLK(os.stat(dev_path).st_mode):
        raise xapi.storage.api.volume.Unimplemented(
            "Not a block device: %s" % dev_path)

    # Switch to 'noop' scheduler
    sched_file = "/sys/block/%s/queue/scheduler" % (os.path.basename(os.readlink(dev_path)))
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

def decomposeISCSIuri(dbg, uri):
    if (uri.scheme != "iscsi" or not uri.netloc or not uri.path):
        raise xapi.storage.api.volume.SR_does_not_exist("The SR URI is invalid; please use iscsi://<target>/<targetIQN>/<lun>")

    target = uri.netloc
    [null, iqn, lunid] = uri.path.split("/")
    return (target, iqn, lunid)

class Implementation(xapi.storage.api.volume.SR_skeleton):

    def probe(self, dbg, uri):
        raise AssertionError("not implemented")

    def attach(self, dbg, uri):
        log.debug("%s: SR.attach: uri=%s" % (dbg, uri))

        # Notify other pool members we have arrived
        inventory = xcp.environ.readInventory()
        session = XenAPI.xapi_local()
        session.xenapi.login_with_password("root", "")
        this_host = session.xenapi.host.get_by_uuid(
            inventory.get("INSTALLATION_UUID"))
        # FIXME: Do not notify offline hosts
        # FIXME: See ffs.call_plugin_in_pool()
        for host in session.xenapi.host.get_all():
            if host != this_host:
                log.debug("%s: notifying host %s we have arrived" % (dbg, session.xenapi.host.get_name_label(host)))
                session.xenapi.host.call_plugin(
                    host, "gfs2setup", "gfs2Reload", {})

        # Zone in the LUN on this host
        dev_path = zoneInLUN(dbg, uri)

        # Mount the gfs2 filesystem
        mnt_path = mount(dbg, dev_path)

        log.debug("%s: mounted on %s" % (dbg, mnt_path))
        uri = "file://" + mnt_path
        return uri

    def create(self, dbg, uri, name, description, configuration):
        log.debug("%s: SR.create: uri=%s, config=%s" % (dbg, uri, configuration))

        # Fetch the pool uuid to use as cluster id
        session = XenAPI.xapi_local()
        session.xenapi.login_with_password("root", "")
        pool = session.xenapi.pool.get_all()[0]
        pool_uuid = session.xenapi.pool.get_uuid(pool)

        # Cluster id is quite limited in size
        cluster_name = pool_uuid[:8]
        # Generate a UUID for the filesystem name
        # According to mkfs.gfs2 manpage, SR name can only be 1--16 chars in length
        sr_name = str(uuid.uuid4())[0:16]
        fsname = "%s:%s" % (cluster_name, sr_name)

        # Zone-in the LUN
        dev_path = zoneInLUN(dbg, uri)
        log.debug("%s: dev_path = %s" % (dbg, dev_path))

        # Make sure we wipe any previous GFS2 metadata
        dd_dev_path = "of=" + dev_path
        cmd = ["/usr/bin/dd", "if=/dev/zero", dd_dev_path, "bs=1M", "count=10", "oflag=direct"]
        call(dbg, cmd)

        # Make the filesystem
        cmd = ["/usr/sbin/mkfs.gfs2",
               "-t", fsname,
               "-p", "lock_dlm",
               "-r", "2048",
               "-J", "128",
               "-O",
               "-j", "16",
               dev_path]
        call(dbg, cmd)

        # Temporarily mount the filesystem so we can write the SR metadata
        mnt_path = mount_local(dbg, dev_path)

        # create metadata DB
        import sqlite3
        conn = sqlite3.connect(mnt_path + "/sqlite3-metadata.db")
        conn.execute("create table VDI(key integer primary key, snap int, parent int, name text, description text)")
        conn.commit()
        conn.close()

        read_caching = True
        if 'read_caching' in configuration:
            if configuration['read_caching'] not in ['true', 't', 'on', '1', 'yes']:
                read_caching = False

        meta = {
            "name": name,
            "description": description,
            "uri": uri,
            "mountpath": mnt_path,
            "fsname": fsname,
            "read_caching": read_caching,
            "keys": {}
        }
        metapath = mnt_path + "/meta.json"
        log.debug("%s: dumping metadata to %s: %s" % (dbg, metapath, meta))

        with open(metapath, "w") as json_fp:
            json.dump(meta, json_fp)
            json_fp.write("\n")

        umount(dbg, mnt_path)

        # Don't leave it zoned-in
        zoneOutLUN(dbg, uri)
        return

    def destroy(self, dbg, sr):
        # no need to destroy anything
        return

    def detach(self, dbg, sr):
        # Get the iSCSI uri from the SR metadata
        uri = getFromSRMetadata(dbg, sr, 'uri')

        # Unmount the FS
        sr_path = getSRpath(dbg, sr)
        cmd = ["/usr/bin/umount", sr_path]
        call(dbg, cmd)

        # Log out of the iSCSI session
        zoneOutLUN(dbg, uri)

    def ls(self, dbg, sr):
        import volume
        import libvhd
        return libvhd.ls(dbg, sr, volume.gfs2BaseCallbacks())

    def stat(self, dbg, sr):
        # Get the filesystem size
        statvfs = os.statvfs(urlparse.urlparse(sr).path)
        psize = statvfs.f_blocks * statvfs.f_frsize
        fsize = statvfs.f_bfree * statvfs.f_frsize
        log.debug("%s: statvfs says psize = %Ld" % (dbg, psize))

        return {
            "sr": sr,
            "name": "SR Name",
            "description": "GFS2 SR",
            "total_space": psize,
            "free_space": fsize,
            "datasources": [],
            "clustered": True,
            "health": ["Healthy", ""]
        }

if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.volume.SR_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == 'SR.probe':
        cmd.probe()
    elif base == 'SR.attach':
        cmd.attach()
    elif base == 'SR.create':
        cmd.create()
    elif base == 'SR.destroy':
        cmd.destroy()
    elif base == 'SR.detach':
        cmd.detach()
    elif base == 'SR.ls':
        cmd.ls()
    elif base == 'SR.stat':
        cmd.stat()
    else:
        raise xapi.storage.api.volume.Unimplemented(base)
