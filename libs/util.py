#!/usr/bin/env python

from __future__ import absolute_import
import time
from xapi.storage import log
import xml.dom.minidom
import fcntl
import errno
import subprocess

RETRY_MAX = 20 # retries
RETRY_PERIOD = 1.0 # seconds

#Opens and locks a file, returns filehandle
def try_lock_file(dbg, filename, mode='a+'):
    try:
        f = open(filename, mode)
    except:
        raise xapi.storage.api.volume.Unimplemented(
            "Couldn't open refcount file: {}".format(filename)
        )

    retries = 0

    while True:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except IOError as e:
            # raise on unrelated IOErrors
            if e.errno != errno.EAGAIN:
                raise

        if retries >= RETRY_MAX:
            raise xapi.storage.api.volume.Unimplemented(
                "Couldn't lock refcount file: {}".format(filename)
            )

        time.sleep(RETRY_PERIOD)

    return f


def lock_file(dbg, filename, mode='a+'):
    f = open(filename, mode)
    fcntl.flock(f, fcntl.LOCK_EX)
    return f


#Unlocks and closes file
def unlock_file(dbg, filehandle):
    fcntl.flock(filehandle, fcntl.LOCK_UN)
    filehandle.close()


def call(dbg, cmd_args, error=True, simple=True, exp_rc=0):
    """[call dbg cmd_args] executes [cmd_args]
    if [error] and exit code != exp_rc, log and throws a BackendError
    if [simple], returns only stdout
    """
    log.debug("{}: Running cmd {}".format(dbg, cmd_args))
    p = subprocess.Popen(
        cmd_args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        close_fds=True
    )

    stdout, stderr = p.communicate()

    if error and p.returncode != exp_rc:
        log.error(
            "{}: {} exitted with code {}: {}".format(
                dbg,
                ' '.join(cmd_args),
                p.returncode,
                stderr
            )
        )

        # TODO: FIXME: Remove dependency on Xapi.
        #raise xapi.InternalError("%s exitted with non-zero code %d: %s"
        #                         % (" ".join(cmd_args), p.returncode, stderr))

    if simple:
        return stdout
    return stdout, stderr, p.returncode

def get_current_host():
    """Gets the current host name.

    Tightly bound to xcp & XenAPI, mock out for Unit Tests
    """
    import socket
    return socket.gethostname()

    import xcp.environ
    import XenAPI

    inventory = xcp.environ.readInventory()
    session = XenAPI.xapi_local()
    session.xenapi.login_with_password("root", "")
    this_host = session.xenapi.host.get_by_uuid(inventory.get("INSTALLATION_UUID"))
    return session.xenapi.host.get_name_label(this_host)
