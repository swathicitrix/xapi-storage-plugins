#!/usr/bin/env python

import os
import sys
import time
from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.common import call
import xapi.storage.libs.poolhelper
import mmap
import signal
import pickle
import shelve
import fcntl
import struct

BLK_SIZE = 512

MSG_OK        = '\x00'
MSG_FENCE     = '\x01'
MSG_FENCE_ACK = '\x02'

WD_TIMEOUT = 60

DLMREF = "/var/run/sr-ref/dlmref"
DLMREF_LOCK = "/var/run/sr-ref/dlmref.lock"

IOCWD = 0xc0045706

def demonize():
    for fd in [0, 1, 2]:
        try:
            os.close(fd)
        except OSError:
            pass

def block_read(bd, offset):
    f = os.open(bd, os.O_RDONLY)
    os.lseek(f, offset, os.SEEK_SET)
    s = os.read(f, BLK_SIZE)
    os.close(f)
    return s[0]
    
def block_write(bd, offset, msg):
    f = os.open(bd, os.O_RDWR | os.O_DIRECT)
    os.lseek(f, offset, os.SEEK_SET)
    m = mmap.mmap(-1, BLK_SIZE)
    m[0] = msg
    os.write(f, m)
    os.close(f)

def dlm_fence_daemon(node_id):
    n = int(node_id)
    log.debug("Starting dlm_fence_daemon on node_id=%d" % n)
    wd = os.open("/dev/watchdog", os.O_WRONLY)
    def dlm_fence_daemon_signal_handler(sig, frame):
        log.debug("dlm_fence_daemon_signal_handler")
        os.write(wd, "V")
        os.close(wd)
        log.debug("dlm_fence_daemon: exiting cleanly")
        exit(0)
    signal.signal(signal.SIGUSR1, dlm_fence_daemon_signal_handler)
    demonize()
    while True:
        f = util.lock_file("SSSS", DLMREF_LOCK, "r+")
        d = shelve.open(DLMREF)
        klist = d.keys()
        for key in klist:
            bd = "/dev/" + key + "/sbd"
            ret = block_read(bd, BLK_SIZE * 2 * n)
            if ret == MSG_OK:
                pass
            elif ret == MSG_FENCE:
                log.debug("dlm_fence_daemon: MSG_FENCE")
                log.debug("dlm_fence_daemon: Settingo WD timeout to 1 second")
                s = struct.pack ("i", 1)
                fcntl.ioctl(wd, 3221509894 , s)
                log.debug("dlm_fence_daemon: writing MSG_FENCE_ACK")
                ret = block_write(bd, BLK_SIZE * ((2 * n) + 1), MSG_FENCE_ACK)
                log.debug("dlm_fence_daemon: MSG_FENCE_ACK sent")
                # force reboot here
                return
        d.close()
        util.unlock_file("SSSS", f)
        os.write(wd, "w")
        time.sleep(1)

def dlm_fence_node(node_id):
    n = int(node_id)
    log.debug("dlm_fence_node node_id=%d" % n)
    f = util.lock_file("dlm_fence_node", DLMREF_LOCK, "r+")
    d = shelve.open(DLMREF)
    klist = d.keys()
    for key in klist:
        bd = "/dev/" + key + "/sbd"
        ret = block_write(bd, BLK_SIZE * 2 * n, MSG_FENCE)
    d.close()
    util.unlock_file("dlm_fence_node", f)
    # Wait for an ACK for WD_TIMEOUT + 10 seconds or assume 
    # node has been fenced
    for i in range(1, WD_TIMEOUT + 10):
        f = util.lock_file("dlm_fence_node", DLMREF_LOCK, "r+")
        d = shelve.open(DLMREF)
        klist = d.keys()
        for key in klist:
            bd = "/dev/" + key + "/sbd"
            ret = block_read(bd, BLK_SIZE * ((2 * n) + 1))
            if ret == MSG_FENCE_ACK:
                log.debug("dlm_fence_node got MSG_FENCE_ACK for node_id=%d" % n)
                time.sleep(2)
                util.unlock_file("dlm_fence_node", f)
                exit(0)
        d.close()
        util.unlock_file("dlm_fence_node", f)
        time.sleep(1)

def dlm_fence_clear_by_id(node_id, scsi_id):
    n = int(node_id)
    bd = "/dev/" + scsi_id + "/sbd"
    log.debug("dlm_fence_clear_by_id: clearing node_id=%d, scsi_id=%s" %
              (n, scsi_id))
    ret = block_write(bd, BLK_SIZE * 2 * n, MSG_OK)
    ret = block_write(bd, BLK_SIZE * ((2 * n) + 1), MSG_OK)

def dlm_fence_daemon_stop(node_id):
    with open("/var/run/sr-ref/dlm_fence_daemon.pickle") as f:
        dlm_fence_daemon = pickle.load(f)
    dlm_fence_daemon.send_signal(signal.SIGUSR1)
    dlm_fence_daemon.wait()
    os.unlink("/var/run/sr-ref/dlm_fence_daemon.pickle")
    return

def dlm_fence_daemon_start(node_id):
    import subprocess
    args = ['/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.gfs2/fence_tool.py',
            "dlm_fence_daemon", str(node_id)]
    dlm_fence_daemon = subprocess.Popen(args)
    log.debug("dlm_fence_daemon_start: node_id=%d" % node_id)
    with open("/var/run/sr-ref/dlm_fence_daemon.pickle", 'w+') as f:
        pickle.dump(dlm_fence_daemon, f)

def dlm_fence_no_args():
    log.debug("dlm_fence_no_args")
    for line in sys.stdin:
        log.debug("dlm_fence_no_args: %s" % line)
        if line.startswith("node="):
            node_id = int(int(line[5:]) % 4096)
    dlm_fence_node(node_id)

if __name__ == "__main__":
    if len(sys.argv) == 1:
        dlm_fence_no_args()
    else:
        cmd = sys.argv[1]
        if cmd == "dlm_fence_daemon":
            dlm_fence_daemon(sys.argv[2])
        elif cmd == "dlm_fence_daemon_stop":
            dlm_fence_daemon_stop(sys.argv[2])
        elif cmd == "dlm_fence_node":
            dlm_fence_node(sys.argv[2])
        elif cmd == "dlm_fence_clear_by_id":
            dlm_fence_clear_by_id(sys.argv[2], sys.argv[3])
        elif cmd == "dlm_fence_print":
            dlm_fence_print()
                   

