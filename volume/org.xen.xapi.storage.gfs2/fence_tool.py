#!/usr/bin/env python

import os
import sys
import time
from xapi.storage import log
from xapi.storage.libs import util
from xapi.storage.common import call
import xapi.storage.libs.poolhelper
import mmap

BLK_SIZE = 512

MSG_OK        = '\x00'
MSG_FENCE     = '\x01'
MSG_FENCE_ACK = '\x02'
MSG_EXIT      = '\x03'
MSG_EXIT_ACK  = '\x04'

WD_TIMEOUT = 60

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

def dlm_fencing_daemon(node_id):
    n = int(node_id)
    log.debug("Starting dlm_fencing_daemon on node_id=%d" % n)
    wd = os.open("/dev/watchdog", os.O_WRONLY)
    while True:
        f = util.lock_file("SSSS", "/var/run/sr-ref/dlmref", "r+")
        file_content = f.readlines()
        for line in file_content:
            bd = "/dev/" + line.rstrip() + "/sbd"
            ret = block_read(bd, BLK_SIZE * 2 * n)
            if ret == MSG_OK:
                log.debug("dlm_fencing_daemon: MSG_OK")
            elif ret == MSG_FENCE:
                log.debug("dlm_fencing_daemon: MSG_FENCE")
                log.debug("dlm_fencing_daemon: suspending all GFS2 mounts")
                # suspend all gfs2 LVs
                for iscsi_id in file_content:
                    gfs2_bd = "/dev/" + line.rstrip() + "/gfs2"
                    cmd = ["/usr/sbin/dmsetup", "suspend", gfs2_bd]
                    call("dlm_fencing_daemon", cmd)
                log.debug("dlm_fencing_daemon: writing MSG_FENCE_ACK")
                ret = block_write(bd, BLK_SIZE * ((2 * n) + 1), MSG_FENCE_ACK)
                log.debug("dlm_fencing_daemon: triggering a reboot")
                # force reboot here
                return
            elif ret == MSG_EXIT:
                log.debug("dlm_fencing_daemon: MSG_EXIT")
                log.debug("dlm_fencing_daemon: closing WD device")
                os.write(wd, "V")
                os.close(wd)
                log.debug("dlm_fencing_daemon: writing MSG_EXIT_ACK")
                ret = block_write(bd, BLK_SIZE * ((2 * n) + 1), MSG_EXIT_ACK)
                return
        util.unlock_file("SSSS", f)
        os.write(wd, "w")
        time.sleep(1)

def dlm_fence_node(node_id):
    n = int(node_id)
    log.debug("dlm_fence_node node_id=%d" % n)
    f = util.lock_file("SSSS", "/var/run/sr-ref/dlmref", "r+")
    file_content = f.readlines()
    for line in file_content:
        bd = "/dev/" + line.rstrip() + "/sbd"
        ret = block_write(bd, BLK_SIZE * 2 * n, MSG_FENCE)
    # Wait for an ACK for WD_TIMEOUT + 10 seconds or assume 
    # node has been fenced
    for i in range(1, WD_TIMEOUT + 10):
        for line in file_content:
            bd = "/dev/" + line.rstrip() + "/sbd"
            ret = block_read(bd, BLK_SIZE * ((2 * n) + 1))
            if ret == MSG_FENCE_ACK:
                log.debug("dlm_fence_node got ACK for node_id=%d" % n)
                return
        time.sleep(1)

def dlm_fence_clear_by_id(node_id, scsi_id):
    n = int(node_id)
    bd = "/dev/" + scsi_id + "/sbd"
    log.debug("dlm_fence_clear_by_id: clearing node_id=%d, scsi_id=%s" %
              (n, scsi_id))
    ret = block_write(bd, BLK_SIZE * 2 * n, MSG_OK)
    ret = block_write(bd, BLK_SIZE * ((2 * n) + 1), MSG_OK)

def dlm_fence_daemon_stop(node_id):
    n = int(node_id)
    log.debug("dlm_fence_daemon_stop node_id=%d" % n)
    f = util.lock_file("SSSS", "/var/run/sr-ref/dlmref", "r+")
    file_content = f.readlines()
    for line in file_content:
        bd = "/dev/" + line.rstrip() + "/sbd"
        ret = block_write(bd, BLK_SIZE * 2 * n, MSG_EXIT)
    # Wait for an ACK for WD_TIMEOUT + 10 seconds or assume
    # node has been fenced
    for i in range(1, WD_TIMEOUT + 10):
        for line in file_content:
            bd = "/dev/" + line.rstrip() + "/sbd"
            ret = block_read(bd, BLK_SIZE * ((2 * n) + 1))
            if ret == MSG_EXIT_ACK:
                log.debug("dlm_fence_daemon_stop got MSG_EXIT_ACK for node_id=%d" % n)
                return
        time.sleep(1)
    

if __name__ == "__main__":
    cmd = sys.argv[1]
    if cmd == "dlm_fencing_daemon":
        dlm_fencing_daemon(sys.argv[2])
    elif cmd == "dlm_fence_daemon_stop":
        dlm_fence_daemon_stop(sys.argv[2])
    elif cmd == "dlm_fence_node":
        dlm_fence_node(sys.argv[2])
    elif cmd == "dlm_fence_clear_by_id":
        dlm_fence_clear_by_id(sys.argv[2], sys.argv[3])
                   

