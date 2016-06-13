from xapi.storage.libs.util import call
from xapi.storage.libs import log

MSIZE_MB = 2 * 1024 * 1024
OPT_LOG_ERR = "--debug"

VHD_UTIL_BIN = '/usr/bin/vhd-util'

def __num_bits(val):
    count = 0
    while val:
        count += val & 1
        val = val >> 1
    return count

def __count_bits(bitmap):
    count = 0
    for i in range(len(bitmap)):
        count += __num_bits(ord(bitmap[i]))
    return count

def is_empty(dbg, vol_path):
    cmd = [VHD_UTIL_BIN, "read", OPT_LOG_ERR, "-B", "-n", vol_path]
    ret = call(dbg, cmd)
    return __count_bits(ret) == 0

def create(dbg, vol_path, size_mib):
    cmd = [VHD_UTIL_BIN, "create", "-n", vol_path,
           "-s", str(size_mib), "-S", str(MSIZE_MB)]
    return call(dbg, cmd)

def resize(dbg, vol_path, size_mib):
    cmd = [VHD_UTIL_BIN, "resize", "-n", vol_path,
           "-s", str(size_mib), "-f"]
    return call(dbg, cmd)

def reset(dbg, vol_path):
    "zero out the disk (kill all data inside the VHD file)"
    cmd = [VHD_UTIL_BIN, "modify", OPT_LOG_ERR, "-z", "-n", vol_path]
    return call(dbg, cmd)

def snapshot(dbg, vol_path, snap_path):
    cmd = [VHD_UTIL_BIN, "snapshot",
           "-n", snap_path, "-p", vol_path, "-S", str(MSIZE_MB)]
    return call(dbg, cmd)

def get_parent(dbg, vol_path):
    cmd = [VHD_UTIL_BIN, "query", "-n", vol_path, "-p"]
    return call(dbg, cmd).rstrip()

def get_vsize(dbg, vol_path):
    # vsize is returned in MB but we want to return bytes
    cmd = [VHD_UTIL_BIN, "query", "-n", vol_path, "-v"]
    out = call(dbg, cmd).rstrip()
    return int(out) * 1024 * 1024

def get_psize(dbg, vol_path):
    cmd = [VHD_UTIL_BIN, "query", "-n", vol_path, "-s"]
    return call(dbg, cmd).rstrip()

def set_parent(dbg, vol_path, parent_path):
    cmd = [VHD_UTIL_BIN, "modify",
           "-n", vol_path, "-p", parent_path]
    return call(dbg, cmd)

def is_parent_pointing_to_path(dbg, vol_path, parent_path):
    stdout = get_parent(dbg, vol_path)
    path = stdout.rstrip()
    log.debug("is_parent_pointing_to_path %s %s" % (parent_path, path))
    return parent_path[-12:] == path[-12:]
