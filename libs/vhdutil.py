from xapi.storage.libs.util import call
from xapi.storage.libs import log

MSIZE_MB = 2 * 1024 * 1024
OPT_LOG_ERR = "--debug"

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
    cmd = ["/usr/bin/vhd-util", "read", OPT_LOG_ERR, "-B", "-n", vol_path]
    ret = call(dbg, cmd)
    return __count_bits(ret) == 0

def create(dbg, vol_path, size_mib):
    cmd = ["/usr/bin/vhd-util", "create", "-n", vol_path,
           "-s", str(size_mib), "-S", str(MSIZE_MB)]
    return call(dbg, cmd)

def resize(dbg, vol_path, size_mib):
    cmd = ["/usr/bin/vhd-util", "resize", "-n", vol_path,
           "-s", str(size_mib), "-f"]
    return call(dbg, cmd)

def reset(dbg, vol_path):
    "zero out the disk (kill all data inside the VHD file)"
    cmd = ["/usr/bin/vhd-util", "modify", OPT_LOG_ERR, "-z", "-n", vol_path]
    return call(dbg, cmd)

def snapshot(dbg, vol_path, snap_path):
    cmd = ["/usr/bin/vhd-util", "snapshot",
           "-n", snap_path, "-p", vol_path, "-S", str(MSIZE_MB)]
    return call(dbg, cmd)

def get_parent(dbg, vol_path):
    cmd = ["/usr/bin/vhd-util", "query", "-n", vol_path, "-p"]
    return call(dbg, cmd)

def set_parent(dbg, vol_path, parent_path):
    cmd = ["/usr/bin/vhd-util", "modify",
           "-n", vol_path, "-p", parent_path]
    return call(dbg, cmd)
