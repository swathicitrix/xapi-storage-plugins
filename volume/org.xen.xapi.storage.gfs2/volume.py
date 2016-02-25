#!/usr/bin/env python

import urlparse
import errno
import os
import sys
import xapi.storage.api.volume
from xapi.storage import log
from xapi.storage.libs import libvhd

class gfs2BaseCallbacks():
    def volumeCreate(self, opq, name, size):
        vol_dir = os.path.join(opq, name)
        vol_path = os.path.join(vol_dir, name)
        os.makedirs(vol_dir, mode=0755)
        open(vol_path, 'a').close()
        return vol_path
    def volumeDestroy(self, opq, name):
        vol_dir = os.path.join(opq, name)
        vol_path = os.path.join(vol_dir, name)
        try:
            os.unlink(vol_path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                pass
            else:
                raise
        try:
            os.rmdir(vol_dir)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                pass
            else:
                raise
    def volumeGetPath(self, opq, name):
        return os.path.join(opq, name, name)
    def volumeActivateLocal(self, opq, name):
        pass
    def volumeDeactivateLocal(self, opq, name):
        pass
    def volumeRename(self, opq, old_name, new_name):
        os.rename(os.path.join(opq, old_name),
                  os.path.join(opq, new_name))
        os.rename(os.path.join(opq, new_name, old_name),
                  os.path.join(opq, new_name, new_name))
        return os.path.join(opq, new_name, new_name)
    def volumeResize(self, opq, name, new_size):
        pass
    def volumeGetPhysSize(self, opq, name):
        stat = os.stat(os.path.join(opq, name, name))
        return stat.st_blocks * 512
    def volumeStartOperations(self, sr, mode):
        return urlparse.urlparse(sr).path
        import sr
        opq = sr.getSRpath("dbg", sr)
        return opq
    def volumeStopOperations(self, opq):
        pass
    def volumeMetadataGetPath(self, opq):
        return os.path.join(opq, "sqlite3-metadata.db")
        

class Implementation(xapi.storage.api.volume.Volume_skeleton):

    def clone(self, dbg, sr, key):
        return libvhd.clone(dbg, sr, key, gfs2BaseCallbacks())

    def snapshot(self, dbg, sr, key):
        return libvhd.clone(dbg, sr, key, gfs2BaseCallbacks())

    def create(self, dbg, sr, name, description, size):
        return libvhd.create(dbg, sr, name, description, size, 
                             gfs2BaseCallbacks())

    def destroy(self, dbg, sr, key):
        return libvhd.destroy(dbg, sr, key, gfs2BaseCallbacks())

    def resize(self, dbg, sr, key, new_size):
        return libvhd.destroy(dbg, sr, key, new_size, 
                              gfs2BaseCallbacks())

    def set(self, dbg, sr, key, k, v):
        libvhd.set(dbg, sr, key, k, v, gfs2BaseCallbacks())

    def unset(self, dbg, sr, key, k):
        libvhd.unset(dbg, sr, key, k, gfs2BaseCallbacks())

    def set_description(self, dbg, sr, key, new_description):
        libvhd.set_description(dbg, sr, key, new_description,
                               gfs2BaseCallbacks())

    def set_name(self, dbg, sr, key, new_name):
        libvhd.set_name(dbg, sr, key, new_name, gfs2BaseCallbacks())

    def stat(self, dbg, sr, key):
        return libvhd.stat(dbg, sr, key, gfs2BaseCallbacks())

if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.volume.Volume_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.create":
        cmd.create()
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.resize":
        cmd.resize()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.set_description":
        cmd.set_description()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.snapshot":
        cmd.snapshot()
    elif base == "Volume.stat":
        cmd.stat()
    elif base == "Volume.unset":
        cmd.unset()
    else:
        raise xapi.storage.api.volume.Unimplemented(base)
