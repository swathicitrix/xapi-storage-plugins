#!/usr/bin/env python

import os
import sys
import xapi.storage.api.volume

from xapi.storage import log
from xapi.storage.libs.libvhd import VHDVolume

class gfs2BaseCallbacks(libvhd.BaseCallbacks):
    def volumeCreate(sr, name, size):
        return vol_path
    def volumeDestroy(sr, name):
        return None
    def volumeActivateLocal(opaque, sr, name):
        return None
    def volumeDeactivateLocal(opaque, sr, name):
        return None
    def volumeRename(opaque, sr, old_name, new_name):
        return vol_path
    def volumeResize(opaque, sr, name, new_size):
        return new_size
    def volumeGetPhysSize(opaque, sr, name):
        return phys_size
    def volumeStartOperations(sr, mode):
        return opaque
    def volumeStopOperations(opaque):
        return None
    def volumeMetadataGetPath(opaque, key):
        return None

class Implementation(xapi.storage.api.volume.Volume_skeleton):

    def clone(self, dbg, sr, key):
        return VHDVolume.clone(dbg, sr, key, gfs2BaseCallbacks)

    def snapshot(self, dbg, sr, key):
        return VHDVolume.snapshot(dbg, sr, key, gfs2BaseCallbacks)

    def create(self, dbg, sr, name, description, size):
        return VHDVolume.create(
            dbg,
            sr,
            name,
            description,
            size,
            gfs2BaseCallbacks
        )

    def destroy(self, dbg, sr, key):
        return VHDVolume.destroy(dbg, sr, key, gfs2BaseCallbacks)

    def resize(self, dbg, sr, key, new_size):
        return VHDVolume.destroy(
            dbg,
            sr,
            key,
            new_size,
            gfs2BaseCallbacks
        )

    def set(self, dbg, sr, key, k, v):
        VHDVolume.set(key, k, v, gfs2BaseCallbacks)
        return None

    def unset(self, dbg, sr, key, k):
        VHDVolume.unset(key, k, gfs2BaseCallbacks)
        return None

    def set_description(self, dbg, sr, key, new_description):
        VHDVolume.set_description(
            key,
            new_description,
            gfs2BaseCallbacks
        )
        return None

    def set_name(self, dbg, sr, key, new_name):
        VHDVolume.set_name(key, new_name, gfs2BaseCallbacks)
        return None

    def stat(self, dbg, sr, key):
        return VHDVolume.stat(key, gfs2BaseCallbacks)

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
