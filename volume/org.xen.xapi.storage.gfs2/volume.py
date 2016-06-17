#!/usr/bin/env python

import os
import sys
import xapi.storage.api.volume
from xapi.storage import log
from xapi.storage.libs.libvhd import VHDVolume
import gfs2

class Implementation(xapi.storage.api.volume.Volume_skeleton):

    def clone(self, dbg, sr, key):
        return VHDVolume.clone(dbg, sr, key, gfs2.Callbacks())

    def snapshot(self, dbg, sr, key):
        return VHDVolume.clone(dbg, sr, key, gfs2.Callbacks())

    def create(self, dbg, sr, name, description, size):
        return VHDVolume.create(
            dbg,
            sr,
            name,
            description,
            size,
            gfs2.Callbacks()
        )

    def destroy(self, dbg, sr, key):
        return VHDVolume.destroy(dbg, sr, key, gfs2.Callbacks())

    def resize(self, dbg, sr, key, new_size):
        return VHDVolume.resize(
            dbg,
            sr,
            key,
            new_size,
            gfs2.Callbacks()
        )

    def set(self, dbg, sr, key, k, v):
        VHDVolume.set(dbg, sr, key, k, v, gfs2.Callbacks())

    def unset(self, dbg, sr, key, k):
        VHDVolume.unset(dbg, sr, key, k, gfs2.Callbacks())

    def set_description(self, dbg, sr, key, new_description):
        VHDVolume.set_description(
            dbg,
            sr,
            key,
            new_description,
            gfs2.Callbacks()
        )

    def set_name(self, dbg, sr, key, new_name):
        VHDVolume.set_name(dbg, sr, key, new_name, gfs2.Callbacks())

    def stat(self, dbg, sr, key):
        return VHDVolume.stat(dbg, sr, key, gfs2.Callbacks())

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
