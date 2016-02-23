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

mountpoint_root = "/var/run/sr-mount/"

class Implementation(xapi.storage.api.volume.SR_skeleton):

    def probe(self, dbg, uri):
        raise AssertionError("not implemented")

    def attach(self, dbg, uri):
        url = urlparse.urlparse(uri)
        dev_path = url.path
        mnt_path = os.path.abspath(mountpoint_root + dev_path)
        try:
            os.makedirs(mnt_path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(mnt_path):
                pass
            else:
                raise

        # Mount the gfs2 filesystem
        cmd = ["/usr/bin/mount", "-t", "gfs2", "-o",
               "noatime,nodiratime,lockproto=lock_nolock", dev_path, mnt_path]
        call(dbg, cmd)

        log.debug("%s: mounted on %s" % (dbg, mnt_path))
        uri = "file://" + mnt_path
        return uri

    def create(self, dbg, uri, name, description, configuration):
        url = urlparse.urlparse(uri)
        # Make the filesystem
        dev_path = url.path
        dd_dev_path = "of=" + dev_path

        cmd = ["/usr/bin/dd", "if=/dev/zero", dd_dev_path, "bs=1M", "count=10", "oflag=direct"]
        call(dbg, cmd)

        cmd = ["/usr/sbin/mkfs.gfs2", "-O", "-p", "lock_nolock",
            "-r", "2048",
            "-J", "256",
            "-j", "16", dev_path]
        call(dbg, cmd)
        return

    def destroy(self, dbg, sr):
        # no need to destroy anything
        return

    def detach(self, dbg, sr):
        # Unmount the FS
        url = urlparse.urlparse(sr)
        cmd = ["/usr/bin/umount", url.path]
        call(dbg, cmd)
        return

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
