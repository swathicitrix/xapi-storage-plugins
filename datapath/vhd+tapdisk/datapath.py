#!/usr/bin/env python

import urlparse
import os
import sys
import xapi
import xapi.storage.api.datapath
import xapi.storage.api.volume
import importlib
from xapi.storage.libs.libvhd import VHDDatapath
from xapi.storage import log


def get_sr_callbacks(dbg, uri):
    u = urlparse.urlparse(uri)
    sr = u.netloc
    sys.path.insert(0, '/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.' + sr)
    mod = importlib.import_module(sr)
    return mod.Callbacks()
    
class Implementation(xapi.storage.api.datapath.Datapath_skeleton):

    def activate(self, dbg, uri, domain):
        cb = get_sr_callbacks(dbg, uri)
        VHDDatapath.activate(dbg, uri, domain, cb)

    def attach(self, dbg, uri, domain):
        cb = get_sr_callbacks(dbg, uri)
        return VHDDatapath.attach(dbg, uri, domain, cb)

    def detach(self, dbg, uri, domain):
        cb = get_sr_callbacks(dbg, uri)
        VHDDatapath.detach(dbg, uri, domain, cb)

    def deactivate(self, dbg, uri, domain):
        cb = get_sr_callbacks(dbg, uri)
        VHDDatapath.deactivate(dbg, uri, domain, cb)

    def open(self, dbg, uri, persistent):
        cb = get_sr_callbacks(dbg, uri)
        VHDDatapath.epc_open(dbg, uri, persistent, cb)
        return None

    def close(self, dbg, uri):
        cb = get_sr_callbacks(dbg, uri)
        VHDDatapath.epc_close(dbg, uri, cb)
        return None

if __name__ == "__main__":
    try:
        log.log_call_argv()
        cmd = xapi.storage.api.datapath.Datapath_commandline(Implementation())
        base = os.path.basename(sys.argv[0])
        if base == "Datapath.activate":
            cmd.activate()
        elif base == "Datapath.attach":
            cmd.attach()
        elif base == "Datapath.close":
            cmd.close()
        elif base == "Datapath.deactivate":
            cmd.deactivate()
        elif base == "Datapath.detach":
            cmd.detach()
        elif base == "Datapath.open":
            cmd.open()
        else:
            raise xapi.storage.api.datapath.Unimplemented(base)
    except:
        log.error("datapath:vhd+tapdisk: error {}".format(sys.exc_info()))
        raise
