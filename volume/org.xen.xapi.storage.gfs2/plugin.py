#!/usr/bin/env python

import os
import sys
import xapi.storage.api.plugin
from xapi.storage import log

class Implementation(xapi.storage.api.plugin.Plugin_skeleton):

    def diagnostics(self, dbg):
        return "No diagnostic data to report"

    def query(self, dbg):
        return {
            "plugin": "gfs2",
            "name": "GFS2 filesystem Volume plugin",
            "description": ("This plugin creates a GFS2 filesystem SR on a "
                            "shared block device"),
            "vendor": "None",
            "copyright": "(C) 2015 Citrix Inc",
            "version": "3.0",
            "required_api_version": "3.0",
            "features": [
                "SR_ATTACH",
                "SR_DETACH",
                "SR_CREATE",
                "VDI_CREATE",
                "VDI_DESTROY",
                "VDI_ATTACH",
                "VDI_ATTACH_OFFLINE",
                "VDI_DETACH",
                "VDI_ACTIVATE",
                "VDI_DEACTIVATE",
                "VDI_UPDATE",
                "VDI_CLONE",
                "VDI_SNAPSHOT",
                "VDI_RESIZE",
                "SR_METADATA"],
            "configuration": {},
            "required_cluster_stack": ['corosync']}

if __name__ == "__main__":
    try:
        log.log_call_argv()
        cmd = xapi.storage.api.plugin.Plugin_commandline(Implementation())
        base = os.path.basename(sys.argv[0])
        if base == 'Plugin.diagnostics':
            cmd.diagnostics()
        elif base == 'Plugin.Query':
            cmd.query()
        else:
            raise xapi.storage.api.plugin.Unimplemented(base)
    except:
        log.error("gfs2 volume plugin: error {}".format(sys.exc_info()))
        raise
