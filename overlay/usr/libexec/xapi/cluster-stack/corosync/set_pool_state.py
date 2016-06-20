#!/usr/bin/python

import sys
import os
import urlparse
import get_ha_sr_mount
import shutil

if __name__ == '__main__':
    if sys.argv[1] == "invalid":
        mount = get_ha_sr_mount.get_sr_mount()
        shutil.rmtree(mount + "/.ha")
