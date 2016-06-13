#!/bin/python

import sys
import os
import urlparse

def get_sr_mount():
    with open("/etc/xensource/static-vdis/0/volume-uri", "r") as fd:
        volume_uri = fd.read()
        return str(urlparse.urlparse(volume_uri).path.split("|")[0])

print get_sr_mount()
