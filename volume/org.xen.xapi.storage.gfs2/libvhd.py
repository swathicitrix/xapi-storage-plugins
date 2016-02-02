#!/usr/bin/env python

import uuid
import shelve
from xapi.storage.common import call


def create(dbg, sr, name, description, size, cb):

    _uuid = str(uuid.uuid4())
    vol_name = _uuid + ".vhd"

    opq = cb.volumeStartOperations(sr, 'w')

    vol_path = cb.volumeCreate(opq, vol_name, size)
    cb.volumeActivateLocal(opq, vol_name)

    # Calculate virtual size (round up size to nearest MiB)
    size = int(size)
    size_mib = size / 1048576
    if size % 1048576 != 0:
        size_mib = size_mib + 1
    vsize = size_mib * 1048576

    # Create the VHD
    cmd = ["/usr/bin/vhd-util", "create", "-n", vol_path,
           "-s", str(size_mib)]
    call(dbg, cmd)

    cb.volumeDeactivateLocal(opq, vol_name)

    # Fetch physical utilisation
    psize = cb.volumeGetPhysSize(opq, vol_name)

    # Save metadata
    meta_path = cb.volumeMetadataGetPath(opq, vol_name)

    cb.volumeStopOperations(opq)

    d = shelve.open(meta_path)
    meta = {
        "name": name,
        "description": description,
        "keys": {},
        "childrens": [],
        "parent": "None"
    }   

    d[vol_name] = meta
    d.close()

    return {
        "key": vol_name,
        "uuid": vol_name,
        "name": name,
        "description": description,
        "read_write": True,
        "virtual_size": vsize,
        "physical_utilisation": psize,
        "uri": ["vhd+file://" + vol_path],
        "keys": {},
    }

def set(dbg, sr, key, k, v, cb):
    set_property(dbg, sr, key, k, v, cb)

def unset(dbg, sr, key, k, cb):
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq, key)
    d = shelve.open(meta_path)
    meta = d[key]
    del meta[k]
    d[key] = meta
    d.close()

def set_name(dbg, sr, key, new_name, cb):
    set_property(dbg, sr, key, "name", new_name, cb)

def set_description(dbg, sr, key, new_description, cb):
    set_property(dbg, sr, key, "description", new_description, cb)

def set_property(dbg, sr, key, field, value, cb):
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq, key)
    d = shelve.open(meta_path)
    meta = d[key]
    meta[field] = value
    d[key] = meta
    d.close()
