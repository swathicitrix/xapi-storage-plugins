#!/usr/bin/env python

import uuid
import sqlite3
import os
from xapi.storage.common import call


def create(dbg, sr, name, description, size, cb):

    # Calculate virtual size (round up size to nearest MiB)
    size = int(size)
    size_mib = size / 1048576
    if size % 1048576 != 0:
        size_mib = size_mib + 1
    vsize = size_mib * 1048576

    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = sqlite3.connect(meta_path)
    res = conn.execute("insert into VOLUMES(snap, name, description) values (?, ?, ?)", 
                       (0, name, description))
    vol_name = res.lastrowid

    vol_path = cb.volumeCreate(opq, vol_name, size)
    cb.volumeActivateLocal(opq, vol_name)

    # Create the VHD
    cmd = ["/usr/bin/vhd-util", "create", "-n", vol_path,
           "-s", str(size_mib)]
    call(dbg, cmd)

    cb.volumeDeactivateLocal(opq, vol_name)

    # Fetch physical utilisation
    psize = cb.volumeGetPhysSize(opq, vol_name)

    # Save metadata

    cb.volumeStopOperations(opq)

    conn.commit()
    conn.close()

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

def destroy(dbg, sr, name, cb):
    opq = cb.volumeStartOperations(sr, 'w')
    cb.volumeDestroy(opq, name)
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = sqlite3.connect(meta_path)
    res = conn.execute("delete from VOLUMES where rowid = (?)", (int(name),))
    conn.commit()
    conn.close()

def clone(dbg, sr, key, cb):
    snap_name = str(uuid.uuid4()) + ".vhd"
    base_name = str(uuid.uuid4()) + ".vhd"

    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)

    d = shelve.open(meta_path)
    meta_parent = d[str(key)]
    snap_path = cb.volumeCreate(opq, snap_name, meta_parent["vsize"])

    # Snapshot from key
    cmd = ["/usr/bin/vhd-util", "snapshot",
               "-n", snap_path, "-p", cb.volumeGetPath(opq, key)]
    output = call(dbg, cmd)
    
    # NB. As an optimisation, "vhd-util snapshot A->B" will check if
    #     "A" is empty. If it is, it will set "B.parent" to "A.parent"
    #     instead of "A" (provided "A" has a parent) and we are done.
    #     If "B.parent" still points to "A", we need to rebase "A".
    
    # Fetch the parent of the newly created snapshot
    cmd = ["/usr/bin/vhd-util", "query", "-n", snap_path, "-p"]
    stdout = call(dbg, cmd)
    parent_key = os.path.basename(stdout.rstrip())

    if parent_key[-12:] == key[-12:]:
        log.debug("%s: Volume.snapshot: parent_key == key" % (dbg))

        base_path = cb.volumeRename(opq, key, base_name)
        cb.volumeCreate(opq, key, size)
        cmd = ["/usr/bin/vhd-util", "snapshot",
               "-n", key, "-p", base_path]
        output = call(dbg, cmd)

        # Finally, update the snapshot parent to the rebased volume
        cmd = ["/usr/bin/vhd-util", "modify",
               "-n", snap_path, "-p", base_path]
        output = call(dbg, cmd)

        meta_base = {
            "name": "",
            "description": "",
            "vsize": "",
            "keys": {},
            "childrens": [snap_name, key],
            "parent": meta_parent["parent"]
        }
        d[base_name] = meta_base

    d[snap_name] = meta_parent 
    d.close()

    psize = cb.volumeGetPhysSize(opq, snap_name)

    return {
        "uuid": snap_name,
        "key": snap_name,
        "name": meta_parent["name"],
        "description": meta_parent["description"],
        "read_write": True,
        "virtual_size": meta_parent["vsize"],
        "physical_utilisation": psize,
        "uri": ["vhd+lv://" + snap_path],
        "keys": meta_parent["keys"]
    }

def stat(dbg, sr, key, cb):
    opq = cb.volumeStartOperations(sr, 'r')
    meta_path = cb.volumeMetadataGetPath(opq)
    d = shelve.open(meta_path)
    meta = d[str(key)]
    d.close()
    psize = cb.volumeGetPhysSize(opq, key)
    return {
        "uuid": key,
        "key": key,
        "name": meta["name"],
        "description": meta["description"],
        "read_write": True,
        "virtual_size": meta["vsize"],
        "physical_utilisation": psize,
        "uri": ["vhd+file://" + cb.volumeGetPath(opq, key)],
        "keys": meta["keys"]
    }

def ls(dbg, sr, cb):
    results = []    
    opq = cb.volumeStartOperations(sr, 'r')
    meta_path = cb.volumeMetadataGetPath(opq)
    d = shelve.open(meta_path)
    klist = d.keys()

    for key in klist:
        meta = d[str(key)]
        # We do not want to report non-leaf nodes
        if meta["childrens"]:
            continue
        psize = cb.volumeGetPhysSize(opq, key)
        results.append({
                "uuid": key,
                "key": key,
                "name": meta["name"],
                "description": meta["description"],
                "read_write": True,
                "virtual_size": meta["vsize"],
                "physical_utilisation": psize,
                "uri": ["vhd+file://" + cb.volumeGetPath(opq, key)],
                "keys": meta["keys"]
        })

    d.close()
    return results

def set(dbg, sr, key, k, v, cb):
    set_property(dbg, sr, key, k, v, cb)

def unset(dbg, sr, key, k, cb):
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    d = shelve.open(meta_path)
    meta = d[str(key)]
    del meta[k]
    d[str(key)] = meta
    d.close()

def set_name(dbg, sr, key, new_name, cb):
    set_property(dbg, sr, key, "name", new_name, cb)

def set_description(dbg, sr, key, new_description, cb):
    set_property(dbg, sr, key, "description", new_description, cb)

def set_property(dbg, sr, key, field, value, cb):
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    d = shelve.open(meta_path)
    meta = d[str(key)]
    meta[field] = value
    d[str(key)] = meta
    d.close()
