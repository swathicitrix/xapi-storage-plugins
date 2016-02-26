#!/usr/bin/env python

import uuid
import sqlite3
import os
from xapi.storage.common import call
from xapi.storage import log
import xapi.storage.libs.poolhelper

def create(dbg, sr, name, description, size, cb):

    # Calculate virtual size (round up size to nearest MiB)
    size = int(size)
    size_mib = size / 1048576
    if size % 1048576 != 0:
        size_mib = size_mib + 1
    vsize = size_mib * 1048576

    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    vol_uuid = str(uuid.uuid4())

    conn = sqlite3.connect(meta_path)
    res = conn.execute("insert into VDI(snap, name, description, uuid, vsize) values (?, ?, ?, ?, ?)", 
                       (0, name, description, vol_uuid, str(vsize)))
    vol_name = str(res.lastrowid)

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
        "uuid": vol_uuid,
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
    res = conn.execute("delete from VDI where rowid = (?)", (int(name),))
    conn.commit()
    conn.close()
    cb.volumeStopOperations(opq)

def clone(dbg, sr, key, cb):
    snap_uuid = str(uuid.uuid4())

    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    key_path = cb.volumeGetPath(opq, key)

    conn = sqlite3.connect(meta_path)
    res = conn.execute("select name,parent,description,uuid,vsize from VDI where rowid = (?)",
                       (int(key),)).fetchall()
    (p_name, p_parent, p_desc, p_uuid, p_vsize) = res[0]
    
    res = conn.execute("insert into VDI(snap, parent, name, description, uuid, vsize) values (?, ?, ?, ?, ?, ?)", 
                       (0, int(key), p_name, p_desc, snap_uuid, p_vsize))
    snap_name = str(res.lastrowid)
    snap_path = cb.volumeCreate(opq, snap_name, int(p_vsize))

    # Snapshot from key
    cmd = ["/usr/bin/vhd-util", "snapshot",
               "-n", snap_path, "-p", key_path]
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
        xapi.storage.libs.poolhelper.suspend_datapath_in_pool(dbg, key_path)
        res = conn.execute("insert into VDI(snap, parent) values (?, ?)",
                           (0, p_parent))
        base_name = str(res.lastrowid)
        base_path = cb.volumeRename(opq, key, base_name)
        cb.volumeCreate(opq, key, int(p_vsize))

        cmd = ["/usr/bin/vhd-util", "snapshot",
               "-n", key_path, "-p", base_path]
        output = call(dbg, cmd)

        # Finally, update the snapshot parent to the rebased volume
        cmd = ["/usr/bin/vhd-util", "modify",
               "-n", snap_path, "-p", base_path]
        output = call(dbg, cmd)
        res = conn.execute("update VDI set parent = (?) where rowid = (?)",
                           (int(base_name), int(snap_name),) )
        
        xapi.storage.libs.poolhelper.resume_datapath_in_pool(dbg, key_path)

    conn.commit()
    conn.close()
    psize = cb.volumeGetPhysSize(opq, snap_name)
    cb.volumeStopOperations(opq)

    return {
        "uuid": snap_uuid,
        "key": snap_name,
        "name": p_name,
        "description": p_desc,
        "read_write": True,
        "virtual_size": p_vsize,
        "physical_utilisation": psize,
        "uri": ["vhd+lv://" + snap_path],
        "keys": {}
    }

def stat(dbg, sr, key, cb):
    opq = cb.volumeStartOperations(sr, 'r')
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = sqlite3.connect(meta_path)
    res = conn.execute("select name,description,uuid,vsize from VDI where rowid = (?)", 
                       (int(key),)).fetchall()
    conn.commit()
    conn.close()

    (name,desc,uuid,vsize) = res[0]
    psize = cb.volumeGetPhysSize(opq, key)
    cb.volumeStopOperations(opq)

    return {
        "uuid": uuid,
        "key": key,
        "name": name,
        "description": desc,
        "read_write": True,
        "virtual_size": vsize,
        "physical_utilisation": psize,
        "uri": ["vhd+file://" + cb.volumeGetPath(opq, key)],
        "keys": {}
    }

def ls(dbg, sr, cb):
    results = []    
    opq = cb.volumeStartOperations(sr, 'r')
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = sqlite3.connect(meta_path)

    res = conn.execute("select key,name,description,uuid,vsize from VDI where key not in (select parent from VDI where parent NOT NULL group by parent)").fetchall()
    
    for (key_int,name,desc,uuid,vsize) in res:
        key = str(key_int)
        psize = cb.volumeGetPhysSize(opq, key)
        results.append({
                "uuid": uuid,
                "key": key,
                "name": name,
                "description": desc,
                "read_write": True,
                "virtual_size": vsize,
                "physical_utilisation": psize,
                "uri": ["vhd+file://" + cb.volumeGetPath(opq, key)],
                "keys": {}
        })

    return results

def set(dbg, sr, key, k, v, cb):
    return

def unset(dbg, sr, key, k, cb):
    return

def set_name(dbg, sr, key, new_name, cb):
    set_property(dbg, sr, key, "name", new_name, cb)

def set_description(dbg, sr, key, new_description, cb):
    set_property(dbg, sr, key, "description", new_description, cb)

def set_property(dbg, sr, key, field, value, cb):
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    
    conn = sqlite3.connect(meta_path)
    query = ("update VDI set %s = (?) where rowid = (?)" % field)
    res = conn.execute(query, (value, int(key),) )

    conn.commit()
    conn.close()
    cb.volumeStopOperations(opq)
