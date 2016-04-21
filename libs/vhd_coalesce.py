#!/usr/bin/env python

import importlib
import os
import sys
import time
from xapi.storage import log
from xapi.storage.libs import libvhd
from xapi.storage.common import call
import xapi.storage.libs.poolhelper


def get_sr_callbacks(sr_type):
    sys.path.insert(0, '/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.' + sr_type)
    mod = importlib.import_module(sr_type)
    return mod.Callbacks()

def get_all_nodes(conn):
    results = conn.execute("select * from VDI")
    rows = results.fetchall()
    for row in rows:
        print row
    totalCount = len(rows)
    print ("Found %s total nodes" % totalCount)
    return rows

def find_non_leaf_coalesceable(conn):
    results = conn.execute("select * from (select key, parent, gc_status, count(key) as num from VDI where parent not null group by parent)t where t.num=1 and key in ( select parent from vdi where parent not null group by parent)")
    rows = results.fetchall()
    for row in rows:
        print row
    if len(rows) > 0:
        print ("Found %s non leaf coalescable nodes" % len(rows))
    return rows

def find_leaf_coalesceable(conn):
    results = conn.execute("select * from (select key, parent, gc_status, count(key) as num from VDI where parent not null group by parent)t where t.num=1 and key not in ( select parent from vdi where parent not null group by parent)")
    rows = results.fetchall()
    for row in rows:
        print row
    print ("Found %s leaf coalescable nodes" % len(rows))
    return rows

def find_leaves(key, conn, leaf_accumulator):
    leaf_results = conn.execute("select key from VDI where parent = ?", (int(key),))

    children = leaf_results.fetchall()
    # print children

    if len(children) == 0:
        # This is a leaf add it to list
        # print("Found leaf %s" % key)
        leaf_accumulator.append(key)
    else:
        for child in children:
            find_leaves(str(child[0]), conn, leaf_accumulator)

def tap_ctl_pause(key, conn, cb, opq):
    key_path = cb.volumeGetPath(opq, key)
    res = conn.execute("select active_on from VDI where key = ?", (int(key),)).fetchall()
    active_on = res[0][0]
    if active_on:
        print("SS Key %s active on %s" % (key, active_on))
        xapi.storage.libs.poolhelper.suspend_datapath_on_host("GC", active_on, key_path)

def tap_ctl_unpause(key, conn, cb, opq):
    key_path = cb.volumeGetPath(opq, key)
    res = conn.execute("select active_on from VDI where key = ?", (int(key),)).fetchall()
    active_on = res[0][0]
    if active_on:
        print("SS Key %s active on %s" % (key, active_on))
        xapi.storage.libs.poolhelper.resume_datapath_on_host("GC", active_on, key_path)

def leaf_coalesce_snapshot(key, conn, cb, opq):
    print ("leaf_coalesce_snapshot key=%s" % key)
    key_path = cb.volumeGetPath(opq, key)

    res = conn.execute("select name,parent,description,uuid,vsize from VDI where rowid = (?)",
                       (int(key),)).fetchall()
    (p_name, p_parent, p_desc, p_uuid, p_vsize) = res[0]
    
    tap_ctl_pause(key, conn, cb, opq)
    res = conn.execute("insert into VDI(snap, parent) values (?, ?)",
                       (0, p_parent))
    base_name = str(res.lastrowid)
    base_path = cb.volumeRename(opq, key, base_name)
    cb.volumeCreate(opq, key, int(p_vsize))

    cmd = ["/usr/bin/vhd-util", "snapshot",
           "-n", key_path, "-p", base_path]
    output = call("GC", cmd)
        
    res = conn.execute("update VDI set parent = (?) where rowid = (?)",
                           (int(base_name), int(key),) )
    conn.commit()

    tap_ctl_unpause(key, conn, cb, opq)

def non_leaf_coalesce(key, parent_key, conn, cb, opq):
    print ("non_leaf_coalesce key=%s, parent=%s" % (key, parent_key))
    #conn.execute("key is coalescing")
    key_path = cb.volumeGetPath(opq, key)
    parent_path = cb.volumeGetPath(opq, parent_key)

    print("Running vhd-coalesce on %s" % key)
    cmd = ["/usr/bin/vhd-util", "coalesce", "-n", key_path]
    call("GC", cmd)

    #conn.execute("key coalesced")

    with libvhd.Lock(opq, "gl", cb):
        # reparent all of the children to this node's parent
        children = conn.execute("select key from VDI where parent = (?)",
                                (int(key),)).fetchall()
        # print("List of childrens: %s" % children)
        for child in children:
            child_key = str(child[0])
            child_path = cb.volumeGetPath(opq, child_key)
            res = conn.execute("update VDI set parent = (?) where rowid = (?)",
                               (parent_key, child_key,) )

            #conn.execute("child is being reparented to parent")

            # pause all leafs having child as an ancestor
            leaves = []
            find_leaves(child_key, conn, leaves)
            print("Children %s: pausing all leafes: %s" % (child_key,leaves))
            for leaf in leaves:
                tap_ctl_pause(leaf, conn, cb, opq)

            # reparent child to grandparent
            print("Reparenting %s to %s" % (child_key, parent_key))
            cmd = ["/usr/bin/vhd-util", "modify", "-n", child_path, "-p", parent_path]
            call("GC", cmd)

            # unpause all leafs having child as an ancestor
            print("Children %s: unpausing all leafes: %s" % (child_key,leaves))
            for leaf in leaves:
                tap_ctl_unpause(leaf, conn, cb, opq)

        # remove key
        print("Destroy %s" % key)
        cb.volumeDestroy(opq, key)
        res = conn.execute("delete from VDI where rowid = (?)", (int(key),))

        print("Setting gc_status to None for %s" % parent_key)
        conn.execute("update VDI set gc_status = (?) where rowid = (?)",
                     (None, parent_key,) )

        conn.commit()

def sync_leaf_coalesce(key, parent_key, conn, cb, opq):
    print ("leaf_coalesce_snapshot key=%s" % key)
    key_path = cb.volumeGetPath(opq, key)
    parent_path = cb.volumeGetPath(opq, parent_key)

    res = conn.execute("select parent from VDI where rowid = (?)",
                       (int(parent_key),)).fetchall()
    p_parent = res[0][0]
    print p_parent
    if p_parent:
        p_parent = int(p_parent)
    else:
        p_parent = "?"
    

    tap_ctl_pause(key, conn, cb, opq)

    cmd = ["/usr/bin/vhd-util", "coalesce", "-n", key_path]
    call("GC", cmd)

    cb.volumeDestroy(opq, key)
    base_path = cb.volumeRename(opq, parent_key, key)

    res = conn.execute("delete from VDI where rowid = (?)", (int(parent_key),))
    res = conn.execute("update VDI set parent = (?) where rowid = (?)",
                       (p_parent, int(key),) )
    conn.commit()

    tap_ctl_unpause(key, conn, cb, opq)

def leaf_coalesce(key, parent_key, conn, cb, opq):
    print ("leaf_coalesce key=%s, parent=%s" % (key, parent_key))
    psize = cb.volumeGetPhysSize(opq, key)
    if psize > (20 * 1024 * 1024):
        leaf_coalesce_snapshot(key, conn, cb, opq)
    else:
        sync_leaf_coalesce(key, parent_key, conn, cb, opq)

def find_best_non_leaf_coalesceable(rows):
    return str(rows[0][0]), str(rows[0][1])

def find_best_non_leaf_coalesceable_2(conn, cb, opq):
    with libvhd.Lock(opq, "gl", cb):
        rows = find_non_leaf_coalesceable(conn)
        for row in rows:
            if not row["gc_status"]:
                parent_rows = conn.execute("select gc_status from VDI where key = (?)",
                                (int(row["parent"]),)).fetchall()
                if not parent_rows[0]["gc_status"]:
                    conn.execute("update VDI set gc_status = (?) where rowid = (?)",
                                 ("Coalescing", row["key"],) )
                    conn.execute("update VDI set gc_status = (?) where rowid = (?)",
                                 ("Coalescing", row["parent"],) )
                    conn.commit()
                    return str(row["key"]), str(row["parent"]),
    return "None", "None"

def run_coalesce(sr_type, uri):
    cb = get_sr_callbacks(sr_type)
    opq = cb.volumeStartOperations(uri, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    print meta_path
    conn = libvhd.connectSQLite3(meta_path)

    get_all_nodes(conn)

    while True:
        key, parent_key = find_best_non_leaf_coalesceable_2(conn, cb, opq)
        if (key, parent_key) != ("None", "None"):
            non_leaf_coalesce(key, parent_key, conn, cb, opq)
        else:
            time.sleep(5)

    rows = find_leaf_coalesceable(conn)
    if rows:
        key, parent_key = find_best_non_leaf_coalesceable(rows)
        leaf_coalesce(key, parent_key, conn, cb, opq)

    conn.close()
    cb.volumeStopOperations(opq)

if __name__ == "__main__":
    sr_type = sys.argv[1]
    uri = sys.argv[2]
    run_coalesce(sr_type, uri)
