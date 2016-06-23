#!/usr/bin/env python

from __future__ import absolute_import
import subprocess
import importlib
import os
import sys
import time
import errno

from xapi.storage import log
from xapi.storage.libs import poolhelper

from .vhdutil import VHDUtil
from .metabase import VHDMetabase
from .lock import Lock

# Debug string
GC = 'GC'

# See also: http://stackoverflow.com/a/1160227
def touch(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc: # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

    try:
        open(filename, 'a').close()
    except OSError as exc:
        if exc.errno == errno.EEXIST:
            pass
        else:
            raise

def get_sr_callbacks(sr_type):
    sys.path.insert(
        1,
        '/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.' + sr_type
    )
    mod = importlib.import_module(sr_type)
    return mod.Callbacks()

def find_non_leaf_coalesceable(db):
    results = db.find_non_leaf_coalesceable()
    if len(results) > 0:
        log.debug("Found {} non leaf coalescable nodes".format(len(results)))
    return results

#def find_leaf_coalesceable(db):
#    results = db.find_leaf_coalesceable()
#    for row in results:
#        log.debug("%s" % str(row))
#    log.debug("Found %s leaf coalescable nodes" % len(results))
#    return results

def find_leaves(vhd, db, leaf_accumulator):
    children = db.get_children(vhd.id)
    if len(children) == 0:
        # This is a leaf add it to list
        leaf_accumulator.append(db.get_vdi_for_vhd(vhd.id))
    else:
        for child in children:
            find_leaves(child, db, leaf_accumulator)

def find_root_node(key, db):
    if key.parent_id == None:
        log.debug("Found root node {}".format(key))
        return key
    else:
        parent = db.get_vhd_by_id(key.parent_id)
        return find_root_node(parent, db)

def tap_ctl_pause(node, cb, opq):
    if node.active_on:
        node_path = cb.volumeGetPath(opq, str(node.vhd.id))
        log.debug("VHD {} active on {}".format(node.vhd.id, node.active_on))
        poolhelper.suspend_datapath_on_host(GC, node.active_on, node_path)

def tap_ctl_unpause(node, cb, opq):
    if node.active_on:
        node_path = cb.volumeGetPath(opq, str(node.vhd.id))
        log.debug("VHD {} active on {}".format(node.vhd.id, node.active_on))
        poolhelper.resume_datapath_on_host(GC, node.active_on, node_path)

# def leaf_coalesce_snapshot(key, conn, cb, opq):
#     log.debug("leaf_coalesce_snapshot key=%s" % key)
#     key_path = cb.volumeGetPath(opq, key)

#     res = conn.execute("select name,parent,description,uuid,vsize from VDI where rowid = (?)",
#                        (int(key),)).fetchall()
#     (p_name, p_parent, p_desc, p_uuid, p_vsize) = res[0]

#     tap_ctl_pause(key, conn, cb, opq)
#     res = conn.execute("insert into VDI(snap, parent) values (?, ?)",
#                        (0, p_parent))
#     base_name = str(res.lastrowid)
#     base_path = cb.volumeRename(opq, key, base_name)
#     cb.volumeCreate(opq, key, int(p_vsize))

#     cmd = ["/usr/bin/vhd-util", "snapshot",
#            "-n", key_path, "-p", base_path]
#     output = call("GC", cmd)

#     res = conn.execute("update VDI set parent = (?) where rowid = (?)",
#                            (int(base_name), int(key),) )
#     conn.commit()

#     tap_ctl_unpause(key, conn, cb, opq)

def non_leaf_coalesce(node, parent, uri, cb):
    log.debug("non_leaf_coalesce key={}, parent={}".format(node.id, parent.id))
    #conn.execute("node is coalescing")

    opq = cb.volumeStartOperations(uri, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)

    node_path = cb.volumeGetPath(opq, str(node.id))
    parent_path = cb.volumeGetPath(opq, str(parent.id))

    log.debug("Running vhd-coalesce on {}".format(node.id))
    VHDUtil.coalesce(GC, node_path)

    db = VHDMetabase(meta_path)
    with Lock(opq, "gl", cb):
        # reparent all of the children to this node's parent
        children = db.get_children(node.id)

        # log.debug("List of children: %s" % children)
        for child in children:
            child_path = cb.volumeGetPath(opq, str(child.id))

            # pause all leaves having child as an ancestor
            leaves = []
            find_leaves(child, db, leaves)
            log.debug(
                ("Children of {}: pausing all "
                 "leaves: {}").format(child.id, len(leaves))
            )
            for leaf in leaves:
                tap_ctl_pause(leaf, cb, opq)

            # reparent child to grandparent
            log.debug("Reparenting {} to {}".format(child.id, parent.id))
            with db.write_context():
                db.update_vhd_parent(child.id, parent.id)
                VHDUtil.set_parent(GC, child_path, parent_path)

            # unpause all leaves having child as an ancestor
            log.debug(
                ("Children {}: unpausing all "
                 "leaves: {}").format(child.id, leaves))
            for leaf in leaves:
                tap_ctl_unpause(leaf, cb, opq)

        root_node = find_root_node(parent, db)
        log.debug("Setting gc_status to None root node {}".format(root_node))
        with db.write_context():
            db.update_vhd_gc_status(root_node.id, None)

        # remove key
        log.debug("Destroy {}".format(node.id))
        cb.volumeDestroy(opq, str(node.id))
        with db.write_context():
            db.delete_vhd(node.id)

    db.close()
    cb.volumeStopOperations(opq)

# def sync_leaf_coalesce(key, parent_key, conn, cb, opq):
#     log.debug("leaf_coalesce_snapshot key=%s" % key)
#     key_path = cb.volumeGetPath(opq, key)
#     parent_path = cb.volumeGetPath(opq, parent_key)

#     res = conn.execute("select parent from VDI where rowid = (?)",
#                        (int(parent_key),)).fetchall()
#     p_parent = res[0][0]
#     log.debug("%s" % str(p_parent))
#     if p_parent:
#         p_parent = int(p_parent)
#     else:
#         p_parent = "?"


#     tap_ctl_pause(key, conn, cb, opq)

#     cmd = ["/usr/bin/vhd-util", "coalesce", "-n", key_path]
#     call("GC", cmd)

#     cb.volumeDestroy(opq, key)
#     base_path = cb.volumeRename(opq, parent_key, key)

#     res = conn.execute("delete from VDI where rowid = (?)", (int(parent_key),))
#     res = conn.execute("update VDI set parent = (?) where rowid = (?)",
#                        (p_parent, int(key),) )
#     conn.commit()

#     tap_ctl_unpause(key, conn, cb, opq)

# def leaf_coalesce(key, parent_key, conn, cb, opq):
#     log.debug("leaf_coalesce key=%s, parent=%s" % (key, parent_key))
#     psize = cb.volumeGetPhysSize(opq, key)
#     if psize > (20 * 1024 * 1024):
#         leaf_coalesce_snapshot(key, conn, cb, opq)
#     else:
#         sync_leaf_coalesce(key, parent_key, conn, cb, opq)

#def find_best_non_leaf_coalesceable(rows):
#    return str(rows[0][0]), str(rows[0][1])

def find_best_non_leaf_coalesceable_2(uri, cb):
    opq = cb.volumeStartOperations(uri, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    db = VHDMetabase(meta_path)

    ret = (None, None)
    with Lock(opq, 'gl', cb):
        nodes = find_non_leaf_coalesceable(db)
        for node in nodes:
            if not node.gc_status:
                root_node = find_root_node(node, db)
                if not root_node.gc_status:
                    with db.write_context():
                        db.update_vhd_gc_status(node.id, 'Coalescing')
                        db.update_vhd_gc_status(root_node.id, 'Coalescing')
                    ret = (node, db.get_vhd_by_id(node.parent_id))
                    break
    db.close()
    cb.volumeStopOperations(opq)
    return ret

def remove_garbage_vhds(uri, cb):
    opq = cb.volumeStartOperations(uri, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    db = VHDMetabase(meta_path)

    garbage = db.get_garbage_vhds()

    if len(garbage) > 0:
        for vhd in garbage:
            cb.volumeDestroy(opq, str(vhd.id))
            with db.write_context():
                db.delete_vhd(vhd.id)
    db.close()
    cb.volumeStopOperations(opq)

def daemonize():
    for fd in [0, 1, 2]:
        try:
            os.close(fd)
        except OSError:
            pass

def run_coalesce(sr_type, uri):
    daemonize()

    cb = get_sr_callbacks(sr_type)
    #get_all_nodes(conn)
    opq = cb.volumeStartOperations(uri, 'w')

    gc_running = os.path.join("/var/run/sr-private",
                              cb.getUniqueIdentifier(opq),
                              "gc-running")
    gc_exited = os.path.join("/var/run/sr-private",
                             cb.getUniqueIdentifier(opq),
                             "gc-exited")
    touch(gc_running)

    while True:
        remove_garbage_vhds(uri, cb)

        child, parent = find_best_non_leaf_coalesceable_2(uri, cb)
        if (child, parent) != (None, None):
            non_leaf_coalesce(child, parent, uri, cb)
        else:
            for i in range(10):
                if not os.path.exists(gc_running):
                    touch(gc_exited)
                    return
                time.sleep(3)

    # No leaf coalesce yet
    #rows = find_leaf_coalesceable(conn)
    #if rows:
    #    key, parent_key = find_best_non_leaf_coalesceable(rows)
    #    leaf_coalesce(key, parent_key, conn, cb, opq)

    #conn.close()

class VHDCoalesce(object):
    @staticmethod
    def start_gc(dbg, sr_type, uri):
        args = [os.path.abspath(__file__), sr_type, uri]
        subprocess.Popen(args)
        log.debug("{}: Started GC sr_type={} uri={}".format(dbg, sr_type, uri))

    @staticmethod
    def stop_gc(dbg, sr_type, uri):
        cb = get_sr_callbacks(sr_type)
        opq = cb.volumeStartOperations(uri, 'w')

        gc_running = os.path.join(
            '/var/run/sr-private',
            cb.getUniqueIdentifier(opq),
            'gc-running'
        )

        gc_exited = os.path.join(
            "/var/run/sr-private",
            cb.getUniqueIdentifier(opq),
            'gc-exited'
        )

        os.unlink(gc_running)

        while True:
            if os.path.exists(gc_exited):
                os.unlink(gc_exited)
                return
            else:
                time.sleep(1)

if __name__ == '__main__':
    sr_type = sys.argv[1]
    uri = sys.argv[2]
    run_coalesce(sr_type, uri)
