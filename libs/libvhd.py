#!/usr/bin/env python

import uuid
import sqlite3
import os
import urlparse
import sys
from xapi.storage.libs import util
from xapi.storage.libs import vhdutil
from xapi.storage.libs.util import call
from xapi.storage.libs import log
import xapi.storage.libs.poolhelper
from xapi.storage.libs import VhdMetabase
from xapi.storage.libs import tapdisk, image
from contextlib import contextmanager

DP_URI_PREFIX = "vhd+tapdisk://"
MSIZE_MB = 2 * 1024 * 1024

def create_metabase(path):
    metabase = VhdMetabase.VhdMetabase(path)
    metabase.create()
    metabase.close()

def get_size_mb_and_vsize(size):
    # Calculate virtual size (round up size to nearest MiB)
    size = int(size)
    size_mib = size / 1048576
    if size % 1048576 != 0:
        size_mib = size_mib + 1
    vsize = size_mib * 1048576
    return size_mib, vsize

def create(dbg, sr, name, description, size, cb):
    size_mib,vsize = get_size_mb_and_vsize(size)

    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    vdi_uuid = str(uuid.uuid4())

    db = VhdMetabase.VhdMetabase(meta_path)
    with db.write_context():
        vhd = db.insert_new_vhd(vsize)
        db.insert_vdi(name, description, vdi_uuid, vhd.id)
        vhd_path = cb.volumeCreate(opq, str(vhd.id), vsize)
        vhdutil.create(dbg, vhd_path, size_mib)
    db.close()

    psize = cb.volumeGetPhysSize(opq, str(vhd.id))
    vdi_uri = cb.getVolumeUriPrefix(opq) + vdi_uuid
    cb.volumeStopOperations(opq)

    return {
        "key": vdi_uuid,
        "uuid": vdi_uuid,
        "name": name,
        "description": description,
        "read_write": True,
        "virtual_size": vsize,
        "physical_utilisation": psize,
        "uri": [DP_URI_PREFIX + vdi_uri],
        "keys": {}
        }

def destroy(dbg, sr, key, cb):
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)

    db = VhdMetabase.VhdMetabase(meta_path)
    with Lock(opq, "gl", cb):
        with db.write_context():
            vdi = db.get_vdi_by_id(key)
            db.delete_vdi(key)
        with db.write_context():
            cb.volumeDestroy(opq, str(vdi.vhd.id))
            db.delete_vhd(vdi.vhd.id)
        db.close()
    cb.volumeStopOperations(opq)

def resize(dbg, sr, key, new_size, cb):
    size_mib,vsize = get_size_mb_and_vsize(new_size)

    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)

    db = VhdMetabase.VhdMetabase(meta_path)
    with db.write_context():
        vdi = db.get_vdi_by_id(key)
        db.update_vhd_vsize(vdi.vhd.id, None)
    with db.write_context():
        cb.volumeResize(opq, str(vdi.vhd.id), vsize)
        vol_path = cb.volumeGetPath(opq, str(vdi.vhd.id))
        vhdutil.resize(dbg, vol_path, size_mib)  
        db.update_vhd_vsize(vdi.vhd.id, vsize)
    db.close()

    cb.volumeStopOperations(opq)

def refresh_datapath(dbg, vdi, vol_path, new_vol_path):
    if vdi.active_on:
        xapi.storage.libs.poolhelper.refresh_datapath_on_host(dbg, vdi.active_on, vol_path, new_vol_path)

def clone(dbg, sr, key, cb):
    snap_uuid = str(uuid.uuid4())
    need_extra_snap = False

    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)

    db = VhdMetabase.VhdMetabase(meta_path)
    with Lock(opq, "gl", cb):
        with db.write_context():
            vdi = db.get_vdi_by_id(key)
            vol_path = cb.volumeGetPath(opq, str(vdi.vhd.id))
            snap_vhd = db.insert_child_vhd(vdi.vhd.parent_id, vdi.vhd.vsize)
            snap_path = cb.volumeCreate(opq, str(snap_vhd.id), vdi.vhd.vsize)
            vhdutil.snapshot(dbg, vol_path, snap_path)

            # NB. As an optimisation, "vhd-util snapshot A->B" will check if
            #     "A" is empty. If it is, it will set "B.parent" to "A.parent"
            #     instead of "A" (provided "A" has a parent) and we are done.
            #     If "B.parent" still points to "A", we need to rebase "A".

            if vhdutil.is_parent_pointing_to_path(dbg, snap_path, vol_path):
                need_extra_snap = True
                db.update_vhd_parent(snap_vhd.id, vdi.vhd.id)
                db.update_vdi_vhd_id(vdi.uuid, snap_vhd.id)
            else:
                db.insert_vdi(vdi.name, vdi.description, snap_uuid, snap_vhd.id)

        if need_extra_snap:
            refresh_datapath(dbg, vdi, vol_path, snap_path)
            with db.write_context():
                db.update_vhd_psize(vdi.vhd.id, cb.volumeGetPhysSize(opq, str(vdi.vhd.id)))
                snap_2_vhd = db.insert_child_vhd(vdi.vhd.id, vdi.vhd.vsize)
                snap_2_path = cb.volumeCreate(opq, str(snap_2_vhd.id), vdi.vhd.vsize)
                vhdutil.snapshot(dbg, vol_path, snap_2_path)
                db.insert_vdi(vdi.name, vdi.description, snap_uuid, snap_2_vhd.id)
    db.close()

    if need_extra_snap:
        psize = cb.volumeGetPhysSize(opq, str(snap_2_vhd.id))
    else:
        psize = cb.volumeGetPhysSize(opq, str(snap_vhd.id))

    snap_uri = cb.getVolumeUriPrefix(opq) + snap_uuid
    cb.volumeStopOperations(opq)

    return {
        "uuid": snap_uuid,
        "key": snap_uuid,
        "name": vdi.name,
        "description": vdi.description,
        "read_write": True,
        "virtual_size": vdi.vhd.vsize,
        "physical_utilisation": psize,
        "uri": [DP_URI_PREFIX + snap_uri],
        "keys": {}
    }


def stat(dbg, sr, key, cb):
    opq = cb.volumeStartOperations(sr, 'r')
    meta_path = cb.volumeMetadataGetPath(opq)

    db = VhdMetabase.VhdMetabase(meta_path)
    with db.write_context():
        vdi = db.get_vdi_by_id(key)
        vsize = vdi.vhd.vsize
        if not vsize:
            # vsize can be None if we crashed during a Volume.resize
            vsize = vhdutil.get_vsize(dbg, cb.volumeGetPath(opq, str(vdi.vhd.id)))
            db.update_vhd_vsize(vdi.vhd.id, vsize)
    db.close()

    psize = cb.volumeGetPhysSize(opq, str(vdi.vhd.id))
    vdi_uri = cb.getVolumeUriPrefix(opq) + vdi.uuid
    cb.volumeStopOperations(opq)

    return {
        "uuid": vdi.uuid,
        "key": vdi.uuid,
        "name": vdi.name,
        "description": vdi.description,
        "read_write": True,
        "virtual_size": vsize,
        "physical_utilisation": psize,
        "uri": [DP_URI_PREFIX + vdi_uri],
        "keys": {}
    }

def ls(dbg, sr, cb):
    results = []    
    opq = cb.volumeStartOperations(sr, 'r')
    meta_path = cb.volumeMetadataGetPath(opq)

    db = VhdMetabase.VhdMetabase(meta_path)
    with db.write_context():
        vdis = db.get_all_vdis()

    for vdi in vdis:
        vsize = vdi.vhd.vsize
        if not vsize:
            # vsize can be None if we crashed during a Volume.resize
            with db.write_context():
                vsize = vhdutil.get_vsize(dbg, cb.volumeGetPath(opq, str(vdi.vhd.id)))
                db.update_vhd_vsize(vdi.vhd.id, vsize)
        psize = cb.volumeGetPhysSize(opq, str(vdi.vhd.id))
        vdi_uri = cb.getVolumeUriPrefix(opq) + vdi.uuid
        results.append({
                "uuid": vdi.uuid,
                "key": vdi.uuid,
                "name": vdi.name,
                "description": vdi.description,
                "read_write": True,
                "virtual_size": vsize,
                "physical_utilisation": psize,
                "uri": [DP_URI_PREFIX + vdi_uri],
                "keys": {}
        })

    db.close()
    cb.volumeStopOperations(opq)
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
    
    db = VhdMetabase.VhdMetabase(meta_path)
    with db.write_context():
        vdi = db.get_vdi_by_id(key)        
        if field == "name":
            db.update_vdi_name(vdi.uuid, value)
        elif field == "description":
            db.update_vdi_description(vdi.uuid, value)
    db.close()
    cb.volumeStopOperations(opq)


# here we have all datapath facing functions

def parse_datapath_uri(uri):
    # uri will be like:
    # "vhd+tapdisk://<sr-type>/<sr-mount-or-volume-group>|<volume-name>"
    mount_or_vg,name = urlparse.urlparse(uri).path.split("|") 
    return ("vhd:///" + mount_or_vg, name)     

def attach(dbg, uri, domain, cb):
    sr,key = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'r')

    meta_path = cb.volumeMetadataGetPath(opq)
    db = VhdMetabase.VhdMetabase(meta_path)
    with db.write_context():
        vdi = db.get_vdi_by_id(key)
    # activate LVs chain here
    db.close()

    vol_path = cb.volumeGetPath(opq, str(vdi.vhd.id))
    cb.volumeStopOperations(opq)
    tap = tapdisk.create(dbg)
    tapdisk.save_tapdisk_metadata(dbg, vol_path, tap)
    return {
        'domain_uuid': '0',
        'implementation': ['Tapdisk3', tap.block_device()],
        }

def activate(dbg, uri, domain, cb):
    this_host_label = util.get_current_host()
    sr,key = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    db = VhdMetabase.VhdMetabase(meta_path)

    with Lock(opq, "gl", cb):
        with db.write_context():
            vdi = db.get_vdi_by_id(key)
            db.update_vdi_active_on(vdi.uuid, this_host_label)
            vol_path = cb.volumeGetPath(opq, str(vdi.vhd.id))
            img = image.Vhd(vol_path)
            tap = tapdisk.load_tapdisk_metadata(dbg, vol_path)
            tap.open(dbg, img)
            tapdisk.save_tapdisk_metadata(dbg, vol_path, tap)
    db.close()
    cb.volumeStopOperations(opq)
    
def deactivate(dbg, uri, domain, cb):
    sr,key = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    db = VhdMetabase.VhdMetabase(meta_path)

    with Lock(opq, "gl", cb):
        with db.write_context():
            vdi = db.get_vdi_by_id(key)
            db.update_vdi_active_on(vdi.uuid, None)
            vol_path = cb.volumeGetPath(opq, str(vdi.vhd.id))
            tap = tapdisk.load_tapdisk_metadata(dbg, vol_path)
            tap.close(dbg)

    db.close()
    cb.volumeStopOperations(opq)

def detach(dbg, uri, domain, cb):
    sr,key = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'r')

    meta_path = cb.volumeMetadataGetPath(opq)
    db = VhdMetabase.VhdMetabase(meta_path)
    with db.write_context():
        vdi = db.get_vdi_by_id(key)
    # deactivate LVs chain here
    db.close()

    vol_path = cb.volumeGetPath(opq, str(vdi.vhd.id))
    cb.volumeStopOperations(opq)
    tap = tapdisk.load_tapdisk_metadata(dbg, vol_path)
    tap.destroy(dbg)
    tapdisk.forget_tapdisk_metadata(dbg, vol_path)

def create_single_clone(conn, sr, key, cb):
    pass

def epcopen(dbg, uri, persistent, cb):
    log.debug("%s: Datapath.epcopen: uri == %s" % (dbg, uri))

    sr, key = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'w')

    meta_path = cb.volumeMetadataGetPath(opq)    
    db = VhdMetabase.VhdMetabase(meta_path)

    try:
        with Lock(opq, "gl", cb):
            try:
                with db.write_context():
                    vdi = db.get_vdi_by_id(key)
                    vol_path = cb.volumeGetPath(opq, str(vdi.vhd.id))
                    if (persistent):
                        log.debug("%s: Datapath.epcopen: %s is persistent "
                                  % (dbg, vol_path))
                        if vdi.nonpersistent:
                            # Truncate, etc
                            vhdutil.reset(dbg, vol_path)
                            db.update_vdi_nonpersistent(vdi.uuid, 1)
                    elif vdi.nonpersistent:
                        log.debug(
                            "%s: Datapath.epcopen: %s already marked non-persistent"
                            % (dbg, vol_path))
                        # truncate
                        vhdutil.reset(dbg, vol_path)
                    else:
                        log.debug("%s: Datapath.epcopen: %s is non-persistent"
                                  % (dbg, vol_path))
                        db.update_vdi_nonpersistent(vdi.uuid, 1)
                        if (not vhdutil.is_empty(dbg, vol_path)):
                            # Create single clone
                            create_single_clone(conn, sr, key, cb)
            except:
                log.error("%s: Datapath.epcopen: failed to complete open, %s"
                          % (dbg, sys.exc_info()[0]))
                raise
    finally:
        db.close()

    return None

def epcclose(dbg, uri, cb):
    log.debug("%s: Datapath.epcclose: uri == %s" % (dbg, uri))
    sr, key = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'w')

    meta_path = cb.volumeMetadataGetPath(opq)
    db = VhdMetabase.VhdMetabase(meta_path)
    
    try:
        with Lock(opq, "gl", cb):
            with db.write_context():
                vdi = db.get_vdi_by_id(key)
                vol_path = cb.volumeGetPath(opq, str(vdi.vhd.id))
                if vdi.nonpersistent:
                    # truncate
                    vhdutil.reset(dbg, vol_path)
                    db.update_vdi_nonpersistent(vdi.uuid, None)
    except:
        log.error("%s: Datapath.epcclose: failed to complete close, %s"
                  % (dbg, sys.exc_info()[0]))
        raise
    finally:
        db.close()

    return None

def startGC(dbg, sr_name, uri):
    return
    import vhd_coalesce
    vhd_coalesce.startGC(dbg, sr_name, uri)

def stopGC(dbg, sr_name, uri):
    return
    import vhd_coalesce
    vhd_coalesce.stopGC(dbg, sr_name, uri)

class Lock():
    def __init__(self, opq, name, cb):
        self.opq = opq
        self.name = name
        self.cb = cb

    def __enter__(self):
        self.lock = self.cb.volumeLock(self.opq, self.name)

    def __exit__(self, type, value, traceback):
        return self.cb.volumeUnlock(self.opq, self.lock)
