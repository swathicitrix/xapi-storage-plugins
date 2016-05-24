import uuid
import sqlite3
import os
import urlparse
import sys
from xapi.storage.libs import util
from xapi.storage.libs.util import call
from xapi.storage.libs import log
import xapi.storage.libs.poolhelper
from xapi.storage.libs import VhdMetabase
from xapi.storage.libs import tapdisk, image
from contextlib import contextmanager

DP_URI_PREFIX = "vhd+tapdisk://"
OPT_LOG_ERR = "--debug"
MSIZE_MB = 2 * 1024 * 1024

def create_metabase(path):
    metabase = VhdMetabase.VhdMetabase(path)
    metabase.create()
    metabase.close()

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

    conn = connectSQLite3(meta_path)
    with write_context(conn):
        res = conn.execute("insert into VDI(snap, name, description, uuid, vsize) values (?, ?, ?, ?, ?)", 
                           (0, name, description, vol_uuid, str(vsize)))
        vol_name = str(res.lastrowid)

        vol_path = cb.volumeCreate(opq, vol_name, size)
        cb.volumeActivateLocal(opq, vol_name)

        # Create the VHD
        cmd = ["/usr/bin/vhd-util", "create", "-n", vol_path,
               "-s", str(size_mib), "-S", str(MSIZE_MB)]
        call(dbg, cmd)

        cb.volumeDeactivateLocal(opq, vol_name)

        # Fetch physical utilisation
        psize = cb.volumeGetPhysSize(opq, vol_name)

        vol_uri = cb.getVolumeURI(opq, vol_name)
        cb.volumeStopOperations(opq)

    conn.close()

    return {
        "key": vol_name,
        "uuid": vol_uuid,
        "name": name,
        "description": description,
        "read_write": True,
        "virtual_size": vsize,
        "physical_utilisation": psize,
        "uri": [DP_URI_PREFIX + vol_uri],
        "keys": {},
    }

def destroy(dbg, sr, key, cb):
    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = connectSQLite3(meta_path)
    with Lock(opq, "gl", cb):
        with write_context(conn):
            cb.volumeDestroy(opq, key)
            res = conn.execute("delete from VDI where rowid = (?)", (int(key),))
        conn.close()
    cb.volumeStopOperations(opq)

def resize(dbg, sr, key, new_size, cb):
    size = int(new_size)
    size_mib = size / 1048576
    if size % 1048576 != 0:
        size_mib = size_mib + 1
    vsize = size_mib * 1048576

    opq = cb.volumeStartOperations(sr, 'w')
    key_path = cb.volumeGetPath(opq, key)
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = connectSQLite3(meta_path)
    with write_context(conn):
        conn.execute("update VDI set vsize = (?) where rowid = (?)", (str(vsize), int(key),))
        cb.volumeResize(opq, key, vsize)
        cmd = ["/usr/bin/vhd-util", "resize", "-n", key_path,
               "-s", str(size_mib), "-f"] 
        call(dbg, cmd)

    conn.close()
    cb.volumeStopOperations(opq)

def clone(dbg, sr, key, cb):
    snap_uuid = str(uuid.uuid4())

    opq = cb.volumeStartOperations(sr, 'w')
    meta_path = cb.volumeMetadataGetPath(opq)
    key_path = cb.volumeGetPath(opq, key)

    conn = connectSQLite3(meta_path)
    with Lock(opq, "gl", cb):
        with write_context(conn):
            res = conn.execute("select name,parent,description,uuid,vsize from VDI where rowid = (?)",
                               (int(key),)).fetchall()
            (p_name, p_parent, p_desc, p_uuid, p_vsize) = res[0]

            res = conn.execute("insert into VDI(snap, parent, name, description, uuid, vsize) values (?, ?, ?, ?, ?, ?)", 
                               (0, p_parent, p_name, p_desc, snap_uuid, p_vsize))
            snap_name = str(res.lastrowid)
            snap_path = cb.volumeCreate(opq, snap_name, int(p_vsize))
        
            # Snapshot from key
            cmd = ["/usr/bin/vhd-util", "snapshot",
                   "-n", snap_path, "-p", key_path, "-S", str(MSIZE_MB)]
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
            
                res = conn.execute("select active_on from VDI where key = ?", (int(key),)).fetchall()
                active_on = res[0][0]
                if active_on: 
                    xapi.storage.libs.poolhelper.suspend_datapath_on_host(dbg, active_on, key_path)
                res = conn.execute("insert into VDI(snap, parent) values (?, ?)",
                                   (0, p_parent))
                base_name = str(res.lastrowid)
                base_path = cb.volumeRename(opq, key, base_name)
                cb.volumeCreate(opq, key, int(p_vsize))

                cmd = ["/usr/bin/vhd-util", "snapshot",
                       "-n", key_path, "-p", base_path, "-S", str(MSIZE_MB)]
                output = call(dbg, cmd)

                # Finally, update the snapshot parent to the rebased volume
                cmd = ["/usr/bin/vhd-util", "modify",
                       "-n", snap_path, "-p", base_path]
                output = call(dbg, cmd)
                res = conn.execute("update VDI set parent = (?) where rowid = (?)",
                                   (int(base_name), int(snap_name),) )
                res = conn.execute("update VDI set parent = (?) where rowid = (?)",
                                   (int(base_name), int(key),) )
        
                if active_on: 
                    xapi.storage.libs.poolhelper.resume_datapath_on_host(dbg, active_on, key_path)

    conn.close()
    psize = cb.volumeGetPhysSize(opq, snap_name)
    snap_uri = cb.getVolumeURI(opq, snap_name)
    cb.volumeStopOperations(opq)

    return {
        "uuid": snap_uuid,
        "key": snap_name,
        "name": p_name,
        "description": p_desc,
        "read_write": True,
        "virtual_size": p_vsize,
        "physical_utilisation": psize,
        "uri": [DP_URI_PREFIX + snap_uri],
        "keys": {}
    }

def stat(dbg, sr, key, cb):
    opq = cb.volumeStartOperations(sr, 'r')
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = connectSQLite3(meta_path)
    res = conn.execute("select name,description,uuid,vsize from VDI where rowid = (?)", 
                       (int(key),)).fetchall()
    conn.close()

    (name,desc,uuid,vsize) = res[0]
    psize = cb.volumeGetPhysSize(opq, key)
    key_uri = cb.getVolumeURI(opq, key)
    cb.volumeStopOperations(opq)

    return {
        "uuid": uuid,
        "key": key,
        "name": name,
        "description": desc,
        "read_write": True,
        "virtual_size": vsize,
        "physical_utilisation": psize,
        "uri": [DP_URI_PREFIX + key_uri],
        "keys": {}
    }

def ls(dbg, sr, cb):
    results = []    
    opq = cb.volumeStartOperations(sr, 'r')
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = connectSQLite3(meta_path)
    res = conn.execute("select key,name,description,uuid,vsize from VDI where uuid not NULL").fetchall()
    conn.close()
    
    for (key_int,name,desc,uuid,vsize) in res:
        key = str(key_int)
        psize = cb.volumeGetPhysSize(opq, key)
        vol_path = cb.volumeGetPath(opq, key)
        vol_uri = cb.getVolumeURI(opq, key)
        results.append({
                "uuid": uuid,
                "key": key,
                "name": name,
                "description": desc,
                "read_write": True,
                "virtual_size": vsize,
                "physical_utilisation": psize,
                "uri": [DP_URI_PREFIX + vol_uri],
                "keys": {}
        })

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
    
    conn = connectSQLite3(meta_path)
    with write_context(conn):
        # Danger, danger. Where does field come from? SQL Injection risk!
        query = ("update VDI set %s = (?) where rowid = (?)" % field)
        res = conn.execute(query, (value, int(key),) )
    conn.close()
    cb.volumeStopOperations(opq)



# here we have all datapath facing functions

def parse_datapath_uri(uri):
    # uri will be like:
    # "vhd+tapdisk://<sr-type>/<sr-mount-or-volume-group>|<volume-name>"
    mount_or_vg,name = urlparse.urlparse(uri).path.split("|") 
    return ("vhd:///" + mount_or_vg, name)     

def attach(dbg, uri, domain, cb):
    sr,name = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'r')
    # activate LVs chain here
    vol_path = cb.volumeGetPath(opq, name)
    tap = tapdisk.create(dbg)
    tapdisk.save_tapdisk_metadata(dbg, vol_path, tap)
    return {
        'domain_uuid': '0',
        'implementation': ['Tapdisk3', tap.block_device()],
        }

def activate(dbg, uri, domain, cb):
    this_host_label = util.get_current_host()

    sr,name = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'w')
    vol_path = cb.volumeGetPath(opq, name)
    meta_path = cb.volumeMetadataGetPath(opq)

    with Lock(opq, "gl", cb):
        conn = connectSQLite3(meta_path)
        with write_context(conn):
            res = conn.execute("update VDI set active_on = (?) where rowid = (?)",
                               (this_host_label, int(name),) )

            img = image.Vhd(vol_path)
            tap = tapdisk.load_tapdisk_metadata(dbg, vol_path)
            tap.open(dbg, img)
            tapdisk.save_tapdisk_metadata(dbg, vol_path, tap)

        conn.close()
    
def deactivate(dbg, uri, domain, cb):
    sr,name = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'w')
    vol_path = cb.volumeGetPath(opq, name)
    meta_path = cb.volumeMetadataGetPath(opq)

    conn = connectSQLite3(meta_path)
    with Lock(opq, "gl", cb):
        with write_context(conn):
            res = conn.execute("update VDI set active_on = (?) where rowid = (?)",
                               (None, int(name),) )

            tap = tapdisk.load_tapdisk_metadata(dbg, vol_path)
            tap.close(dbg)

        conn.close()

def detach(dbg, uri, domain, cb):
    sr,name = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'r')
    # deactivate LVs chain here
    vol_path = cb.volumeGetPath(opq, name)
    tap = tapdisk.load_tapdisk_metadata(dbg, vol_path)
    tap.destroy(dbg)
    tapdisk.forget_tapdisk_metadata(dbg, vol_path)

def db_check_vdi_is_nonpersistent(dbg, conn, name):
    log.debug("%s: libvhd.db_check_vdi_is_nonpersistent: name == %s" %
              (dbg, name))

    res = conn.execute("select nonpersistent from VDI where rowid=:row",
                       {"row" : int(name)}).fetchall()
    return res[0][0] == 1

def set_vdi_non_persistent(conn, name):
    conn.execute("update VDI set nonpersistent=1 where rowid=:row",
                 {"row" : int(name)})

def clear_vdi_non_persistent(conn, name):
    conn.execute("update VDI set nonpersistent=NULL where rowid=:row",
                 {"row" : int(name)})

def truncate_leaf_vhd(vol_path, dbg):
    "zero out the disk (kill all data inside the VHD file)"
    cmd = ["/usr/bin/vhd-util", "modify", OPT_LOG_ERR, "-z", "-n", vol_path]
    call(dbg, cmd)

def num_bits(val):
    count = 0
    while val:
        count += val & 1
        val = val >> 1
    return count

def count_bits(bitmap):
    count = 0
    for i in range(len(bitmap)):
        count += num_bits(ord(bitmap[i]))
    return count

def vhd_is_empty(dbg, vol_path):
    cmd = ["/usr/bin/vhd-util", "read", OPT_LOG_ERR, "-B", "-n", vol_path]
    ret = call(dbg, cmd)
    return count_bits(ret) == 0

def create_single_clone(conn, sr, name, cb):
    pass

def epcopen(dbg, uri, persistent, cb):
    log.debug("%s: Datapath.epcopen: uri == %s" % (dbg, uri))

    sr, name = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'w')

    vol_path = cb.volumeGetPath(opq, name)
    meta_path = cb.volumeMetadataGetPath(opq)
    
    conn = connectSQLite3(meta_path)

    try:
        with Lock(opq, "gl", cb):
            try:
                with write_context(conn):
                    if (persistent):
                        log.debug("%s: Datapath.epcopen: %s is persistent "
                                  % (dbg, vol_path))
                        if (db_check_vdi_is_nonpersistent(dbg, conn, name)):
                            # Truncate, etc
                            truncate_leaf_vhd(vol_path)
                            clear_vdi_non_persistent(conn, name)
                    elif (db_check_vdi_is_nonpersistent(dbg, conn, name)):
                        log.debug(
                            "%s: Datapath.epcopen: %s already marked non-persistent"
                            % (dbg, vol_path))
                        # truncate
                        truncate_leaf_vhd(vol_path, dbg)
                    else:
                        log.debug("%s: Datapath.epcopen: %s is non-persistent"
                                  % (dbg, vol_path))
                        set_vdi_non_persistent(conn, name)
                        if (not vhd_is_empty(dbg, vol_path)):
                            # Create single clone
                            create_single_clone(conn, sr, name, cb)
            except:
                log.error("%s: Datapath.epcopen: failed to complete open, %s"
                          % (dbg, sys.exc_info()[0]))
                raise
    finally:
        conn.close()

    return None

def epcclose(dbg, uri, cb):
    log.debug("%s: Datapath.epcclose: uri == %s" % (dbg, uri))
    sr, name = parse_datapath_uri(uri)
    opq = cb.volumeStartOperations(sr, 'w')

    vol_path = cb.volumeGetPath(opq, name)
    meta_path = cb.volumeMetadataGetPath(opq)
    
    conn = connectSQLite3(meta_path)

    try:
        with Lock(opq, "gl", cb):
            with write_context(conn):
                if (db_check_vdi_is_nonpersistent(dbg, conn, name)):
                    # truncate
                    truncate_leaf_vhd(vol_path, dbg)
                    clear_vdi_non_persistent(conn, name)
    except:
        log.error("%s: Datapath.epcclose: failed to complete close, %s"
                  % (dbg, sys.exc_info()[0]))
        raise
    finally:
        conn.close()

    return None

def connectSQLite3(db):
    conn = sqlite3.connect(db, timeout=3600, isolation_level="DEFERRED")
    conn.row_factory = sqlite3.Row
    return conn

def startGC(dbg, sr_name, uri):
    import vhd_coalesce
    vhd_coalesce.startGC(dbg, sr_name, uri)

def stopGC(dbg, sr_name, uri):
    import vhd_coalesce
    vhd_coalesce.stopGC(dbg, sr_name, uri)

@contextmanager
def write_context(conn):
    with conn:
        yield

class Lock():
    def __init__(self, opq, name, cb):
        self.opq = opq
        self.name = name
        self.cb = cb

    def __enter__(self):
        self.lock = self.cb.volumeLock(self.opq, self.name)

    def __exit__(self, type, value, traceback):
        return self.cb.volumeUnlock(self.opq, self.lock)
