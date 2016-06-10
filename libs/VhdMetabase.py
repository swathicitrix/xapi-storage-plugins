#!/usr/bin/env python

import sqlite3
from contextlib import contextmanager

class VDI(object):
    def __init__(self, row):
        self.uuid = row['uuid']
        self.name = row['name']
        self.description = row['description']
        self.vhd = VHD.fromrow(row)

class VHD(object):
    def __init__(self, vhd_id, parent, snap, vsize, psize):
        self.id = vhd_id
        self.parent_id = parent
        self.snap = snap
        self.vsize = vsize
        self.psize = psize

    def is_child_of(self, vhd_2):
        # CALL VHD_UTIL
        if self.parent_id == vhd_2.id:
            return True
        return False

    @classmethod
    def fromrow(cls, row):
        return cls(row['id'],
                   row['parent_id'],
                   row['snap'],
                   row['vsize'],
                   row['psize'])

class VhdMetabase(object):

    def __init__(self, path):
        self.__path = path
        self.__connect()

    def __connect(self):
        self._conn = sqlite3.connect(self.__path)
        self._conn.row_factory = sqlite3.Row

    def create(self):
        with self._conn:
            self._conn.execute(
                "CREATE TABLE vhd(id INTEGER PRIMARY KEY, snap INTEGER, "
                "parent_id INTEGER, vsize INTEGER, psize INTEGER, "
                "gc_status TEXT)")
            self._conn.execute("CREATE INDEX vhd_parent ON vhd(parent_id)")
            self._conn.execute(
                "CREATE TABLE vdi(uuid text PRIMARY KEY, name TEXT, "
                "description TEXT, active_on TEXT, nonpersistent INTEGER, "
                "vhd_id NOT NULL UNIQUE, "
                "FOREIGN KEY(vhd_id) REFERENCES vhd(key))")

    def insert_vdi(self, name, description, uuid, vhd_id):
        res = self._conn.execute(
            "INSERT INTO vdi(uuid, name, description, vhd_id)"
            " values (:uuid, :name, :description, :vhd_id)",
            {"uuid": uuid,
             "name": name,
             "description": description,
             "vhd_id": vhd_id})

    def update_vdi_vhd_id(self, uuid, vhd_id):
        res = self._conn.execute(
            "UPDATE vdi SET vhd_id"
            " values (:vhd_id) WHERE uuid = :uuid",
            {"vhd_id": vhd_id, "uuid": uuid})

    def update_vdi_name(self, uuid, name):
        res = self._conn.execute(
            "UPDATE vdi SET name"
            " values (:name) WHERE uuid = :uuid",
            {"name": name, "uuid": uuid})

    def update_vdi_description(self, uuid, description):
        res = self._conn.execute(
            "UPDATE vdi SET description"
            " values (:description) WHERE uuid = :uuid",
            {"description": description, "uuid": uuid})

    def insert_new_vhd(self, vsize):
        return self.__insert_vhd(None, None, vsize, None)

    def insert_child_vhd(self, parent, vsize):
        return self.__insert_vhd(parent, None, vsize, None)

    def update_vhd_parent(self, vhd_id, parent):
        self.__update_vhd(vhd_id, "parent", parent)

    def update_vhd_vsize(self, vhd_id, vsize):
        self.__update_vhd(vhd_id, "vsize", vsize)

    def update_vhd_psize(self, vhd_id, psize):
        self.__update_vhd(vhd_id, "psize", psize)

    def __update_vhd(self, vhd_id, key, value):
        query = "UPDATE vhd SET %s VALUES (?)" % key
        res = self._conn.execute(query, (key))

    def __insert_vhd(self, parent, snap, vsize, psize):
        res = self._conn.execute(
            "INSERT INTO vhd(parent_id, snap, vsize, psize) " 
            "VALUES (:parent, :snap, :vsize, :psize)",
            {"parent": parent,
             "snap": snap,
             "vsize": vsize,
             "psize": psize})
        return VHD(res.lastrowid, parent, snap, vsize, psize)

    def get_vdi_by_id(self, vdi_uuid):
        res = self._conn.execute(
            "SELECT * FROM vdi INNER JOIN vhd ON vdi.vhd_id = vhd.id "
            "WHERE uuid=:uuid",
            {"uuid" : vdi_uuid})
        row = res.fetchone()
        if (row):
            return VDI(row)
        return None

    def get_all_vdis(self):
        res = self._conn.execute(
            "SELECT * FROM VDI INNER JOIN vhd ON vdi.vhd_id = vhd.id")
        vdis = []
        for row in res:
            vdis.append(VDI(row))
        return vdis

    @contextmanager
    def write_context(self):
        with self._conn:
            yield

    def close(self):
        self._conn.close()
