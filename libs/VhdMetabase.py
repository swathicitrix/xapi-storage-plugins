#!/usr/bin/env python

import sqlite3
from contextlib import contextmanager

class VDI(object):
    def __init__(self, row):
        self.key = row['key']
        self.name = row['name']
        self.parent = row['parent']
        self.description = row['description']
        self.snap = row['snap']
        self.uuid = row['uuid']
        self.vsize = row['vsize']

class VhdMetabase(object):

    def __init__(self, path):
        self.__path = path
        self.__connect()

    def __connect(self):
        self._conn = sqlite3.connect(self.__path)
        self._conn.row_factory = sqlite3.Row

    def create(self):
        with self._conn:
            self._conn.execute("create table VDI(key integer primary key, snap int,"
                         "parent int, name text, description text, vsize text,"
                         "uuid text, active_on text, gc_status text, nonpersistent integer)")
            # TODO: define indexes, parent, uuid, (active_on?)

    def insert_new_vdi(self, name, description, uuid, vsize):
        return self.__insert_vdi(None, name, description, uuid, vsize)

    def insert_child_vdi(self, parent, name, description, uuid, vsize):
        return self.__insert_vdi(parent, name, description, uuid, vsize)

    def __insert_vdi(self, parent, name, description, uuid, vsize):
        res = self._conn.execute(
            "insert into VDI(snap, parent, name, description, uuid, vsize)"
            " values (:snap, :parent, :name, :description,:uuid, :vsize)",
            {"snap": 0,
             "parent": parent,
             "name": name,
             "description": description,
             "uuid": uuid,
             "vsize": str(vsize)})
        return res.lastrowid

    def get_vdi_by_id(self, key_id):
        res = self._conn.execute("select * from VDI where rowid=:row", {"row" : int(key_id)})
        row = res.fetchone()
        if (row):
            return VDI(row)
        return None

    @contextmanager
    def write_context(self):
        with self._conn:
            yield

    def close(self):
        self._conn.close()
