#!/usr/bin/env python

import sqlite3
from contextlib import contextmanager

class VDI(object):
    def __init__(self, uuid, name, description, active_on, nonpersistent, vhd):
        self.uuid = uuid
        self.name = name
        self.description = description
        self.active_on = active_on
        self.nonpersistent = nonpersistent
        self.vhd = vhd

    @classmethod
    def from_row(cls, row):
        vhd = VHD.from_row(row)
        return cls(
            row['uuid'],
            row['name'],
            row['description'],
            row['active_on'],
            row['nonpersistent'],
            vhd)

class VHD(object):
    def __init__(self, vhd_id, parent, snap, vsize, psize, gc_status = None):
        self.id = vhd_id
        self.parent_id = parent
        self.snap = snap
        self.vsize = vsize
        self.psize = psize
        self.gc_status = gc_status

    def is_child_of(self, vhd_2):
        # CALL VHD_UTIL
        if self.parent_id == vhd_2.id:
            return True
        return False

    @classmethod
    def from_row(cls, row):
        return cls(
            row['id'],
            row['parent_id'],
            row['snap'],
            row['vsize'],
            row['psize'],
            row['gc_status']
        )

class VhdMetabase(object):

    def __init__(self, path):
        self.__path = path
        self.__connect()

    def __connect(self):
        self._conn = sqlite3.connect(self.__path, timeout=3600, isolation_level="DEFERRED")
        self._conn.row_factory = sqlite3.Row

    def create(self):
        with self._conn:
            self._conn.execute("""
                CREATE TABLE vhd(
                    id        INTEGER PRIMARY KEY NOT NULL,
                    snap      INTEGER,
                    parent_id INTEGER,
                    vsize     INTEGER,
                    psize     INTEGER,
                    gc_status TEXT
                )"""
            )
            self._conn.execute(
                "CREATE INDEX vhd_parent ON vhd(parent_id)"
            )
            self._conn.execute("""
                CREATE TABLE vdi(
                    uuid          TEXT             PRIMARY KEY,
                    name          TEXT,
                    description   TEXT,
                    active_on     TEXT,
                    nonpersistent INTEGER,
                    vhd_id        INTEGER NOT NULL UNIQUE,
                    FOREIGN KEY(vhd_id) REFERENCES vhd(id)
                )"""
            )
            self._conn.execute(
                "CREATE INDEX vdi_vhd_id ON vdi(vhd_id)"
            )

    def insert_vdi(self, name, description, uuid, vhd_id):
        res = self._conn.execute("""
            INSERT INTO vdi(uuid, name, description, vhd_id)
            VALUES (:uuid, :name, :description, :vhd_id)""",
            {"uuid": uuid,
             "name": name,
             "description": description,
             "vhd_id": vhd_id}
        )

    def delete_vdi(self, uuid):
        self._conn.execute(
            "DELETE FROM vdi WHERE uuid=:uuid",
            {"uuid": uuid})

    def update_vdi_vhd_id(self, uuid, vhd_id):
        self.__update_vdi(uuid, "vhd_id", vhd_id)

    def update_vdi_name(self, uuid, name):
        self.__update_vdi(uuid, "name", name)

    def update_vdi_description(self, uuid, description):
        self.__update_vdi(uuid, "description", description)

    def update_vdi_active_on(self, uuid, active_on):
        self.__update_vdi(uuid, "active_on", active_on)

    def update_vdi_nonpersistent(self, uuid, nonpersistent):
        self.__update_vdi(uuid, "nonpersistent", nonpersistent)

    def __update_vdi(self, uuid, key, value):
        res = self._conn.execute("""
            UPDATE vdi
               SET {} = :{}
             WHERE uuid = :uuid""".format(key, key),
            {key: value,
            "uuid": uuid}
        )

    def insert_new_vhd(self, vsize):
        return self.__insert_vhd(None, None, vsize, None)

    def insert_child_vhd(self, parent, vsize):
        return self.__insert_vhd(parent, None, vsize, None)

    def delete_vhd(self, vhd_id):
        self._conn.execute("DELETE FROM vhd WHERE id=:vhd_id",
                           {"vhd_id": vhd_id})

    def update_vhd_parent(self, vhd_id, parent):
        self.__update_vhd(vhd_id, "parent_id", parent)

    def update_vhd_vsize(self, vhd_id, vsize):
        self.__update_vhd(vhd_id, "vsize", vsize)

    def update_vhd_psize(self, vhd_id, psize):
        self.__update_vhd(vhd_id, "psize", psize)

    def update_vhd_gc_status(self, vhd_id, gc_status):
        self.__update_vhd(vhd_id, "gc_status", gc_status)

    def __update_vhd(self, vhd_id, key, value):
        res = self._conn.execute("""
            UPDATE vhd
               SET {} = :{}
             WHERE id = :vhd_id""".format(key, key),
            {key: value,
            "vhd_id": vhd_id}
        )

    def __insert_vhd(self, parent, snap, vsize, psize):
        res = self._conn.execute(
            "INSERT INTO vhd(parent_id, snap, vsize, psize) " 
            "VALUES (:parent, :snap, :vsize, :psize)",
            {"parent": parent,
             "snap": snap,
             "vsize": vsize,
             "psize": psize}
        )

        return VHD(res.lastrowid, parent, snap, vsize, psize)

    def get_vdi_by_id(self, vdi_uuid):
        res = self._conn.execute("""
            SELECT *
              FROM vdi
                   INNER JOIN vhd
                   ON vdi.vhd_id = vhd.id
             WHERE uuid = :uuid""",
            {"uuid" : vdi_uuid}
        )

        row = res.fetchone()
        if (row):
            return VDI.from_row(row)

        return None

    def get_vdi_for_vhd(self, vhd_id):
        res = self._conn.execute("""
            SELECT *
             FROM vdi
                  INNER JOIN vhd
                          ON vdi.vhd_id = vhd.id
            WHERE vdi.vhd_id = :vhd_id""",
                                 {"vhd_id": vhd_id}
        )
        row = res.fetchone()
        if (row):
            return VDI.from_row(row)

        return None

    def get_all_vdis(self):
        # name, description, active_on, nonpersistent, vhd_id, vsize
        res = self._conn.execute("""
            SELECT *
              FROM vdi
                   INNER JOIN vhd
                   ON vdi.vhd_id = vhd.id
        """)

        vdis = []
        for row in res:
            vdis.append(VDI.from_row(row))

        return vdis

    def get_children(self, vhd_id):
        res = self._conn.execute(
            "SELECT * FROM vhd WHERE parent_id=:parent", {"parent": vhd_id})
        vhds = []
        for row in res:
            vhds.append(VHD.from_row(row))
        return vhds

    def get_vhd_by_id(self, vhd_id):
        res = self._conn.execute("""
            SELECT *
              FROM vhd
             WHERE id = :id""",
            {"id": vhd_id}
        )

        row = res.fetchone()
        if (row):
            return VHD.from_row(row)

        return None

    def get_non_leaf_total_psize(self):
        """Returns the total psize of non-leaf VHDs"""
        total_psize = 0

        res = self._conn.execute("""
            SELECT psize
              FROM vhd
             WHERE psize NOT NULL
        """)

        for row in res:
            total_psize += row['psize']

        return total_psize

    def find_non_leaf_coalesceable(self):
        res = self._conn.execute("""
            SELECT * FROM 
                   (SELECT *, COUNT(id) as num 
                      FROM VHD
                     WHERE parent_id NOT NULL 
                  GROUP BY parent_id
            ) as node
             WHERE node.num = 1 
               AND node.id IN 
                   (SELECT parent_id
                      FROM VHD
                     WHERE parent_id NOT NULL 
                  GROUP BY parent_id)""")
        vhds = []
        for row in res:
            vhds.append(VHD.from_row(row))
        return vhds

    def find_leaf_coalesceable(self):
        res = self._conn.execute("""
            SELECT * FROM 
                   (SELECT *, COUNT(id) as num 
                      FROM VHD
                     WHERE parent_id NOT NULL 
                  GROUP BY parent_id
            ) as node
             WHERE node.num = 1 
               AND node.id NOT IN 
                   (SELECT parent_id
                      FROM VHD
                     WHERE parent_id NOT NULL 
                  GROUP BY parent_id)""")
        vhds = []
        for row in res:
            vhds.append(VHD.from_row(row))
        return vhds

    @contextmanager
    def write_context(self):
        with self._conn:
            yield

    def close(self):
        self._conn.close()
