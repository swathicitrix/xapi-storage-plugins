#!/usr/bin/env python

import sqlite3

class VhdMetabase(object):

    def __init__(self, path):
        self.path = path
        self.connect()

    def connect(self):
        self.conn = sqlite3.connect(self.path)

    def create(self):
        with self.conn:
            self.conn.execute("create table VDI(key integer primary key, snap int,"
                         "parent int, name text, description text, vsize text,"
                         "uuid text, active_on text, gc_status text, nonpersistent integer)")
            # TODO: define indexes, parent, uuid, (active_on?)

    def close(self):
        self.conn.close()
