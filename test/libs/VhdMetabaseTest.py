import unittest
from xapi.storage.libs import VhdMetabase

class StubVhdMetabase(VhdMetabase.VhdMetabase):

    def __init__(self):
        VhdMetabase.VhdMetabase.__init__(self, ":memory:")

    def schema_check(self):
        with self.conn:
            tables = self.conn.execute("PRAGMA table_info('VDI')")
            for row in tables:
                print row


class VhdMetabaseTest(unittest.TestCase):

    def setUp(self):
        self.subject = StubVhdMetabase()
        self.subject.create()

    def tearDown(self):
        self.subject.close()

    def test_database_create(self):
        self.subject.schema_check()
    
