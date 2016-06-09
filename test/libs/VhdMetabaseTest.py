import unittest
import uuid
from xapi.storage.libs import VhdMetabase

class StubVhdMetabase(VhdMetabase.VhdMetabase):

    def __init__(self):
        VhdMetabase.VhdMetabase.__init__(self, ":memory:")

    def schema_check(self):
        with self._conn:
            tables = self._conn.execute("PRAGMA table_info('VDI')")
            for row in tables:
                print row

    def populate_test_set_1(self):
        with self.write_context():
            id = self.insert_new_vdi("VDI1", "First VDI", str(uuid.uuid4()), 10*1024)
            id = self.insert_new_vdi("VDI2", "Second VDI", None, 10*1024)
            id = self.insert_child_vdi(id, "Child1", "First Child VDI", str(uuid.uuid4()), 10*1024)
            pass

class VhdMetabaseTest(unittest.TestCase):

    def setUp(self):
        self.subject = StubVhdMetabase()
        self.subject.create()

    def tearDown(self):
        self.subject.close()

    def test_database_create(self):
        self.subject.schema_check()
    
    def test_vdi_get_by_id_success(self):
        self.subject.populate_test_set_1()
        vdi = self.subject.get_vdi_by_id(1)

        self.assertEquals(1, vdi.key)
        self.assertEquals("VDI1", vdi.name)
        self.assertEquals("First VDI", vdi.description)
        self.assertEquals(None, vdi.parent)
        self.assertEquals(0, vdi.snap)
        self.assertEquals(str(10*1024), vdi.vsize)

    def test_vdi_get_by_missing_id_failure(self):
        self.subject.populate_test_set_1()
        vdi = self.subject.get_vdi_by_id(1000)

        self.assertEquals(None, vdi)
