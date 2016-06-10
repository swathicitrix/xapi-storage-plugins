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
        """
        TBD: Describe the data set
        """
        with self.write_context():
            vhd = self.insert_new_vhd(10*1024)
            self.insert_vdi("VDI1", "First VDI", str(1), vhd.id)

            parent = self.insert_new_vhd(20*1024)
            vhd = self.insert_child_vhd(parent.id, 20*1024)
            self.insert_vdi("Child1", "First Child VDI", str(2), vhd.id)

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
        vdi = self.subject.get_vdi_by_id("1")

        self.assertEquals(str(1), vdi.uuid)
        self.assertEquals(1, vdi.vhd.id)
        self.assertEquals("VDI1", vdi.name)
        self.assertEquals("First VDI", vdi.description)
        self.assertEquals(None, vdi.vhd.parent_id)
        self.assertEquals(None, vdi.vhd.snap)
        self.assertEquals(10*1024, vdi.vhd.vsize)

    def test_vdi_get_by_missing_id_failure(self):
        self.subject.populate_test_set_1()
        vdi = self.subject.get_vdi_by_id(1000)

        self.assertEquals(None, vdi)

    def test_get_all_vdi_success(self):
        self.subject.populate_test_set_1()
        vdis = self.subject.get_all_vdis()

        self.assertEquals(2, len(vdis))
