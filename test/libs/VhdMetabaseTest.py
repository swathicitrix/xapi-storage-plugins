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

    def test_vhd_get_by_missing_id_failure(self):
        self.subject.populate_test_set_1()
        vhd = self.subject.get_vhd_by_id(1000)

        self.assertEquals(None, vhd)

    def test_get_all_vdi_success(self):
        self.subject.populate_test_set_1()
        vdis = self.subject.get_all_vdis()

        self.assertEquals(2, len(vdis))

    def test_update_vhd_psize_success(self):
        self.subject.populate_test_set_1()
        vhd = self.subject.get_vhd_by_id(1)
        psize = vhd.psize

        self.subject.update_vhd_psize(1, 25*1024)
        vhd = self.subject.get_vhd_by_id(1)

        self.assertEquals(25*1024, vhd.psize)

    def test_update_vhd_vsize_success(self):
        self.subject.populate_test_set_1()
        vhd = self.subject.get_vhd_by_id(1)
        psize = vhd.psize

        self.subject.update_vhd_vsize(1, 25*1024)
        vhd = self.subject.get_vhd_by_id(1)

        self.assertEquals(25*1024, vhd.vsize)

    def test_update_vdi_name_success(self):
        self.subject.populate_test_set_1()
        self.subject.update_vdi_name(1, "FirstRenamedVDI");
        
        vdi = self.subject.get_vdi_by_id(1)

        self.assertEquals("FirstRenamedVDI", vdi.name)

    def test_update_vdi_description_success(self):
        self.subject.populate_test_set_1()
        self.subject.update_vdi_description(1, "First Renamed VDI");
        
        vdi = self.subject.get_vdi_by_id(1)

        self.assertEquals("First Renamed VDI", vdi.description)

    def test_update_vdi_active_on_success(self):
        self.subject.populate_test_set_1()
        self.subject.update_vdi_active_on(1, "Host1");
        
        vdi = self.subject.get_vdi_by_id(1)

        self.assertEquals("Host1", vdi.active_on)

        # And clear it again
        self.subject.update_vdi_active_on(1, None);

        vdi = self.subject.get_vdi_by_id(1)

        self.assertEquals(None, vdi.active_on)

    def test_update_vdi_nonpersistent_success(self):
        self.subject.populate_test_set_1()
        self.subject.update_vdi_nonpersistent(1, 1);
        
        vdi = self.subject.get_vdi_by_id(1)

        self.assertEquals(1, vdi.nonpersistent)

        # And clear it again
        self.subject.update_vdi_nonpersistent(1, None);
        
        vdi = self.subject.get_vdi_by_id(1)

        self.assertEquals(None, vdi.nonpersistent)

    def test_update_vdi_vhd_id_success(self):
        self.subject.populate_test_set_1()
        self.subject.insert_child_vhd(3, 20*1024)
        self.subject.update_vdi_vhd_id(2, 4);
        
        vdi = self.subject.get_vdi_by_id(2)

        self.assertEquals(4, vdi.vhd.id)

    def test_update_vhd_parent_success(self):
        self.subject.populate_test_set_1()
        self.subject.insert_child_vhd(3, 20*1024)
  
        self.subject.update_vhd_parent(4, 2);
        
        vhd = self.subject.get_vhd_by_id(4)

        self.assertEquals(2, vhd.parent_id)

    def test_get_children_with_results(self):
        self.subject.populate_test_set_1()

        vhds = self.subject.get_children(2)
        self.assertEquals(1, len(vhds))

    def test_get_children_no_results(self):
        self.subject.populate_test_set_1()

        vhds = self.subject.get_children(1)
        self.assertEquals(0, len(vhds))

    def test_delete_vdi_success(self):
        self.subject.populate_test_set_1()

        self.subject.delete_vdi(2)

        vdis = self.subject.get_all_vdis()
        self.assertEquals(1, len(vdis))

    def test_delete_vhd_success(self):
        self.subject.populate_test_set_1()

        self.subject.insert_child_vhd(2, 24*1024)
        children = self.subject.get_children(2)
        self.assertEquals(2, len(children))

        self.subject.delete_vhd(4)
        children = self.subject.get_children(2)
        self.assertEquals(1, len(children))

 
