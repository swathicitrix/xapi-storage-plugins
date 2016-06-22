import mock
import unittest
from contextlib import contextmanager

from xapi.storage.libs.libvhd.metabase import VDI, VHD
from xapi.storage.libs.libvhd import coalesce

@contextmanager
def test_context():
    yield

class VHDCoalesceTest(unittest.TestCase):

    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDMetabase')
    def test_find_best_non_leaf_coalesce_no_results(self, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.find_non_leaf_coalesceable.return_value = []

        # Call the method
        child, parent = coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

        # Check the result
        self.assertEquals((None, None), (child, parent))

        # Check the calls
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        mockDB.write_context.assert_not_called()
        mockDB.update_vhd_gc_status.assert_not_called()

    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDMetabase')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.log')
    def test_find_best_non_leaf_coalesce_success(self, mocklog, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        mockDB.find_non_leaf_coalesceable.return_value = [
            VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                )
            ]
        mockDB.get_vhd_by_id.side_effect = [
            VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                ),
            VHD(
                1,
                None,
                0,
                10*1024,
                10*1024
                ),
            VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )
            ]

        # Call the method
        child, parent = coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

        # Check the result
        self.assertEquals(3, child.id)
        self.assertEquals(2, parent.id)

        # Check the calls
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        mockDB.write_context.assert_called()
        mockDB.get_vhd_by_id.assert_called()
        calls = [mock.call(3, "Coalescing"), mock.call(1, "Coalescing")]
        mockDB.update_vhd_gc_status.assert_has_calls(calls)

    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDMetabase')
    def test_find_best_non_leaf_coalesce_root_coalescing(self, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        mockDB.find_non_leaf_coalesceable.return_value = [
            VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                )
            ]
        mockDB.get_vhd_by_id.side_effect = [
            VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                ),
            VHD(
                1,
                None,
                0,
                10*1024,
                10*1024,
                "Coalescing"
                )
            ]

        # Call the method
        child, parent = coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

        # Check the result
        self.assertEquals((None, None), (child, parent))

        # Check the calls
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        mockDB.write_context.assert_not_called()
        mockDB.get_vhd_by_id.assert_called()
        mockDB.update_vhd_gc_status.assert_not_called()

    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDMetabase')
    def test_find_best_non_leaf_coalesce_node_coalescing(self, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        mockDB.find_non_leaf_coalesceable.return_value = [
            VHD(
                3,
                2,
                0,
                10*1024,
                10*1024,
                "Coalescing"
                )
            ]
        mockDB.get_vhd_by_id.side_effect = [
            VHD(
                2,
                1,
                0,
                10*1024,
                10*1024)
            ]

        # Call the method
        child, parent = coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

        # Check the result
        self.assertEquals((None, None), (child, parent))

        # Check the calls
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        mockDB.write_context.assert_not_called()
        mockDB.get_vhd_by_id.assert_not_called()
        mockDB.update_vhd_gc_status.assert_not_called()


    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDMetabase')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.log')
    def test_find_best_non_leaf_coalesce_one_coalescing_success(self, mocklog, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        mockDB.find_non_leaf_coalesceable.return_value = [
            VHD(
                3,
                2,
                0,
                10*1024,
                10*1024,
                "Coalescing"
                ),
            VHD(
                5,
                4,
                0,
                10*1024,
                10*1024
                )
            ]
        mockDB.get_vhd_by_id.side_effect = [
            VHD(
                4,
                1,
                0,
                10*1024,
                10*1024
                ),
            VHD(
                1,
                None,
                0,
                10*1024,
                10*1024
                ),
            VHD(
                4,
                1,
                0,
                10*1024,
                10*1024
                )
            ]

        # Call the method
        child, parent = coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

        # Check the result
        self.assertEquals(5, child.id)
        self.assertEquals(4, parent.id)

        # Check the calls
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        mockDB.write_context.assert_called()
        mockDB.get_vhd_by_id.assert_called()
        calls = [mock.call(5, "Coalescing"), mock.call(1, "Coalescing")]
        mockDB.update_vhd_gc_status.assert_has_calls(calls)

    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDMetabase')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDUtil.set_parent')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDUtil.coalesce')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.poolhelper')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.log')
    def test_non_leaf_coalesce_success_non_active(
            self,
            mocklog,
            mockPoolHelper,
            mock_vhdutil_coalesce,
            mock_vhdutil_set_parent,
            mockMetabase):
        # Parameters for method in test
        node = VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                )

        parent = VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )

        # Setup some mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        # This is the rot node of the tree
        mockDB.get_vhd_by_id.side_effect = [
            VHD(
                1,
                None,
                0,
                10*1024,
                10*1024
                )
            ]
        # This is the leaf VHD
        leaf_vhd = VHD(
                    4,
                    3,
                    0,
                    10*1024,
                    10*1024
                    )
        mockDB.get_children.side_effect = [           
                [leaf_vhd],
            []
            ]
        # This is the VDI for the leaf VHD
        mockDB.get_vdi_for_vhd.return_value = VDI("1", "VDI1", "", None, None, leaf_vhd)

        # Call the method
        coalesce.non_leaf_coalesce(node, parent, "test-uri", callbacks)

        # Assert that the methods we expect to be called were called
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        callbacks.volumeDestroy.assert_called()
        mock_vhdutil_coalesce.assert_called()
        mock_vhdutil_set_parent.assert_called()
        mockDB.update_vhd_parent.assert_called_with(4, 2)
        mockDB.delete_vdi.assert_called_with(3)
        # Node wasn't active so no need to refresh the datapath
        mockPoolHelper.suspend_datapath_on_host.assert_not_called()
        mockPoolHelper.resume_datapath_on_host.assert_not_called()

    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDMetabase')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDUtil.set_parent')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.VHDUtil.coalesce')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.poolhelper')
    @mock.patch('xapi.storage.libs.libvhd.coalesce.log')
    def test_non_leaf_coalesce_success_active(
            self,
            mocklog,
            mockPoolHelper,
            mock_vhdutil_coalesce,
            mock_vhdutil_set_parent,
            mockMetabase):
        # Parameters for method in test
        node = VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                )

        parent = VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )

        # Setup some mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        # This is the rot node of the tree
        mockDB.get_vhd_by_id.side_effect = [
            VHD(
                1,
                None,
                0,
                10*1024,
                10*1024
                )
            ]
        # This is the leaf VHD
        leaf_vhd = VHD(
                    4,
                    3,
                    0,
                    10*1024,
                    10*1024
                    )
        mockDB.get_children.side_effect = [           
                [leaf_vhd],
            []
            ]
        # This is the VDI for the leaf VHD
        mockDB.get_vdi_for_vhd.return_value = VDI("1", "VDI1", "", "Host1", None, leaf_vhd)

        # Call the method
        coalesce.non_leaf_coalesce(node, parent, "test-uri", callbacks)

        # Assert that the methods we expect to be called were called
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        callbacks.volumeDestroy.assert_called()
        mock_vhdutil_coalesce.assert_called()
        mock_vhdutil_set_parent.assert_called()
        mockDB.update_vhd_parent.assert_called_with(4, 2)
        mockDB.delete_vdi.assert_called_with(3)
        # Node was active so need to refresh the datapath on the correct host
        mockPoolHelper.suspend_datapath_on_host.assert_called_with("GC", "Host1", mock.ANY)
        mockPoolHelper.resume_datapath_on_host.assert_called_with("GC", "Host1", mock.ANY)
