import mock
import unittest
from contextlib import contextmanager

import vhd_coalesce
import VhdMetabase

@contextmanager
def test_context():
    yield

class VhdCoalesceTest(unittest.TestCase):

    @mock.patch('libvhd.VhdMetabase.VhdMetabase')
    def test_find_best_non_leaf_coalesce_no_results(self, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.find_non_leaf_coalesceable.return_value = []

        # Call the method
        child, parent = vhd_coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

        # Check the result
        self.assertEquals((None, None), (child, parent))

        # Check the calls
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        mockDB.write_context.assert_not_called()
        mockDB.update_vhd_gc_status.assert_not_called()

    @mock.patch('libvhd.VhdMetabase.VhdMetabase')
    def test_find_best_non_leaf_coalesce_success(self, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        mockDB.find_non_leaf_coalesceable.return_value = [
            VhdMetabase.VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                )
            ]
        mockDB.get_vhd_by_id.side_effect = [
            VhdMetabase.VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                ),
            VhdMetabase.VHD(
                1,
                None,
                0,
                10*1024,
                10*1024
                ),
            VhdMetabase.VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )
            ]

        # Call the method
        child, parent = vhd_coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

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

    @mock.patch('libvhd.VhdMetabase.VhdMetabase')
    def test_find_best_non_leaf_coalesce_root_coalescing(self, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        mockDB.find_non_leaf_coalesceable.return_value = [
            VhdMetabase.VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                )
            ]
        mockDB.get_vhd_by_id.side_effect = [
            VhdMetabase.VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                ),
            VhdMetabase.VHD(
                1,
                None,
                0,
                10*1024,
                10*1024,
                "Coalescing"
                )
            ]

        # Call the method
        child, parent = vhd_coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

        # Check the result
        self.assertEquals((None, None), (child, parent))

        # Check the calls
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        mockDB.write_context.assert_not_called()
        mockDB.get_vhd_by_id.assert_called()
        mockDB.update_vhd_gc_status.assert_not_called()

    @mock.patch('libvhd.VhdMetabase.VhdMetabase')
    def test_find_best_non_leaf_coalesce_node_coalescing(self, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        mockDB.find_non_leaf_coalesceable.return_value = [
            VhdMetabase.VHD(
                3,
                2,
                0,
                10*1024,
                10*1024,
                "Coalescing"
                )
            ]
        mockDB.get_vhd_by_id.side_effect = [
            VhdMetabase.VHD(
                2,
                1,
                0,
                10*1024,
                10*1024)
            ]

        # Call the method
        child, parent = vhd_coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

        # Check the result
        self.assertEquals((None, None), (child, parent))

        # Check the calls
        callbacks.volumeStartOperations.assert_called_with("test-uri", 'w')
        callbacks.volumeStopOperations.assert_called()
        mockDB.write_context.assert_not_called()
        mockDB.get_vhd_by_id.assert_not_called()
        mockDB.update_vhd_gc_status.assert_not_called()


    @mock.patch('libvhd.VhdMetabase.VhdMetabase')
    def test_find_best_non_leaf_coalesce_one_coalescing_success(self, mockMetabase):
        # Setup the mocks
        callbacks = mock.MagicMock()
        mockDB = mock.MagicMock()
        mockMetabase.return_value = mockDB
        mockDB.write_context.side_effect = test_context
        mockDB.find_non_leaf_coalesceable.return_value = [
            VhdMetabase.VHD(
                3,
                2,
                0,
                10*1024,
                10*1024,
                "Coalescing"
                ),
            VhdMetabase.VHD(
                5,
                4,
                0,
                10*1024,
                10*1024
                )
            ]
        mockDB.get_vhd_by_id.side_effect = [
            VhdMetabase.VHD(
                4,
                1,
                0,
                10*1024,
                10*1024
                ),
            VhdMetabase.VHD(
                1,
                None,
                0,
                10*1024,
                10*1024
                ),
            VhdMetabase.VHD(
                4,
                1,
                0,
                10*1024,
                10*1024
                )
            ]

        # Call the method
        child, parent = vhd_coalesce.find_best_non_leaf_coalesceable_2("test-uri", callbacks)

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
