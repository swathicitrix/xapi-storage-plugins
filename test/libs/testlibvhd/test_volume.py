import mock
import unittest
from contextlib import contextmanager

from libvhd.metabase import VHD, VDI
import libvhd.datapath

import libvhd.volume

@contextmanager
def test_context():
    yield


class TestVHDVolume(unittest.TestCase):
    
    @mock.patch('libvhd.volume.VHDMetabase')
    @mock.patch('libvhd.volume.VHDUtil')
    @mock.patch('libvhd.datapath.poolhelper')
    def test_clone_refresh_datapath_success(self, poolhelper, mockVHDUtil, mockDatabase):
        callbacks = mock.MagicMock()

        mockDB = mock.MagicMock()

        mockDatabase.return_value = mockDB

        mockDB.write_context.side_effect = test_context

        mockDB.get_vdi_by_id.return_value = VDI(
            "1",
            "Test",
            "Test Desc",
            "Host1",
            0,
            VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )
            )

        mockDB.insert_child_vhd.side_effect = [
            VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                ),
            VHD(
                4,
                2,
                0,
                10*1024,
                10*1024
                )
            ]

        mockVHDUtil.is_parent_pointing_to_path.return_value = True

        clone = libvhd.volume.VHDVolume.clone("test", "test-sr", "test-vhd", callbacks)
        
        poolhelper.refresh_datapath_on_host.assert_called()
        # Snapshot should be called twice
        calls = [mock.ANY, mock.ANY]
        mockVHDUtil.snapshot.assert_has_calls(calls)

        callbacks.volumeStartOperations.assert_called()
        callbacks.volumeStopOperations.assert_called()

    @mock.patch('libvhd.volume.VHDMetabase')
    @mock.patch('libvhd.volume.VHDUtil')
    @mock.patch('libvhd.datapath.poolhelper')
    def test_clone_not_active_success(self, poolhelper, mockVHDUtil, mockDatabase):
        callbacks = mock.MagicMock()

        mockDB = mock.MagicMock()

        mockDatabase.return_value = mockDB

        mockDB.write_context.side_effect = test_context

        mockDB.get_vdi_by_id.return_value = VDI(
            "1",
            "Test",
            "Test Desc",
            None,
            0,
            VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )
            )

        mockDB.insert_child_vhd.side_effect = [ 
            VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                ),
            VHD(
                4,
                2,
                0,
                10*1024,
                10*1024
                )
            ]

        mockVHDUtil.is_parent_pointing_to_path.return_value = True

        clone = libvhd.volume.VHDVolume.clone("test", "test-sr", "test-vhd", callbacks)
        
        poolhelper.refresh_datapath_on_host.assert_not_called()
        # Snapshot should be called twice
        calls = [mock.ANY, mock.ANY]
        mockVHDUtil.snapshot.assert_has_calls(calls)

        callbacks.volumeStartOperations.assert_called()
        callbacks.volumeStopOperations.assert_called()

    @mock.patch('libvhd.volume.VHDMetabase')
    @mock.patch('libvhd.volume.VHDUtil')
    @mock.patch('libvhd.datapath.poolhelper')
    def test_clone_single_success(self, poolhelper, mockVHDUtil, mockDatabase):
        callbacks = mock.MagicMock()

        mockDB = mock.MagicMock()

        mockDatabase.return_value = mockDB

        mockDB.write_context.side_effect = test_context

        mockDB.get_vdi_by_id.return_value = VDI(
            "1",
            "Test",
            "Test Desc",
            None,
            0,
            VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )
            )

        mockDB.insert_child_vhd.side_effects = [
            VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                )
            ]

        mockVHDUtil.is_parent_pointing_to_path.return_value = False

        clone = libvhd.volume.VHDVolume.clone("test", "test-sr", "test-vhd", callbacks)
        
        poolhelper.refresh_datapath_on_host.assert_not_called()
        # Snapshot should be called once
        mockVHDUtil.snapshot.assert_called_once()

        callbacks.volumeStartOperations.assert_called()
        callbacks.volumeStopOperations.assert_called()
