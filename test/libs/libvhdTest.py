import mock
import unittest
from contextlib import contextmanager

import libvhd
import VhdMetabase

@contextmanager
def test_context():
    yield


class LibVhdTest(unittest.TestCase):
    
    def test_parse_datapath_uri(self):
        test_uri = "vhd+tapdisk://gfs2/TestScsiId|GfsTest"

        parsed_uri = libvhd.parse_datapath_uri(test_uri)

        self.assertEquals(("vhd:////TestScsiId", "GfsTest"), parsed_uri)

    @mock.patch('libvhd.VhdMetabase.VhdMetabase')
    @mock.patch('libvhd.vhdutil')
    @mock.patch('libvhd.xapi.storage.libs.poolhelper')
    def test_clone_refresh_datapath_success(self, poolhelper, mockVhdUtil, mockDatabase):
        callbacks = mock.MagicMock()

        mockDB = mock.MagicMock()

        mockDatabase.return_value = mockDB

        mockDB.write_context.side_effect = test_context

        mockDB.get_vdi_by_id.return_value = VhdMetabase.VDI(
            "1",
            "Test",
            "Test Desc",
            "Host1",
            0,
            VhdMetabase.VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )
            )

        mockDB.insert_child_vhd.side_effect = [
            VhdMetabase.VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                ),
            VhdMetabase.VHD(
                4,
                2,
                0,
                10*1024,
                10*1024
                )
            ]

        mockVhdUtil.is_parent_pointing_to_path.return_value = True

        clone = libvhd.clone("test", "test-sr", "test-vhd", callbacks)
        
        poolhelper.refresh_datapath_on_host.assert_called()
        # Snapshot should be called twice
        calls = [mock.ANY, mock.ANY]
        mockVhdUtil.snapshot.assert_has_calls(calls)

        callbacks.volumeStartOperations.assert_called()
        callbacks.volumeStopOperations.assert_called()

    @mock.patch('libvhd.VhdMetabase.VhdMetabase')
    @mock.patch('libvhd.vhdutil')
    @mock.patch('libvhd.xapi.storage.libs.poolhelper')
    def test_clone_not_active_success(self, poolhelper, mockVhdUtil, mockDatabase):
        callbacks = mock.MagicMock()

        mockDB = mock.MagicMock()

        mockDatabase.return_value = mockDB

        mockDB.write_context.side_effect = test_context

        mockDB.get_vdi_by_id.return_value = VhdMetabase.VDI(
            "1",
            "Test",
            "Test Desc",
            None,
            0,
            VhdMetabase.VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )
            )

        mockDB.insert_child_vhd.side_effect = [ 
            VhdMetabase.VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                ),
            VhdMetabase.VHD(
                4,
                2,
                0,
                10*1024,
                10*1024
                )
            ]

        mockVhdUtil.is_parent_pointing_to_path.return_value = True

        clone = libvhd.clone("test", "test-sr", "test-vhd", callbacks)
        
        poolhelper.refresh_datapath_on_host.assert_not_called()
        # Snapshot should be called twice
        calls = [mock.ANY, mock.ANY]
        mockVhdUtil.snapshot.assert_has_calls(calls)

        callbacks.volumeStartOperations.assert_called()
        callbacks.volumeStopOperations.assert_called()

    @mock.patch('libvhd.VhdMetabase.VhdMetabase')
    @mock.patch('libvhd.vhdutil')
    @mock.patch('libvhd.xapi.storage.libs.poolhelper')
    def test_clone_single_success(self, poolhelper, mockVhdUtil, mockDatabase):
        callbacks = mock.MagicMock()

        mockDB = mock.MagicMock()

        mockDatabase.return_value = mockDB

        mockDB.write_context.side_effect = test_context

        mockDB.get_vdi_by_id.return_value = VhdMetabase.VDI(
            "1",
            "Test",
            "Test Desc",
            None,
            0,
            VhdMetabase.VHD(
                2,
                1,
                0,
                10*1024,
                10*1024
                )
            )

        mockDB.insert_child_vhd.side_effects = [
            VhdMetabase.VHD(
                3,
                2,
                0,
                10*1024,
                10*1024
                )
            ]

        mockVhdUtil.is_parent_pointing_to_path.return_value = False

        clone = libvhd.clone("test", "test-sr", "test-vhd", callbacks)
        
        poolhelper.refresh_datapath_on_host.assert_not_called()
        # Snapshot should be called once
        mockVhdUtil.snapshot.assert_called_once()

        callbacks.volumeStartOperations.assert_called()
        callbacks.volumeStopOperations.assert_called()
