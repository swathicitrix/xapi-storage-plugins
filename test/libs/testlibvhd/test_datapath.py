import unittest

import libvhd.datapath


class TestVHDDatapath(unittest.TestCase):

    def test__parse_uri(self):
        test_uri = "vhd+tapdisk://gfs2/TestScsiId|GfsTest"

        parsed_uri = libvhd.datapath._parse_uri(test_uri)

        self.assertEquals(("vhd:////TestScsiId", "GfsTest"), parsed_uri)

