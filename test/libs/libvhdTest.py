import unittest

from libvhd import *

class LibVhdTest(unittest.TestCase):
    
    def test_parse_datapath_uri(self):
        test_uri = "vhd+tapdisk://gfs2/TestScsiId|GfsTest"

        parsed_uri = parse_datapath_uri(test_uri)

        self.assertEquals(("vhd:////TestScsiId", "GfsTest"), parsed_uri)
