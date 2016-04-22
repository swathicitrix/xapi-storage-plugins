'''
set the required datapath here:
1. raw+file:// -> uses raw file plus tapdisk
2. loop+blkback:// -> converts raw file to loop disk using losetup and then connects the device directly to blkback
'''

DP_URI_PREFIX = "loop+blkback://"
