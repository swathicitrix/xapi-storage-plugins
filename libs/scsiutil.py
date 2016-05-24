import os
import re
import glob
from xapi.storage.common import call
from xapi.storage import log
import xapi.storage.libs.poolhelper

SCSI_ID_BIN = '/usr/lib/udev/scsi_id'
SECTOR_SHIFT = 9

def match_dm(s):
    regex = re.compile("mapper/")
    return regex.search(s, 0)

def match_vendor(s):
    regex = re.compile("^Vendor:")
    return regex.search(s, 0)

def getdev(path):
    realpath = os.path.realpath(path)
    if match_dm(realpath):
        newpath = realpath.replace("/dev/mapper/","/dev/disk/by-id/scsi-")
    else:
        newpath = path
    return os.path.realpath(newpath).split('/')[-1]

def rawdev(dev):
    device = getdev(dev)
    if device.startswith('dm-') and device[3:].isdigit():
        return device

    return re.sub('[0-9]*$', '', device)

def getsize(dbg, path):
    dev = getdev(path)
    sysfs = os.path.join('/sys/block',dev,'size')
    size = 0
    if os.path.exists(sysfs):
        try:
            f=open(sysfs, 'r')
            size = (long(f.readline()) << SECTOR_SHIFT)
            f.close()
        except:
            pass
    return size

def SCSIid_sanitise(str):
    text = re.sub("^\s+","",str)
    return re.sub("\s+","_",text)

def getSCSIid(dbg, path):
    """Get the SCSI id of a block device

        Input:
            path -- (str) path to block device; can be symlink

        Return:
            scsi_id -- (str) the device's SCSI id

        Raise:
            util.CommandException
    """

    try:
        stdout = call(dbg, [SCSI_ID_BIN, '-g', '--device', path])
    except: # fallback call
        dev = rawdev(path)
        stdout = call(dbg, [SCSI_ID_BIN, '-g', '-s', '/block/%s' % dev])

    return SCSIid_sanitise(stdout[:-1])

def getserial(dbg, path):
    dev = os.path.join('/dev',getdev(path))
    try:
        cmd = ["sginfo", "-s", dev]
        text = re.sub("\s+","",call(dbg, cmd))
    except:
        raise xapi.storage.api.volume.Unimplemented(
              "An error occured querying device serial number [%s]" % dev)
    try:
        return text.split("'")[1]
    except:
        return ''

def getmanufacturer(dbg, path):
    cmd = ["sginfo", "-M", path]
    try:
        for line in filter(match_vendor, call(dbg, cmd).split('\n')):
            return line.replace(' ','').split(':')[-1]
    except:
        return ''
