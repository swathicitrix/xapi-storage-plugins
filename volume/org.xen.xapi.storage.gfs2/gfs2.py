import urlparse
import errno
import os
import fcntl
from xapi.storage import log

class Callbacks():
    def volumeCreate(self, opq, name, size):
        log.debug("volumeCreate opq=%s name=%s size=%d" % (opq, name, size))
        vol_dir = os.path.join(opq, name)
        vol_path = os.path.join(vol_dir, name)
        os.makedirs(vol_dir, mode=0755)
        open(vol_path, 'a').close()
        return vol_path
    def volumeDestroy(self, opq, name):
        log.debug("volumeDestroy opq=%s name=%s" % (opq, name))
        vol_dir = os.path.join(opq, name)
        vol_path = os.path.join(vol_dir, name)
        try:
            os.unlink(vol_path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                pass
            else:
                raise
        try:
            os.rmdir(vol_dir)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                pass
            else:
                raise
    def volumeGetPath(self, opq, name):
        log.debug("volumeGetPath opq=%s name=%s" % (opq, name))
        return os.path.join(opq, name, name)
    def volumeActivateLocal(self, opq, name):
        pass
    def volumeDeactivateLocal(self, opq, name):
        pass
    def volumeRename(self, opq, old_name, new_name):
        log.debug("volumeRename opq=%s old=%s new=%s" % (opq, old_name, new_name))
        os.rename(os.path.join(opq, old_name),
                  os.path.join(opq, new_name))
        os.rename(os.path.join(opq, new_name, old_name),
                  os.path.join(opq, new_name, new_name))
        return os.path.join(opq, new_name, new_name)
    def volumeResize(self, opq, name, new_size):
        pass
    def volumeGetPhysSize(self, opq, name):
        stat = os.stat(os.path.join(opq, name, name))
        return stat.st_blocks * 512
    def volumeStartOperations(self, sr, mode):
        return urlparse.urlparse(sr).path
        import sr
        opq = sr.getSRpath("dbg", sr)
        return opq
    def volumeStopOperations(self, opq):
        pass
    def volumeMetadataGetPath(self, opq):
        return os.path.join(opq, "sqlite3-metadata.db")
    def getVolumeURI(self, opq, name):
        return "gfs2/" + opq + "|" + name
    def volumeLock(self, opq, name):
        log.debug("volumeLock opq=%s name=%s" % (opq, name))
        vol_path = os.path.join(opq, name)
        lock = open(vol_path, 'w+')
        fcntl.flock(lock, fcntl.LOCK_EX)
        return lock
    def volumeUnlock(self, opq, lock):
        log.debug("volumeUnlock opq=%s" % opq)
        fcntl.flock(lock, fcntl.LOCK_UN)
        lock.close
