from __future__ import absolute_import
import os
import errno
import fcntl
import cPickle
import shutil

from xapi.storage import log

# pragma pylint: disable=anomalous-backslash-in-string
IQN_RE = (
    '(iqn\.(?:19[89][0-9]|20[0-9][0-9])-(?:0[1-9]|1[0-2])'
    '\.[a-z0-9\.\-]*:?[a-z0-9\.\-:]*)'
)
# pragma pylint: enable=anomalous-backslash-in-string

class RefCounter(object):
    """RefCounter class.

    RefCounter objects are used to keep track of actions that may be
    called more than 1 times by different actors.

    N.B.: All methods, bar reset(), will fail with some kind of
          exception, unless lock() is called first or execution
          is in a 'with' statement block.
    """

    ROOT_PATH = '/var/run/nonpersistent/smapiv3/refcount'
    TYPES = frozenset(['lvm', 'iscsi', 'tapdisk'])
    LOCK_SFX = '.lock'

    def __init__(self, *entries):
        """RefCounter object init.

        Args:
            *entries: (varargs >= 2)(str)
                1st entry: one of 'TYPES'
                last entry: refcounter name
                intermidiate entries: can be used for grouping related
                    refcounts together (e.g. VG name that contains
                    all of its LV refcounts)

        Raise:
            TypeError
            ValueError
        """

        if len(entries) < 2:
            raise TypeError(
                "__init__() takes at least 2 arguments ({} given)".format(
                    len(entries)
                )
            )

        if entries[0] not in RefCounter.TYPES:
            raise ValueError(
                "1st entry not in '{}': {}".format(
                    list(RefCounter.TYPES),
                    entries[0]
                )
            )

        # TODO: Make sure they are 'filesystem friendly'
        self.__entries = entries

        # Lock file objects.
        self.__locks = [None] * len(self.__entries)

        self.__refcounter_path = os.path.join(
            RefCounter.ROOT_PATH,
            *self.__entries
        )

        # Keeps refcounter in memory
        self.__refcount_dict = None

    def __enter__(self):
        self.lock()
        return self

    def __exit__(self, exception_type, exception_val, trace):
        self.unlock()

    def lock(self):
        """Lock refcount file and copy its contents in memory.

        This method is idempotent.
        """
        if self.__locks[-1] is not None:
            return

        self.__open_and_lock()

        with open(self.__refcounter_path, 'a+') as f:
            try:
                self.__refcount_dict = cPickle.load(f)
            except EOFError:
                self.__refcount_dict = {}


    def unlock(self):
        """Dump in-memory refcount contents to file and unlock it.

        This method is idempotent.
        """
        if self.__locks[-1] is None:
            return

        if self.get_count() == 0:
            self.reset()
        else:
            with open(self.__refcounter_path, 'w') as f:
                cPickle.dump(self.__refcount_dict, f, cPickle.HIGHEST_PROTOCOL)

        self.__unlock_and_close()
        self.__refcount_dict = None

    def increment(self, key, func_ptr, *func_args):
        """Call func_ptr(*func_args). Increment refcount accordingly.

        If count == 0, call 'func_ptr(*func_args)'.
        If count > 0:
            if 'key' has not been seen yet, add 'key' (increments by 1).
            else, increment requests for 'key' (does not increment refcount).

        """
        if self.get_count() == 0:
            func_ptr(*func_args)

        try:
            self.__refcount_dict[key] += 1
        except KeyError:
            self.__refcount_dict[key] = 1


    def decrement(self, key, func_ptr, *func_args):
        """

        If count == 1, call 'func_ptr(*func_args)'.
        Delete 'key' from refcounter (decrements by 1).
        """
        try:
            reqs = self.__refcount_dict[key]
        except KeyError:
            RefCounter.__log("Key '{}' not opened.", key)
            return

        if self.get_count() == 1:
            func_ptr(*func_args)
            RefCounter.__log(
                "Key '{}' had {} open requests before closing.",
                key,
                reqs
            )

        del self.__refcount_dict[key]

    def get_count(self):
        """Returns current count for RefCounter."""
        return len(self.__refcount_dict)

    def will_increase(self, key):
        """Dry-run for increment()."""
        if key in self.__refcount_dict:
            return False

        return True

    def will_decrease(self, key):
        """Dry-run for decrement()."""
        if key in self.__refcount_dict:
            return True

        return False

    def get_open_requests(self, key):
        """Returns the number of increment() calls for 'key'."""
        try:
            requests = self.__refcount_dict[key]
        except KeyError:
            requests = 0

        return requests

    def __open_and_lock(self, start_i=None, stop_i=None):
        """Open '.lock' files and lock them.
        """
        if start_i is None:
            start_i = 0
        if stop_i is None:
            stop_i = len(self.__entries)

        # If we don't start from 0, that means 'start_i - 1'
        # is ex_locked; sh_lock it and continue.
        if 0 < start_i < len(self.__locks):
            fcntl.flock(self.__locks[start_i - 1], fcntl.LOCK_SH)
            print "sh_lock: {}".format(self.__locks[start_i - 1].name)

        incremental_path = os.path.join(
            RefCounter.ROOT_PATH,
            *self.__entries[:start_i]
        )
        for i in xrange(start_i, stop_i):
            lock_path = os.path.join(
                incremental_path,
                self.__entries[i] + RefCounter.LOCK_SFX
            )

            try:
                os.makedirs(incremental_path, 0o644)
            except OSError as exc:
                if exc.errno == errno.EEXIST:
                    if os.path.isfile(incremental_path):
                        self.__unlock_and_close(i - 1)
                        raise OSError(
                            "Cannot create RefCounter group '{}'; RefCounter "
                            "file with the same name already exists.".format(
                                self.__entries[i]
                            )
                        )
                else:
                    raise


            self.__locks[i] = open(lock_path, 'a+')
            fcntl.flock(self.__locks[i], fcntl.LOCK_SH)
            print "open_and_sh_lock: {}".format(self.__locks[i].name)

            incremental_path = os.path.join(
                incremental_path,
                self.__entries[i]
            )

        # The last entry we lock is allowed to be
        # a directory only when called by reset()
        if stop_i == len(self.__entries) and os.path.isdir(incremental_path):
            self.__unlock_and_close()
            raise OSError(
                "Cannot create RefCounter file '{}'; RefCounter group with "
                "the same name already exists.".format(self.__entries[i])
            )

        if stop_i - start_i > 0:
            fcntl.flock(self.__locks[stop_i - 1], fcntl.LOCK_EX)
            print "ex_lock: {}".format(self.__locks[stop_i - 1].name)

    def __unlock_and_close(self, start_i=None, stop_i=None):
        """

        N.B.: Closing of the locks is done from right to left.
              start_i > stop_i
              (xrange(start_i, stop_i, -1))
        Args:
            start_i: (int) start index (inclusive)
            stop_i: (int) stop index (non-inclusive)
        """
        if start_i is None:
            start_i = len(self.__locks) - 1
        if stop_i is None:
            stop_i = -1

        for i in xrange(start_i, stop_i, -1):
            self.__locks[i].close()
            print "unlock_and_close: {}".format(self.__locks[i].name)
            self.__locks[i] = None

        # Move the ex_lock to the last
        # open lock, if there is one.
        if -1 < stop_i < len(self.__locks) - 1:
            fcntl.flock(self.__locks[stop_i], fcntl.LOCK_EX)
            print "ex_lock: {}".format(self.__locks[stop_i].name)


    def reset(self, entry=None):
        """Resets all refcounters from 'entry' and forwards.

        WARNING:
        The 'entry' requested to be reset MUST NOT be present in any
        other locked RefCount instance in the same calling process,
        or the process will deadlock.

        Args:
            entry (str/None): If 'entry' is None, the refcounter
                is removed.
        """
        print "==> reset()"
        if entry is None:
            entry = self.__entries[-1]

        idx = self.__entries.index(entry)

        if self.__locks[-1] is None:
            self.__open_and_lock(0, idx + 1)
            self.__remove_entry(entry)
            self.__unlock_and_close(idx)
        else:
            self.__unlock_and_close(len(self.__entries) - 1, idx)
            self.__remove_entry(entry)
            self.__open_and_lock(idx + 1)
            self.__refcount_dict = {}
        print "<== reset()"

    def __remove_entry(self, entry):
        """Removes filesystem entry."""
        idx = self.__entries.index(entry)

        path = os.path.join(RefCounter.ROOT_PATH, *self.__entries[:idx + 1])

        try:
            os.unlink(path)
        except OSError as exc:
            if exc.errno == errno.ENOENT:
                pass
            elif exc.errno == errno.EISDIR:
                shutil.rmtree(path)
            else:
                raise

    @staticmethod
    def __log(msg, *args):
        log.debug("RefCounter: " + msg.format(*args))
