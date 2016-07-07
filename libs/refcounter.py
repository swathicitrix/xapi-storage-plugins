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

    N.B.: All methods, bar reset(), will raise a 'TypeError'
          exception, unless lock() is called first or execution
          is in a 'with' statement block.
    """

    ROOT_PATH = '/var/run/nonpersistent/smapiv3/refcount'
    TYPES = frozenset(['lvm', 'iscsi', 'tapdisk'])
    LOCK_SFX = '.lock'

    def __init__(self, *entries):
        """RefCounter object init.

        Args:
            *entries (str): varargs >= 2
                1st entry: one of 'TYPES'
                last entry: refcounter name
                intermidiate entries: can be used for grouping related
                    refcounts together (e.g. VG name that contains
                    all of its LV refcounts)

        Raises:
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

        if len(entries) != len(set(entries)):
            raise ValueError(
                "Entries are not unique: '{}'".format(entries)
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
        """Get a locked RefCounter instance."""
        self.lock()
        return self

    def __exit__(self, exception_type, exception_val, trace):
        """Unlock RefCounter instance and destroy it."""
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

        self.__log("Locked.")


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
        self.__log("Unlocked.")

    def increment(self, key, func_ptr, *func_args):
        """Call func_ptr() and/or increment refcount accordingly.

        If count is 0, call 'func_ptr(*func_args)'.
        else,
            if 'key' has not been seen yet, add 'key'
            (increments count by 1).
            else, increment requests for 'key'
            (does not increment refcount).

        Args:
            key (str): uniquely identifies the refcounter user
            func_ptr (function/method): the function/method to be
                called, if the count is 0.
            *func_args (...): arguments to pass to 'func_ptr'

        Raises:
            TypeError
            (Anything that 'func_ptr' can raise)
        """
        if self.get_count() == 0:
            func_ptr(*func_args)

        try:
            self.__refcount_dict[key] += 1
            self.__log("Key '{}' exists; not incrementing.", key)
        except KeyError:
            self.__refcount_dict[key] = 1
            self.__log("Key '{}' added; count = {}.", key, self.get_count())

    def decrement(self, key, func_ptr, *func_args):
        """Call func_ptr() and/or decrement refcount accordingly.

        If count is 1, call 'func_ptr(*func_args)'.
        Delete 'key' from refcounter (decrements by 1).

        Args:
            key (str): uniquely identifies the refcounter user
            func_ptr (function/method): the function/method to be
                called, if the count is 1 and 'key' is refcounted.
            *func_args (...): arguments to pass to 'func_ptr'

        Raises:
            TypeError
            (Anything that 'func_ptr' can raise)
        """
        try:
            reqs = self.__refcount_dict[key]
        except KeyError:
            self.__log("Key '{}' not open.", key)
            return

        if self.get_count() == 1:
            func_ptr(*func_args)

        del self.__refcount_dict[key]

        self.__log(
            "Key '{}' had {} open requests before closing.",
            key,
            reqs
        )

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

        self.__log(
            "'{}' successfully reset.",
            os.path.join(*self.__entries[:idx + 1])
        )
        print "<== reset()"

    def get_count(self):
        """Returns current count for RefCounter.

        Raises:
            TypeError
        """
        return len(self.__refcount_dict)

    def will_increase(self, key):
        """Dry-run for increment().

        Args:
            key (str): uniquely identifies the refcounter user

        Raises:
            TypeError
        """
        if key in self.__refcount_dict:
            return False

        return True

    def will_decrease(self, key):
        """Dry-run for decrement().

        Args:
            key (str): uniquely identifies the refcounter user

        Raises:
            TypeError
        """
        if key in self.__refcount_dict:
            return True

        return False

    def get_open_requests(self, key):
        """Returns the number of increment() calls for 'key'.

        Args:
            key (str): uniquely identifies the refcounter user

        Raises:
            TypeError
        """
        try:
            requests = self.__refcount_dict[key]
        except KeyError:
            requests = 0

        return requests

    def __open_and_lock(self, start_i=None, stop_i=None):
        """Open '.lock' files and lock them.

        The locking is done from left to right. All locks are share
        locked, except the last one which is exclusively locked.

        Args:
            start_i (int): start index (inclusive)
            stop_i: (int) stop index (non-inclusive)

        Raises:
            OSError
        """
        if start_i is None:
            start_i = 0
        if stop_i is None:
            stop_i = len(self.__entries)

        # If we don't start from 0, we assume 'start_i - 1'
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
        """Closes open '.lock' files.

        The unlocking is done from right to left.
        'start_i' should be greater than 'stop_i'.

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
        if start_i > stop_i and -1 < stop_i < len(self.__locks) - 1:
            fcntl.flock(self.__locks[stop_i], fcntl.LOCK_EX)
            print "ex_lock: {}".format(self.__locks[stop_i].name)

    def __remove_entry(self, entry):
        """Removes filesystem entry.

        Args:
            entry (str): path to file or directory

        Raises:
            ValueError
        """
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

    def __log(self, msg, *args):
        log.debug(
            "RefCounter [{}]: ".format(os.path.join(*self.__entries)) +
            msg.format(*args)
        )
