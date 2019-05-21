"""
Open compressed files transparently.
"""
from __future__ import print_function, division, absolute_import

import gzip
import sys
import io
import os
import time
from subprocess import Popen, PIPE

from ._version import version as __version__


_PY3 = sys.version > '3'

if not _PY3:
    import bz2file as bz2
else:
    try:
        import bz2
    except ImportError:
        bz2 = None

try:
    import lzma
except ImportError:
    lzma = None


if _PY3:
    basestring = str

try:
    import pathlib  # Exists in Python 3.4+
except ImportError:
    pathlib = None

try:
    from os import fspath  # Exists in Python 3.6+
except ImportError:
    def fspath(path):
        if hasattr(path, "__fspath__"):
            return path.__fspath__()
        # Python 3.4 and 3.5 do not support the file system path protocol
        if pathlib is not None and isinstance(path, pathlib.Path):
            return str(path)
        return path


def _available_cpu_count():
    """
    Number of available virtual or physical CPUs on this system
    Adapted from http://stackoverflow.com/a/1006301/715090
    """
    try:
        return len(os.sched_getaffinity(0))
    except AttributeError:
        pass
    import re
    try:
        with open('/proc/self/status') as f:
            status = f.read()
        m = re.search(r'(?m)^Cpus_allowed:\s*(.*)$', status)
        if m:
            res = bin(int(m.group(1).replace(',', ''), 16)).count('1')
            if res > 0:
                return res
    except IOError:
        pass
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError, NotImplementedError):
        return 1


class Closing(object):
    """
    Inherit from this class and implement a close() method to offer context
    manager functionality.
    """
    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __del__(self):
        try:
            self.close()
        except:
            pass


class PipedGzipWriter(Closing):
    """
    Write gzip-compressed files by running an external gzip or pigz process and
    piping into it. On Python 2, this is faster than using gzip.open(). On
    Python 3, it allows to run the compression in a separate process and can
    therefore also be faster.
    """

    def __init__(self, path, mode='wt', compresslevel=6, threads=None):
        """
        mode -- one of 'w', 'wt', 'wb', 'a', 'at', 'ab'
        compresslevel -- gzip compression level
        threads (int) -- number of pigz threads. If this is set to None, a reasonable default is
            used. At the moment, this means that the number of available CPU cores is used, capped
            at four to avoid creating too many threads. Use 0 to let pigz use all available cores.
        """
        if mode not in ('w', 'wt', 'wb', 'a', 'at', 'ab'):
            raise ValueError("Mode is '{0}', but it must be 'w', 'wt', 'wb', 'a', 'at' or 'ab'".format(mode))

        # TODO use a context manager
        self.outfile = open(path, mode)
        self.devnull = open(os.devnull, mode)
        self.closed = False
        self.name = path

        kwargs = dict(stdin=PIPE, stdout=self.outfile, stderr=self.devnull)
        # Setting close_fds to True in the Popen arguments is necessary due to
        # <http://bugs.python.org/issue12786>.
        # However, close_fds is not supported on Windows. See
        # <https://github.com/marcelm/cutadapt/issues/315>.
        if sys.platform != 'win32':
            kwargs['close_fds'] = True

        if 'w' in mode and compresslevel != 6:
            extra_args = ['-' + str(compresslevel)]
        else:
            extra_args = []

        pigz_args = ['pigz']
        if threads is None:
            threads = min(_available_cpu_count(), 4)
        if threads != 0:
            pigz_args += ['-p', str(threads)]
        try:
            self.process = Popen(pigz_args + extra_args, **kwargs)
            self.program = 'pigz'
        except OSError:
            # pigz not found, try regular gzip
            try:
                self.process = Popen(['gzip'] + extra_args, **kwargs)
                self.program = 'gzip'
            except (IOError, OSError):
                self.outfile.close()
                self.devnull.close()
                raise
        except IOError:  # TODO IOError is the same as OSError on Python 3.3
            self.outfile.close()
            self.devnull.close()
            raise
        if _PY3 and 'b' not in mode:
            self._file = io.TextIOWrapper(self.process.stdin)
        else:
            self._file = self.process.stdin

    def write(self, arg):
        self._file.write(arg)

    def close(self):
        self.closed = True
        self._file.close()
        retcode = self.process.wait()
        self.outfile.close()
        self.devnull.close()
        if retcode != 0:
            raise IOError("Output {0} process terminated with exit code {1}".format(self.program, retcode))

    def __iter__(self):
        return self

    def __next__(self):
        raise io.UnsupportedOperation('not readable')


class PipedGzipReader(Closing):
    """
    Open a pipe to pigz for reading a gzipped file. Even though pigz is mostly
    used to speed up writing, when it can use many compression threads, it is
    also faster than gzip when reading (three times faster).
    """

    def __init__(self, path, mode='r'):
        """
        Raise an OSError when pigz could not be found.
        """
        if mode not in ('r', 'rt', 'rb'):
            raise ValueError("Mode is '{0}', but it must be 'r', 'rt' or 'rb'".format(mode))
        self.process = Popen(['pigz', '-cd', path], stdout=PIPE, stderr=PIPE)
        self.name = path
        if _PY3 and 'b' not in mode:
            self._file = io.TextIOWrapper(self.process.stdout)
        else:
            self._file = self.process.stdout
        if _PY3:
            self._stderr = io.TextIOWrapper(self.process.stderr)
        else:
            self._stderr = self.process.stderr
        self.closed = False
        # Give the subprocess a little bit of time to report any errors (such as
        # a non-existing file)
        time.sleep(0.01)
        self._raise_if_error()

    def close(self):
        self.closed = True
        retcode = self.process.poll()
        if retcode is None:
            # still running
            self.process.terminate()
        self._raise_if_error()

    def __iter__(self):
        for line in self._file:
            yield line
        self.process.wait()
        self._raise_if_error()

    def _raise_if_error(self):
        """
        Raise IOError if process is not running anymore and the
        exit code is nonzero.
        """
        retcode = self.process.poll()
        if retcode is not None and retcode != 0:
            message = self._stderr.read().strip()
            raise IOError(message)

    def read(self, *args):
        data = self._file.read(*args)
        if len(args) == 0 or args[0] <= 0:
            # wait for process to terminate until we check the exit code
            self.process.wait()
        self._raise_if_error()
        return data

    def readinto(self, *args):
        data = self._file.readinto(*args)
        return data


def _open_stdin_or_out(mode):
    # Do not return sys.stdin or sys.stdout directly as we want the returned object
    # to be closable without closing sys.stdout.
    std = dict(r=sys.stdin, w=sys.stdout)[mode[0]]
    if not _PY3:
        # Enforce str type on Python 2
        # Note that io.open is slower than regular open() on Python 2.7, but
        # it appears to be the only API that has a closefd parameter.
        mode = mode[0] + 'b'
    return io.open(std.fileno(), mode=mode, closefd=False)


def _open_bz2(filename, mode):
    if bz2 is None:
        raise ImportError("Cannot open bz2 files: The bz2 module is not available")
    if _PY3:
        return bz2.open(filename, mode)
    else:
        if mode[0] == 'a':
            raise ValueError("mode '{0}' not supported with BZ2 compression".format(mode))
        return bz2.BZ2File(filename, mode)


def _open_xz(filename, mode):
    if lzma is None:
        raise ImportError(
            "Cannot open xz files: The lzma module is not available (use Python 3.3 or newer)")
    return lzma.open(filename, mode)


def _open_gz(filename, mode, compresslevel, threads):
    if sys.version_info[:2] == (2, 7):
        buffered_reader = io.BufferedReader
        buffered_writer = io.BufferedWriter
    else:
        buffered_reader = lambda x: x
        buffered_writer = lambda x: x
    if _PY3:
        exc = FileNotFoundError  # was introduced in Python 3.3
    else:
        exc = OSError
    if 'r' in mode:
        try:
            return PipedGzipReader(filename, mode)
        except exc:
            # pigz is not installed
            return buffered_reader(gzip.open(filename, mode))
    else:
        try:
            return PipedGzipWriter(filename, mode, compresslevel, threads=threads)
        except exc:
            return buffered_writer(gzip.open(filename, mode, compresslevel=compresslevel))


def xopen(filename, mode='r', compresslevel=6, threads=None):
    """
    A replacement for the "open" function that can also read and write
    compressed files transparently. The supported compression formats are gzip,
    bzip2 and xz. If the filename is '-', standard output (mode 'w') or input (mode 'r') is returned.

    The file type is determined based on the filename: .gz is gzip, .bz2 is bzip2 and .xz is
    xz/lzma.

    When writing a gzip-compressed file, the following methods are tried in order to get the
    best speed 1) using a pigz (parallel gzip) subprocess; 2) using a gzip subprocess;
    3) gzip.open. A single gzip subprocess can be faster than gzip.open because it runs in a
    separate process.

    Uncompressed files are opened with the regular open().

    mode can be: 'rt', 'rb', 'at', 'ab', 'wt', or 'wb'. Also, the 't' can be omitted,
    so instead of 'rt', 'wt' and 'at', the abbreviations 'r', 'w' and 'a' can be used.

    In Python 2, the 't' and 'b' characters are ignored.

    Append mode ('a', 'at', 'ab') is not available with BZ2 compression and
    will raise an error.

    compresslevel is the gzip compression level. It is not used for bz2 and xz.

    threads is the number of threads for pigz. If left at None, then the pigz
    default is used. With pigz 2.4, this is "the number of online processors,
    or 8 if unknown".
    """
    if mode in ('r', 'w', 'a'):
        mode += 't'
    if mode not in ('rt', 'rb', 'wt', 'wb', 'at', 'ab'):
        raise ValueError("mode '{0}' not supported".format(mode))
    if not _PY3:
        mode = mode[0]
    filename = fspath(filename)
    if not isinstance(filename, basestring):
        raise ValueError("the filename must be a string")
    if compresslevel not in range(1, 10):
        raise ValueError("compresslevel must be between 1 and 9")

    if filename == '-':
        return _open_stdin_or_out(mode)
    elif filename.endswith('.bz2'):
        return _open_bz2(filename, mode)
    elif filename.endswith('.xz'):
        return _open_xz(filename, mode)
    elif filename.endswith('.gz'):
        return _open_gz(filename, mode, compresslevel, threads)
    else:
        # Python 2.6 and 2.7 have io.open, which we could use to make the returned
        # object consistent with the one returned in Python 3, but reading a file
        # with io.open() is 100 times slower (!) on Python 2.6, and still about
        # three times slower on Python 2.7 (tested with "for _ in io.open(path): pass")
        return open(filename, mode)
