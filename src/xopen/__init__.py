"""
Open compressed files transparently.
"""

__all__ = ["xopen", "PipedGzipWriter", "PipedGzipReader", "__version__"]

import gzip
import sys
import io
import os
import bz2
import time
import stat
import signal
import pathlib
from subprocess import Popen, PIPE

from ._version import version as __version__


try:
    import lzma
except ImportError:
    lzma = None


try:
    from os import fspath  # Exists in Python 3.6+
except ImportError:
    def fspath(path):
        if hasattr(path, "__fspath__"):
            return path.__fspath__()
        # Python 3.4 and 3.5 have pathlib, but do not support the file system
        # path protocol
        if pathlib is not None and isinstance(path, pathlib.Path):
            return str(path)
        if not isinstance(path, str):
            raise TypeError("path must be a string")
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
    except OSError:
        pass
    try:
        import multiprocessing
        return multiprocessing.cpu_count()
    except (ImportError, NotImplementedError):
        return 1


class Closing:
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
        except Exception:
            pass


class PipedGzipWriter(Closing):
    """
    Write gzip-compressed files by running an external gzip or pigz process and
    piping into it. pigz is tried first. It is fast because it can compress using
    multiple cores.

    If pigz is not available, a gzip subprocess is used. On Python 2, this saves
    CPU time because gzip.GzipFile is slower. On Python 3, gzip.GzipFile is on
    par with gzip itself, but running an external gzip can still reduce wall-clock
    time because the compression happens in a separate process.
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
            raise ValueError(
                "Mode is '{}', but it must be 'w', 'wt', 'wb', 'a', 'at' or 'ab'".format(mode))

        # TODO use a context manager
        self.outfile = open(path, mode)
        self.devnull = open(os.devnull, mode)
        self.closed = False
        self.name = path

        if threads is None:
            threads = min(_available_cpu_count(), 4)
        try:
            self.process, self.program = self._open_process(
                mode, compresslevel, threads, self.outfile, self.devnull)
        except OSError:
            self.outfile.close()
            self.devnull.close()
            raise

        if 'b' not in mode:
            self._file = io.TextIOWrapper(self.process.stdin)
        else:
            self._file = self.process.stdin

    @staticmethod
    def _open_process(mode, compresslevel, threads, outfile, devnull):
        pigz_args = ['pigz']
        if threads != 0:
            pigz_args += ['-p', str(threads)]
        extra_args = []
        if 'w' in mode and compresslevel != 6:
            extra_args += ['-' + str(compresslevel)]

        kwargs = dict(stdin=PIPE, stdout=outfile, stderr=devnull)

        # Setting close_fds to True in the Popen arguments is necessary due to
        # <http://bugs.python.org/issue12786>.
        # However, close_fds is not supported on Windows. See
        # <https://github.com/marcelm/cutadapt/issues/315>.
        if sys.platform != 'win32':
            kwargs['close_fds'] = True

        try:
            process = Popen(pigz_args + extra_args, **kwargs)
            program = 'pigz'
        except OSError:  # TODO Use FileNotFound instead (Python 3)
            # pigz not found, try regular gzip
            process = Popen(['gzip'] + extra_args, **kwargs)
            program = 'gzip'
        return process, program

    def write(self, arg):
        self._file.write(arg)

    def close(self):
        if self.closed:
            return
        self.closed = True
        self._file.close()
        retcode = self.process.wait()
        self.outfile.close()
        self.devnull.close()
        if retcode != 0:
            raise OSError(
                "Output {} process terminated with exit code {}".format(self.program, retcode))

    def __iter__(self):
        return self

    def __next__(self):
        raise io.UnsupportedOperation('not readable')


class PipedGzipReader(Closing):
    """
    Open a pipe to pigz for reading a gzipped file. Even though pigz is mostly
    used to speed up writing by using many compression threads, it is
    also faster when reading, even when forced to use a single thread
    (ca. 2x speedup).
    """

    def __init__(self, path, mode='r', threads=None):
        """
        Raise an OSError when pigz could not be found.
        """
        if mode not in ('r', 'rt', 'rb'):
            raise ValueError("Mode is '{}', but it must be 'r', 'rt' or 'rb'".format(mode))

        pigz_args = ['pigz', '-cd', path]

        if threads is None:
            # Single threaded behaviour by default because:
            # - Using a single thread to read a file is the least unexpected
            #   behaviour. (For users of xopen, who do not know which backend is used.)
            # - There is quite a substantial overhead (+25% CPU time) when
            #   using multiple threads while there is only a 10% gain in wall
            #   clock time.
            threads = 1

        pigz_args += ['-p', str(threads)]

        self.process = Popen(pigz_args, stdout=PIPE, stderr=PIPE)
        self.name = path
        if 'b' not in mode:
            self._file = io.TextIOWrapper(self.process.stdout)
        else:
            self._file = self.process.stdout
        self._stderr = io.TextIOWrapper(self.process.stderr)
        self.closed = False
        # Give the subprocess a little bit of time to report any errors (such as
        # a non-existing file)
        time.sleep(0.01)
        self._raise_if_error()

    def close(self):
        if self.closed:
            return
        self.closed = True
        retcode = self.process.poll()
        if retcode is None:
            # still running
            self.process.terminate()
            allow_sigterm = True
        else:
            allow_sigterm = False
        self.process.wait()
        self._file.close()
        self._raise_if_error(allow_sigterm=allow_sigterm)
        self._stderr.close()

    def __iter__(self):
        return self

    def __next__(self):
        return self._file.__next__()

    def _raise_if_error(self, allow_sigterm=False):
        """
        Raise IOError if process is not running anymore and the exit code is
        nonzero. If allow_sigterm is set and a SIGTERM exit code is
        encountered, no error is raised.
        """
        retcode = self.process.poll()
        if (
            retcode is not None and retcode != 0
            and not (allow_sigterm and retcode == -signal.SIGTERM)
        ):
            message = self._stderr.read().strip()
            self._file.close()
            self._stderr.close()
            raise OSError("{} (exit code {})".format(message, retcode))

    def read(self, *args):
        return self._file.read(*args)

    def readinto(self, *args):
        return self._file.readinto(*args)

    def readline(self, *args):
        return self._file.readline(*args)

    def seekable(self):
        return self._file.seekable()

    def peek(self, n=None):
        return self._file.peek(n)

    def readable(self):
        return self._file.readable()

    def writable(self):
        return self._file.writable()

    def flush(self):
        return None


def _open_stdin_or_out(mode):
    # Do not return sys.stdin or sys.stdout directly as we want the returned object
    # to be closable without closing sys.stdout.
    std = dict(r=sys.stdin, w=sys.stdout)[mode[0]]
    return open(std.fileno(), mode=mode, closefd=False)


def _open_bz2(filename, mode):
    return bz2.open(filename, mode)


def _open_xz(filename, mode):
    if lzma is None:
        raise ImportError(
            "Cannot open xz files: The lzma module is not available (use Python 3.3 or newer)")
    return lzma.open(filename, mode)


def _open_gz(filename, mode, compresslevel, threads):
    if threads != 0:
        try:
            if 'r' in mode:
                return PipedGzipReader(filename, mode, threads=threads)
            else:
                return PipedGzipWriter(filename, mode, compresslevel, threads=threads)
        except FileNotFoundError:
            pass  # We try without threads.

    if 'r' in mode:
        return gzip.open(filename, mode)
    else:
        return gzip.open(filename, mode, compresslevel=compresslevel)


def _detect_format_from_content(filename):
    """
    Attempts to detect file format from the content by reading the first
    6 bytes. Returns None if no format could be detected.
    """
    try:
        if stat.S_ISREG(os.stat(filename).st_mode):
            with open(filename, "rb") as fh:
                bs = fh.read(6)
            if bs[:2] == b'\x1f\x8b':
                # https://tools.ietf.org/html/rfc1952#page-6
                return "gz"
            elif bs[:3] == b'\x42\x5a\x68':
                # https://en.wikipedia.org/wiki/List_of_file_signatures
                return "bz2"
            elif bs[:6] == b'\xfd\x37\x7a\x58\x5a\x00':
                # https://tukaani.org/xz/xz-file-format.txt
                return "xz"
    except OSError:
        return None


def _detect_format_from_extension(filename):
    """
    Attempts to detect file format from the filename extension.
    Returns None if no format could be detected.
    """
    if filename.endswith('.bz2'):
        return "bz2"
    elif filename.endswith('.xz'):
        return "xz"
    elif filename.endswith('.gz'):
        return "gz"
    else:
        return None


def xopen(filename, mode='r', compresslevel=6, threads=None):
    """
    A replacement for the "open" function that can also read and write
    compressed files transparently. The supported compression formats are gzip,
    bzip2 and xz. If the filename is '-', standard output (mode 'w') or
    standard input (mode 'r') is returned.

    The file type is determined based on the filename: .gz is gzip, .bz2 is bzip2, .xz is
    xz/lzma and no compression assumed otherwise.

    mode can be: 'rt', 'rb', 'at', 'ab', 'wt', or 'wb'. Also, the 't' can be omitted,
    so instead of 'rt', 'wt' and 'at', the abbreviations 'r', 'w' and 'a' can be used.

    In Python 2, the 't' and 'b' characters are ignored.

    Append mode ('a', 'at', 'ab') is not available with BZ2 compression and
    will raise an error.

    compresslevel is the compression level for writing to gzip files.
    This parameter is ignored for the other compression formats.

    threads only has a meaning when reading or writing gzip files.

    When threads is None (the default), reading or writing a gzip file is done with a pigz
    (parallel gzip) subprocess if possible. See PipedGzipWriter and PipedGzipReader.

    When threads = 0, no subprocess is used.
    """
    if mode in ('r', 'w', 'a'):
        mode += 't'
    if mode not in ('rt', 'rb', 'wt', 'wb', 'at', 'ab'):
        raise ValueError("Mode '{}' not supported".format(mode))
    filename = fspath(filename)
    if compresslevel not in range(1, 10):
        raise ValueError("compresslevel must be between 1 and 9")

    if filename == '-':
        return _open_stdin_or_out(mode)

    detected_format = _detect_format_from_extension(filename)
    if detected_format is None and "w" not in mode:
        detected_format = _detect_format_from_content(filename)

    if detected_format == "gz":
        return _open_gz(filename, mode, compresslevel, threads)
    elif detected_format == "xz":
        return _open_xz(filename, mode)
    elif detected_format == "bz2":
        return _open_bz2(filename, mode)
    else:
        return open(filename, mode)
