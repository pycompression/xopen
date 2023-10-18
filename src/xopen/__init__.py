"""
Open compressed files transparently.
"""

__all__ = [
    "xopen",
    "PipedGzipReader",
    "PipedGzipWriter",
    "PipedIGzipReader",
    "PipedIGzipWriter",
    "PipedPigzReader",
    "PipedPigzWriter",
    "PipedPBzip2Reader",
    "PipedPBzip2Writer",
    "PipedXzReader",
    "PipedXzWriter",
    "PipedZstdReader",
    "PipedZstdWriter",
    "PipedPythonIsalReader",
    "PipedPythonIsalWriter",
    "__version__",
]

import gzip
import sys
import io
import os
import bz2
import lzma
import stat
import signal
import pathlib
import subprocess
import tempfile
import time
from abc import ABC, abstractmethod
from subprocess import Popen, PIPE, DEVNULL
from typing import (
    Optional,
    Union,
    TextIO,
    AnyStr,
    IO,
    List,
    Set,
    overload,
    BinaryIO,
    Literal,
)
from types import ModuleType

from ._version import version as __version__

# 128K buffer size also used by cat, pigz etc. It is faster than the 8K default.
BUFFER_SIZE = max(io.DEFAULT_BUFFER_SIZE, 128 * 1024)

igzip: Optional[ModuleType]
isal_zlib: Optional[ModuleType]
igzip_threaded: Optional[ModuleType]

try:
    from isal import igzip, igzip_threaded, isal_zlib
except ImportError:
    igzip = None
    isal_zlib = None
    igzip_threaded = None

try:
    import zstandard  # type: ignore
except ImportError:
    zstandard = None

try:
    import fcntl

    # fcntl.F_SETPIPE_SZ will be available in python 3.10.
    # https://github.com/python/cpython/pull/21921
    # If not available: set it to the correct value for known platforms.
    if not hasattr(fcntl, "F_SETPIPE_SZ") and sys.platform == "linux":
        setattr(fcntl, "F_SETPIPE_SZ", 1031)
except ImportError:
    fcntl = None  # type: ignore

_MAX_PIPE_SIZE_PATH = pathlib.Path("/proc/sys/fs/pipe-max-size")
try:
    _MAX_PIPE_SIZE = int(
        _MAX_PIPE_SIZE_PATH.read_text(encoding="ascii")
    )  # type: Optional[int]
except (
    OSError
):  # Catches file not found and permission errors. Possible other errors too.
    _MAX_PIPE_SIZE = None


FilePath = Union[str, bytes, os.PathLike]


def _available_cpu_count() -> int:
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
        with open("/proc/self/status") as f:
            status = f.read()
        m = re.search(r"(?m)^Cpus_allowed:\s*(.*)$", status)
        if m:
            res = bin(int(m.group(1).replace(",", ""), 16)).count("1")
            if res > 0:
                return res
    except OSError:
        pass
    count = os.cpu_count()
    return 1 if count is None else count


def _set_pipe_size_to_max(fd: int) -> None:
    """
    Set pipe size to maximum on platforms that support it.
    :param fd: The file descriptor to increase the pipe size for.
    """
    if not hasattr(fcntl, "F_SETPIPE_SZ") or not _MAX_PIPE_SIZE:
        return
    try:
        fcntl.fcntl(fd, fcntl.F_SETPIPE_SZ, _MAX_PIPE_SIZE)  # type: ignore
    except OSError:
        pass


def _can_read_concatenated_gz(program: str) -> bool:
    """
    Check if a concatenated gzip file can be read properly. Not all deflate
    programs handle this properly.
    """
    fd, temp_path = tempfile.mkstemp(suffix=".gz", prefix="xopen.")
    try:
        # Create a concatenated gzip file. gzip.compress recreates the contents
        # of a gzip file including header and trailer.
        with open(temp_path, "wb") as temp_file:
            temp_file.write(gzip.compress(b"AB") + gzip.compress(b"CD"))
        try:
            result = subprocess.run(
                [program, "-c", "-d", temp_path], check=True, stderr=PIPE, stdout=PIPE
            )
            return result.stdout == b"ABCD"
        except subprocess.CalledProcessError:
            # Program can't read zip
            return False
    finally:
        os.close(fd)
        os.remove(temp_path)


class Closing(ABC):
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

    @abstractmethod
    def close(self):
        """Called when exiting the context manager"""


class PipedCompressionWriter(Closing):
    """
    Write Compressed files by running an external process and piping into it.
    """

    def __init__(
        self,
        path: FilePath,
        program_args: List[str],
        mode="wt",
        compresslevel: Optional[int] = None,
        threads_flag: Optional[str] = None,
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        """
        mode -- one of 'w', 'wt', 'wb', 'a', 'at', 'ab'
        compresslevel -- compression level
        threads_flag -- which flag is used to denote the number of threads in the program.
            If set to none, program will be called without threads flag.
        threads (int) -- number of threads. If this is set to None, a reasonable default is
            used. At the moment, this means that the number of available CPU cores is used, capped
            at four to avoid creating too many threads. Use 0 to use all available cores.
        """
        if mode not in ("w", "wt", "wb", "a", "at", "ab"):
            raise ValueError(
                "Mode is '{}', but it must be 'w', 'wt', 'wb', 'a', 'at' or 'ab'".format(
                    mode
                )
            )

        # TODO use a context manager
        self.outfile = open(path, mode[0] + "b")
        self.closed: bool = False
        self.name: str = str(os.fspath(path))
        self._mode: str = mode
        self._program_args: List[str] = program_args
        self._threads_flag: Optional[str] = threads_flag

        if threads is None:
            threads = min(_available_cpu_count(), 4)
        self._threads = threads
        try:
            self.process = self._open_process(
                mode, compresslevel, threads, self.outfile
            )
        except OSError:
            self.outfile.close()
            raise
        assert self.process.stdin is not None
        _set_pipe_size_to_max(self.process.stdin.fileno())

        if "b" not in mode:
            self._file = io.TextIOWrapper(
                self.process.stdin, encoding=encoding, errors=errors, newline=newline
            )  # type: IO
        else:
            self._file = self.process.stdin

    def __repr__(self):
        return "{}('{}', mode='{}', program='{}', threads={})".format(
            self.__class__.__name__,
            self.name,
            self._mode,
            " ".join(self._program_args),
            self._threads,
        )

    def _open_process(
        self,
        mode: str,
        compresslevel: Optional[int],
        threads: int,
        outfile: TextIO,
    ) -> Popen:
        program_args: List[str] = self._program_args[:]  # prevent list aliasing
        if threads != 0 and self._threads_flag is not None:
            program_args += [f"{self._threads_flag}{threads}"]
        extra_args = []
        if "w" in mode and compresslevel is not None:
            extra_args += ["-" + str(compresslevel)]

        kwargs = dict(stdin=PIPE, stdout=outfile, stderr=DEVNULL)

        # Setting close_fds to True in the Popen arguments is necessary due to
        # <http://bugs.python.org/issue12786>.
        # However, close_fds is not supported on Windows. See
        # <https://github.com/marcelm/cutadapt/issues/315>.
        if sys.platform != "win32":
            kwargs["close_fds"] = True

        process = Popen(program_args + extra_args, **kwargs)  # type: ignore
        return process

    def write(self, arg: AnyStr) -> int:
        return self._file.write(arg)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self._file.close()
        retcode = self.process.wait()
        self.outfile.close()
        if retcode != 0:
            try:
                cause = (
                    f". Possible cause: {os.strerror(retcode)}" if retcode > 1 else ""
                )
            except ValueError:
                cause = ""
            raise OSError(
                "Output process '{}' terminated with exit code {}{}".format(
                    " ".join(self._program_args),
                    retcode,
                    cause,
                )
            )

    def __iter__(self):
        # For compatibility with Pandas, which checks for an __iter__ method
        # to determine whether an object is file-like.
        return self

    def __next__(self):
        raise io.UnsupportedOperation("not readable")


class PipedCompressionReader(Closing):
    """
    Open a pipe to a process for reading a compressed file.
    """

    # This exit code is not interpreted as an error when terminating the process
    _allowed_exit_code: Optional[int] = -signal.SIGTERM
    # If this message is printed on stderr on terminating the process,
    # it is not interpreted as an error
    _allowed_exit_message: Optional[bytes] = None

    def __init__(
        self,
        path: FilePath,
        program_args: List[Union[str, bytes]],
        mode: str = "r",
        threads_flag: Optional[str] = None,
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        """
        Raise an OSError when pigz could not be found.
        """
        if mode not in ("r", "rt", "rb"):
            raise ValueError(
                "Mode is '{}', but it must be 'r', 'rt' or 'rb'".format(mode)
            )
        self._program_args = program_args
        path = os.fspath(path)
        if isinstance(path, bytes) and sys.platform == "win32":
            path = path.decode()
        program_args = program_args + ["-cd", path]

        if threads_flag is not None:
            if threads is None:
                # Single threaded behaviour by default because:
                # - Using a single thread to read a file is the least unexpected
                #   behaviour. (For users of xopen, who do not know which backend is used.)
                # - There is quite a substantial overhead (+25% CPU time) when
                #   using multiple threads while there is only a 10% gain in wall
                #   clock time.
                threads = 1
            program_args += [f"{threads_flag}{threads}"]
        self._threads = threads
        self.process = Popen(program_args, stdout=PIPE, stderr=PIPE)
        self.name = path

        assert self.process.stdout is not None
        _set_pipe_size_to_max(self.process.stdout.fileno())

        self._mode = mode
        if "b" not in mode:
            self._file: IO = io.TextIOWrapper(
                self.process.stdout, encoding=encoding, errors=errors, newline=newline
            )
        else:
            self._file = self.process.stdout
        self.closed = False
        self._wait_for_output_or_process_exit()
        self._raise_if_error()

    def __repr__(self):
        return "{}('{}', mode='{}', program='{}', threads={})".format(
            self.__class__.__name__,
            self.name,
            self._mode,
            " ".join(self._program_args),
            self._threads,
        )

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        retcode = self.process.poll()
        check_allowed_code_and_message = False
        if retcode is None:
            # still running
            self.process.terminate()
            check_allowed_code_and_message = True
        _, stderr_message = self.process.communicate()
        self._file.close()
        self._raise_if_error(check_allowed_code_and_message, stderr_message)

    def __iter__(self):
        return self

    def __next__(self) -> Union[str, bytes]:
        return self._file.__next__()

    def _wait_for_output_or_process_exit(self):
        """
        Wait for the process to produce at least some output, or has exited.
        """
        # The program may crash due to a non-existing file, internal error etc.
        # In that case we need to check. However the 'time-to-crash' differs
        # between programs. Some crash faster than others.
        # Therefore we peek the first character(s) of stdout. Peek will return at
        # least one byte of data, unless the buffer is empty or at EOF. If at EOF,
        # we should wait for the program to exit. This way we ensure the program
        # has at least decompressed some output, or stopped before we continue.

        # stdout is io.BufferedReader if set to PIPE
        while True:
            first_output = self.process.stdout.peek(1)
            if first_output or self.process.poll() is not None:
                break
            time.sleep(0.01)

    def _raise_if_error(
        self, check_allowed_code_and_message: bool = False, stderr_message: bytes = b""
    ) -> None:
        """
        Raise OSError if process is not running anymore and the exit code is
        nonzero. If check_allowed_code_and_message is set, OSError is not raised when
        (1) the exit value of the process is equal to the value of the allowed_exit_code
        attribute or (2) the allowed_exit_message attribute is set and it matches with
        stderr_message.
        """
        retcode = self.process.poll()

        if sys.platform == "win32" and retcode == 1 and stderr_message == b"":
            # Special case for Windows. Winapi terminates processes with exit code 1
            # and an empty error message.
            return

        if retcode is None:
            # process still running
            return
        if retcode == 0:
            # process terminated successfully
            return

        if check_allowed_code_and_message:
            if retcode == self._allowed_exit_code:
                # terminated with allowed exit code
                return
            if self._allowed_exit_message and stderr_message.startswith(
                self._allowed_exit_message
            ):
                # terminated with another exit code, but message is allowed
                return

        assert self.process.stderr is not None
        if not stderr_message:
            stderr_message = self.process.stderr.read()

        self._file.close()
        raise OSError("{!r} (exit code {})".format(stderr_message, retcode))

    def read(self, *args) -> Union[str, bytes]:
        return self._file.read(*args)

    def readinto(self, *args):
        return self._file.readinto(*args)

    def readline(self, *args) -> Union[str, bytes]:
        return self._file.readline(*args)

    def seekable(self) -> bool:
        return self._file.seekable()

    def tell(self) -> int:
        return self._file.tell()

    def seek(self, offset, whence=0) -> int:
        return self._file.seek(offset, whence)

    def peek(self, n: Optional[int] = None):
        if hasattr(self._file, "peek"):
            return self._file.peek(n)  # type: ignore
        else:
            raise AttributeError("Peek is not available when 'b' not in mode")

    def readable(self) -> bool:
        return self._file.readable()

    def writable(self) -> bool:
        return self._file.writable()

    def flush(self) -> None:
        return None


class PipedGzipReader(PipedCompressionReader):
    """
    Open a pipe to gzip for reading a gzipped file.
    """

    def __init__(
        self, path, mode: str = "r", *, encoding="utf-8", errors=None, newline=None
    ):
        super().__init__(
            path, ["gzip"], mode, encoding=encoding, errors=errors, newline=newline
        )


class PipedGzipWriter(PipedCompressionWriter):
    """
    Write gzip-compressed files by running an external gzip process and
    piping into it. On Python 3, gzip.GzipFile is on par with gzip itself,
    but running an external gzip can still reduce wall-clock time because
    the compression happens in a separate process.
    """

    def __init__(
        self,
        path,
        mode: str = "wt",
        compresslevel: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        """
        mode -- one of 'w', 'wt', 'wb', 'a', 'at', 'ab'
        compresslevel -- compression level
        threads (int) -- number of pigz threads. If this is set to None, a reasonable default is
            used. At the moment, this means that the number of available CPU cores is used, capped
            at four to avoid creating too many threads. Use 0 to let pigz use all available cores.
        """
        if compresslevel is not None and compresslevel not in range(1, 10):
            raise ValueError("compresslevel must be between 1 and 9")
        super().__init__(
            path,
            ["gzip", "--no-name"],
            mode,
            compresslevel,
            None,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedPigzReader(PipedCompressionReader):
    """
    Open a pipe to pigz for reading a gzipped file. Even though pigz is mostly
    used to speed up writing by using many compression threads, it is
    also faster when reading, even when forced to use a single thread
    (ca. 2x speedup).
    """

    def __init__(
        self,
        path,
        mode: str = "r",
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        super().__init__(
            path,
            ["pigz"],
            mode,
            "-p",
            threads,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedPigzWriter(PipedCompressionWriter):
    """
    Write gzip-compressed files by running an external pigz process and
    piping into it. pigz can compress using multiple cores. It is also more
    efficient than gzip on only one core. (But then igzip is even faster and
    should be preferred if the compression level allows it.)
    """

    _accepted_compression_levels: Set[int] = set(list(range(10)) + [11])

    def __init__(
        self,
        path,
        mode: str = "wt",
        compresslevel: Optional[int] = None,
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        """
        mode -- one of 'w', 'wt', 'wb', 'a', 'at', 'ab'
        compresslevel -- compression level
        threads (int) -- number of pigz threads. If this is set to None, a reasonable default is
            used. At the moment, this means that the number of available CPU cores is used, capped
            at four to avoid creating too many threads. Use 0 to let pigz use all available cores.
        """
        if (
            compresslevel is not None
            and compresslevel not in self._accepted_compression_levels
        ):
            raise ValueError("compresslevel must be between 0 and 9 or 11")
        super().__init__(
            path,
            ["pigz", "--no-name"],
            mode,
            compresslevel,
            "-p",
            threads,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedPBzip2Reader(PipedCompressionReader):
    """
    Open a pipe to pbzip2 for reading a bzipped file.
    """

    _allowed_exit_code = None
    _allowed_exit_message = b"\n *Control-C or similar caught [sig=15], quitting..."

    def __init__(
        self,
        path,
        mode: str = "r",
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        super().__init__(
            path,
            ["pbzip2"],
            mode,
            "-p",
            threads,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedPBzip2Writer(PipedCompressionWriter):
    """
    Write bzip2-compressed files by running an external pbzip2 process and
    piping into it. pbzip2 can compress using multiple cores.
    """

    def __init__(
        self,
        path,
        mode: str = "wt",
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        # Use default compression level for pbzip2: 9
        super().__init__(
            path,
            ["pbzip2"],
            mode,
            9,
            "-p",
            threads,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedXzReader(PipedCompressionReader):
    """
    Open a pipe to xz for reading an xz-compressed file. A future
    version of xz will be able to decompress using multiple cores.

    (N.B. As of 21 March 2022, this feature is only implemented in xz's
    master branch.)
    """

    def __init__(
        self,
        path,
        mode: str = "r",
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        super().__init__(
            path,
            ["xz"],
            mode,
            "-T",
            threads,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedXzWriter(PipedCompressionWriter):
    """
    Write xz-compressed files by running an external xz process and
    piping into it. xz can compress using multiple cores.
    """

    _accepted_compression_levels: Set[int] = set(range(10))

    def __init__(
        self,
        path,
        mode: str = "wt",
        compresslevel: Optional[int] = None,
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        """
        mode -- one of 'w', 'wt', 'wb', 'a', 'at', 'ab'
        compresslevel -- compression level
        threads (int) -- number of xz threads. If this is set to None, a reasonable default is
            used. At the moment, this means that the number of available CPU cores is used, capped
            at four to avoid creating too many threads. Use 0 to let xz use all available cores.
        """
        if (
            compresslevel is not None
            and compresslevel not in self._accepted_compression_levels
        ):
            raise ValueError("compresslevel must be between 0 and 9")
        super().__init__(
            path,
            ["xz"],
            mode,
            compresslevel,
            "-T",
            threads,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedIGzipReader(PipedCompressionReader):
    """
    Uses igzip for reading of a gzipped file. This is much faster than either
    gzip or pigz which were written to run on a wide array of systems. igzip
    can only run on x86 and ARM architectures, but is able to use more
    architecture-specific optimizations as a result.
    """

    def __init__(
        self, path, mode: str = "r", *, encoding="utf-8", errors=None, newline=None
    ):
        if not _can_read_concatenated_gz("igzip"):
            # Instead of elaborate version string checking once the problem is
            # fixed, it is much easier to use this, "proof in the pudding" type
            # of evaluation.
            raise ValueError(
                "This version of igzip does not support reading "
                "concatenated gzip files and is therefore not "
                "safe to use. See: https://github.com/intel/isa-l/issues/143"
            )
        super().__init__(
            path, ["igzip"], mode, encoding=encoding, errors=errors, newline=newline
        )


class PipedZstdReader(PipedCompressionReader):
    """
    Open a pipe to zstd for reading a zstandard-compressed file (.zst).
    """

    def __init__(
        self,
        path,
        mode: str = "r",
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        super().__init__(
            path,
            ["zstd"],
            mode,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedZstdWriter(PipedCompressionWriter):
    """
    Write Zstandard-compressed files by running an external xz process and
    piping into it. xz can compress using multiple cores.
    """

    _accepted_compression_levels: Set[int] = set(range(1, 20))

    def __init__(
        self,
        path,
        mode: str = "wt",
        compresslevel: Optional[int] = None,
        threads: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        """
        mode -- one of 'w', 'wt', 'wb', 'a', 'at', 'ab'
        compresslevel -- compression level
        threads (int) -- number of zstd threads. If this is set to None, a reasonable default is
            used. At the moment, this means that the number of available CPU cores is used, capped
            at four to avoid creating too many threads. Use 0 to let zstd use all available cores.
        """
        if (
            compresslevel is not None
            and compresslevel not in self._accepted_compression_levels
        ):
            raise ValueError("compresslevel must be between 1 and 19")
        super().__init__(
            path,
            ["zstd"],
            mode,
            compresslevel,
            "-T",
            threads,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedIGzipWriter(PipedCompressionWriter):
    """
    Uses igzip for writing a gzipped file. This is much faster than either
    gzip or pigz which were written to run on a wide array of systems. igzip
    can only run on x86 and ARM architectures, but is able to use more
    architecture-specific optimizations as a result.

    Threads are supported by a flag, but do not add any speed. Also on some
    distro version (isal package in debian buster) the thread flag is not
    present. For these reason threads are omitted from the interface.
    Only compresslevel 0-3 are supported and these output slightly different
    filesizes from their pigz/gzip counterparts.
    See: https://gist.github.com/rhpvorderman/4f1201c3f39518ff28dde45409eb696b
    """

    def __init__(
        self,
        path,
        mode: str = "wt",
        compresslevel: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        if compresslevel is not None and compresslevel not in range(0, 4):
            raise ValueError("compresslevel must be between 0 and 3")
        super().__init__(
            path,
            ["igzip", "--no-name"],
            mode,
            compresslevel,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedPythonIsalReader(PipedCompressionReader):
    def __init__(
        self, path, mode: str = "r", *, encoding="utf-8", errors=None, newline=None
    ):
        super().__init__(
            path,
            [sys.executable, "-m", "isal.igzip"],
            mode,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


class PipedPythonIsalWriter(PipedCompressionWriter):
    def __init__(
        self,
        path,
        mode: str = "wt",
        compresslevel: Optional[int] = None,
        *,
        encoding="utf-8",
        errors=None,
        newline=None,
    ):
        if compresslevel is not None and compresslevel not in range(0, 4):
            raise ValueError("compresslevel must be between 0 and 3")
        super().__init__(
            path,
            [sys.executable, "-m", "isal.igzip", "--no-name"],
            mode,
            compresslevel,
            encoding=encoding,
            errors=errors,
            newline=newline,
        )


def _open_stdin_or_out(mode: str, **text_mode_kwargs) -> IO:
    # Do not return sys.stdin or sys.stdout directly as we want the returned object
    # to be closable without closing sys.stdout.
    std = dict(r=sys.stdin, w=sys.stdout)[mode[0]]
    return open(std.fileno(), mode=mode, closefd=False, **text_mode_kwargs)


def _open_bz2(filename, mode: str, threads: Optional[int], **text_mode_kwargs):
    if threads != 0:
        try:
            if "r" in mode:
                return PipedPBzip2Reader(filename, mode, threads, **text_mode_kwargs)
            else:
                return PipedPBzip2Writer(filename, mode, threads, **text_mode_kwargs)
        except OSError:
            pass  # We try without threads.

    return bz2.open(filename, mode, **text_mode_kwargs)


def _open_xz(
    filename,
    mode: str,
    compresslevel: Optional[int],
    threads: Optional[int],
    **text_mode_kwargs,
):
    if compresslevel is None:
        compresslevel = 6

    if threads != 0:
        try:
            if "r" in mode:
                return PipedXzReader(filename, mode, threads, **text_mode_kwargs)
            else:
                return PipedXzWriter(
                    filename, mode, compresslevel, threads, **text_mode_kwargs
                )
        except OSError:
            pass  # We try without threads.

    return lzma.open(
        filename,
        mode,
        preset=compresslevel if "w" in mode else None,
        **text_mode_kwargs,
    )


def _open_zst(  # noqa: C901
    filename,
    mode: str,
    compresslevel: Optional[int],
    threads: Optional[int],
    **text_mode_kwargs,
):
    assert compresslevel != 0
    if compresslevel is None:
        compresslevel = 3
    if threads != 0:
        try:
            if "r" in mode:
                return PipedZstdReader(filename, mode, **text_mode_kwargs)
            else:
                return PipedZstdWriter(
                    filename, mode, compresslevel, threads, **text_mode_kwargs
                )
        except OSError:
            if zstandard is None:
                # No fallback available
                raise

    if zstandard is None:
        raise ImportError("zstandard module (python-zstandard) not available")
    if compresslevel is not None and "w" in mode:
        cctx = zstandard.ZstdCompressor(level=compresslevel)
    else:
        cctx = None
    f = zstandard.open(
        filename,
        mode,
        cctx=cctx,
        **text_mode_kwargs,
    )
    if mode == "rb":
        return io.BufferedReader(f)
    elif mode == "wb":
        return io.BufferedWriter(f)
    return f


def _open_external_gzip_reader(
    filename, mode, compresslevel, threads, **text_mode_kwargs
):
    assert mode in ("rt", "rb")
    try:
        return PipedIGzipReader(filename, mode, **text_mode_kwargs)
    except (OSError, ValueError):
        # No igzip installed or version does not support reading
        # concatenated files.
        pass
    try:
        return PipedPigzReader(filename, mode, threads=threads, **text_mode_kwargs)
    except OSError:
        return PipedGzipReader(filename, mode, **text_mode_kwargs)


def _open_external_gzip_writer(
    filename, mode, compresslevel, threads, **text_mode_kwargs
):
    assert mode in ("wt", "wb", "at", "ab")
    try:
        return PipedIGzipWriter(filename, mode, compresslevel, **text_mode_kwargs)
    except (OSError, ValueError):
        # No igzip installed or compression level higher than 3
        pass
    try:
        return PipedPigzWriter(
            filename, mode, compresslevel, threads=threads, **text_mode_kwargs
        )
    except OSError:
        return PipedGzipWriter(filename, mode, compresslevel, **text_mode_kwargs)


def _open_gz(  # noqa: C901
    filename, mode: str, compresslevel, threads, **text_mode_kwargs
):
    assert mode in ("rt", "rb", "wt", "wb", "at", "ab")
    # With threads == 0 igzip_threaded defers to igzip.open, but that is not
    # desirable as a reproducible header is required.
    if igzip_threaded and threads != 0:
        try:
            return igzip_threaded.open(  # type: ignore
                filename,
                mode,
                isal_zlib.ISAL_DEFAULT_COMPRESSION  # type: ignore
                if compresslevel is None
                else compresslevel,
                **text_mode_kwargs,
                threads=1 if threads is None else threads,
            )
        except ValueError:  # Wrong compression level
            pass
    if threads != 0:
        try:
            if "r" in mode:
                return _open_external_gzip_reader(
                    filename, mode, compresslevel, threads, **text_mode_kwargs
                )
            else:
                return _open_external_gzip_writer(
                    filename, mode, compresslevel, threads, **text_mode_kwargs
                )
        except OSError:
            pass  # We try without threads.

    if "r" in mode:
        if igzip is not None:
            return igzip.open(filename, mode, **text_mode_kwargs)
        return gzip.open(filename, mode, **text_mode_kwargs)

    g = _open_reproducible_gzip(
        filename,
        mode=mode[0] + "b",
        compresslevel=compresslevel,
    )
    if "t" in mode:
        return io.TextIOWrapper(g, **text_mode_kwargs)
    return g


def _open_reproducible_gzip(filename, mode, compresslevel):
    """
    Open a gzip file for writing (without external processes)
    that has neither mtime nor the file name in the header
    (equivalent to gzip --no-name)
    """
    assert mode in ("rb", "wb", "ab")
    # Neither gzip.open nor igzip.open have an mtime option, and they will
    # always write the file name, so we need to open the file separately
    # and pass it to gzip.GzipFile/igzip.IGzipFile.
    binary_file = open(filename, mode=mode)
    kwargs = dict(
        fileobj=binary_file,
        filename="",
        mode=mode,
        mtime=0,
    )
    gzip_file = None
    if igzip is not None:
        try:
            gzip_file = igzip.IGzipFile(
                **kwargs,
                compresslevel=isal_zlib.ISAL_DEFAULT_COMPRESSION
                if compresslevel is None
                else compresslevel,
            )
        except ValueError:
            # Compression level not supported, move to built-in gzip.
            pass
    if gzip_file is None:
        gzip_file = gzip.GzipFile(
            **kwargs,
            # Override gzip.open's default of 9 for consistency
            # with command-line gzip.
            compresslevel=6 if compresslevel is None else compresslevel,
        )
    # When (I)GzipFile is created with a fileobj instead of a filename,
    # the passed file object is not closed when (I)GzipFile.close()
    # is called. This forces it to be closed.
    gzip_file.myfileobj = binary_file
    return gzip_file


def _detect_format_from_content(filename: FilePath) -> Optional[str]:
    """
    Attempts to detect file format from the content by reading the first
    6 bytes. Returns None if no format could be detected.
    """
    try:
        if stat.S_ISREG(os.stat(filename).st_mode):
            with open(filename, "rb") as fh:
                bs = fh.read(6)
            if bs[:2] == b"\x1f\x8b":
                # https://tools.ietf.org/html/rfc1952#page-6
                return "gz"
            elif bs[:3] == b"\x42\x5a\x68":
                # https://en.wikipedia.org/wiki/List_of_file_signatures
                return "bz2"
            elif bs[:6] == b"\xfd\x37\x7a\x58\x5a\x00":
                # https://tukaani.org/xz/xz-file-format.txt
                return "xz"
            elif bs[:4] == b"\x28\xb5\x2f\xfd":
                # https://datatracker.ietf.org/doc/html/rfc8478#section-3.1.1
                return "zst"
    except OSError:
        pass

    return None


def _detect_format_from_extension(filename: Union[str, bytes]) -> Optional[str]:
    """
    Attempt to detect file format from the filename extension.
    Return None if no format could be detected.
    """
    for ext in ("bz2", "xz", "gz", "zst"):
        if isinstance(filename, bytes):
            if filename.endswith(b"." + ext.encode()):
                return ext
        else:
            if filename.endswith("." + ext):
                return ext
    return None


@overload
def xopen(
    filename: FilePath,
    mode: Literal["r", "w", "a", "rt", "wt", "at"] = ...,
    compresslevel: Optional[int] = ...,
    threads: Optional[int] = ...,
    *,
    encoding: str = ...,
    errors: Optional[str] = ...,
    newline: Optional[str] = ...,
    format: Optional[str] = ...,
) -> TextIO:
    ...


@overload
def xopen(
    filename: FilePath,
    mode: Literal["rb", "wb", "ab"],
    compresslevel: Optional[int] = ...,
    threads: Optional[int] = ...,
    *,
    encoding: str = ...,
    errors: None = ...,
    newline: None = ...,
    format: Optional[str] = ...,
) -> BinaryIO:
    ...


def xopen(  # noqa: C901  # The function is complex, but readable.
    filename: FilePath,
    mode: Literal["r", "w", "a", "rt", "rb", "wt", "wb", "at", "ab"] = "r",
    compresslevel: Optional[int] = None,
    threads: Optional[int] = None,
    *,
    encoding: str = "utf-8",
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    format: Optional[str] = None,
) -> IO:
    """
    A replacement for the "open" function that can also read and write
    compressed files transparently. The supported compression formats are gzip,
    bzip2, xz and zstandard. If the filename is '-', standard output (mode 'w') or
    standard input (mode 'r') is returned.

    When writing, the file format is chosen based on the file name extension:
    - .gz uses gzip compression
    - .bz2 uses bzip2 compression
    - .xz uses xz/lzma compression
    - .zst uses zstandard compression
    - otherwise, no compression is used

    When reading, if a file name extension is available, the format is detected
    using it, but if not, the format is detected from the contents.

    mode can be: 'rt', 'rb', 'at', 'ab', 'wt', or 'wb'. Also, the 't' can be omitted,
    so instead of 'rt', 'wt' and 'at', the abbreviations 'r', 'w' and 'a' can be used.

    compresslevel is the compression level for writing to gzip, xz and zst files.
    This parameter is ignored for the other compression formats.
    If set to None, a default depending on the format is used:
    gzip: 6, xz: 6, zstd: 3.

    When threads is None (the default), compressed file formats are read or written
    using a pipe to a subprocess running an external tool such as ``igzip``,
    ``pbzip2``, ``pigz`` etc., see PipedIGzipWriter, PipedIGzipReader etc.
    If the external tool supports multiple threads, *threads* can be set to an int
    specifying the number of threads to use.
    If no external tool supporting the compression format is available, the file is
    opened calling the appropriate Python function
    (that is, no subprocess is spawned).

    Set threads to 0 to force opening the file without using a subprocess.

    encoding, errors and newline are used when opening a file in text mode.
    The parameters have the same meaning as in the built-in open function,
    except that the default encoding is always UTF-8 instead of the
    preferred locale encoding.

    format overrides the autodetection of input and output formats. This can be
    useful when compressed output needs to be written to a file without an
    extension. Possible values are "gz", "xz", "bz2", "zst".
    """
    if mode in ("r", "w", "a"):
        mode += "t"  # type: ignore
    if mode not in ("rt", "rb", "wt", "wb", "at", "ab"):
        raise ValueError("Mode '{}' not supported".format(mode))
    filename = os.fspath(filename)

    if "b" in mode:
        # Do not pass encoding etc. in binary mode as this raises errors.
        text_mode_kwargs = dict()
    else:
        text_mode_kwargs = dict(encoding=encoding, errors=errors, newline=newline)
    if filename == "-":
        return _open_stdin_or_out(mode, **text_mode_kwargs)

    if format not in (None, "gz", "xz", "bz2", "zst"):
        raise ValueError(
            f"Format not supported: {format}. "
            f"Choose one of: 'gz', 'xz', 'bz2', 'zst'"
        )
    detected_format = format or _detect_format_from_extension(filename)
    if detected_format is None and "w" not in mode:
        detected_format = _detect_format_from_content(filename)

    if detected_format == "gz":
        opened_file = _open_gz(
            filename, mode, compresslevel, threads, **text_mode_kwargs
        )
    elif detected_format == "xz":
        opened_file = _open_xz(
            filename, mode, compresslevel, threads, **text_mode_kwargs
        )
    elif detected_format == "bz2":
        opened_file = _open_bz2(filename, mode, threads, **text_mode_kwargs)
    elif detected_format == "zst":
        opened_file = _open_zst(
            filename, mode, compresslevel, threads, **text_mode_kwargs
        )
    else:
        opened_file = open(filename, mode, **text_mode_kwargs)  # type: ignore

    # The "write" method for GzipFile is very costly. Lots of python calls are
    # made. To a lesser extent this is true for LzmaFile and BZ2File. By
    # putting a buffer in between, the expensive write method is called much
    # less. The effect is very noticeable when writing small units such as
    # lines or FASTQ records.
    if (
        isinstance(opened_file, (gzip.GzipFile, bz2.BZ2File, lzma.LZMAFile))  # FIXME
        and "w" in mode
    ):
        opened_file = io.BufferedWriter(
            opened_file, buffer_size=BUFFER_SIZE  # type: ignore
        )
    return opened_file
