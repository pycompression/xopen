"""
Open compressed files transparently.
"""

__all__ = [
    "xopen",
    "_PipedCompressionProgram",
    "__version__",
]

import dataclasses
import gzip
import stat
import sys
import io
import os
import bz2
import lzma
import signal
import pathlib
import subprocess
import tempfile
import threading
import time
from typing import (
    Dict,
    Optional,
    Union,
    IO,
    overload,
    BinaryIO,
    Literal,
    Tuple,
)
from types import ModuleType

from ._version import version as __version__

# 128K buffer size also used by cat, pigz etc. It is faster than the 8K default.
BUFFER_SIZE = max(io.DEFAULT_BUFFER_SIZE, 128 * 1024)

XOPEN_DEFAULT_GZIP_COMPRESSION = 1
XOPEN_DEFAULT_BZ2_COMPRESSION = 9
XOPEN_DEFAULT_XZ_COMPRESSION = 6
XOPEN_DEFAULT_ZST_COMPRESSION = 3

igzip: Optional[ModuleType]
isal_zlib: Optional[ModuleType]
igzip_threaded: Optional[ModuleType]
zlib_ng: Optional[ModuleType]
gzip_ng: Optional[ModuleType]
gzip_ng_threaded: Optional[ModuleType]

try:
    from isal import igzip, igzip_threaded, isal_zlib
except ImportError:
    igzip = None
    isal_zlib = None
    igzip_threaded = None

try:
    from zlib_ng import gzip_ng, gzip_ng_threaded, zlib_ng
except ImportError:
    gzip_ng = None
    gzip_ng_threaded = None
    zlib_ng = None

try:
    import zstandard  # type: ignore
except ImportError:
    zstandard = None  # type: ignore

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
FileOrPath = Union[FilePath, IO]


@dataclasses.dataclass
class _ProgramSettings:
    program_args: Tuple[str, ...]
    acceptable_compression_levels: Tuple[int, ...] = tuple(range(1, 10))
    threads_flag: Optional[str] = None
    # This exit code is not interpreted as an error when terminating the process
    allowed_exit_code: Optional[int] = -signal.SIGTERM
    # If this message is printed on stderr on terminating the process,
    # it is not interpreted as an error
    allowed_exit_message: Optional[bytes] = None


_PROGRAM_SETTINGS: Dict[str, _ProgramSettings] = {
    "pbzip2": _ProgramSettings(
        ("pbzip2",),
        tuple(range(1, 10)),
        "-p",
        allowed_exit_code=None,
        allowed_exit_message=b"\n *Control-C or similar caught [sig=15], quitting...",
    ),
    "xz": _ProgramSettings(("xz",), tuple(range(0, 10)), "-T"),
    "zstd": _ProgramSettings(("zstd",), tuple(range(1, 20)), "-T"),
    "pigz": _ProgramSettings(("pigz", "--no-name"), tuple(range(0, 10)) + (11,), "-p"),
    "gzip": _ProgramSettings(("gzip", "--no-name"), tuple(range(1, 10))),
}


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


class _PipedCompressionProgram(io.IOBase):
    """
    Read and write compressed files by running an external process and piping into it.
    """

    def __init__(
        self,
        filename: FileOrPath,
        mode="rb",
        compresslevel: Optional[int] = None,
        threads: Optional[int] = None,
        program_settings: _ProgramSettings = _ProgramSettings(("gzip", "--no-name")),
    ):
        """
        mode -- one of 'w', 'wb', 'a', 'ab'
        compresslevel -- compression level
        threads_flag -- which flag is used to denote the number of threads in the program.
            If set to none, program will be called without threads flag.
        threads (int) -- number of threads. If this is set to None, a reasonable default is
            used. At the moment, this means that the number of available CPU cores is used, capped
            at four to avoid creating too many threads. Use 0 to use all available cores.
        """
        self._error_raised = False
        self._program_args = list(program_settings.program_args)
        self._allowed_exit_code = program_settings.allowed_exit_code
        self._allowed_exit_message = program_settings.allowed_exit_message
        if mode not in ("r", "rb", "w", "wb", "a", "ab"):
            raise ValueError(
                f"Mode is '{mode}', but it must be 'r', 'rb', 'w', 'wb', 'a', or 'ab'"
            )
        if "b" not in mode:
            mode += "b"
        if (
            compresslevel is not None
            and compresslevel not in program_settings.acceptable_compression_levels
        ):
            raise ValueError(
                f"compresslevel must be in {program_settings.acceptable_compression_levels}."
            )
        self._compresslevel = compresslevel
        self.fileobj, self.closefd = _file_or_path_to_binary_stream(filename, mode)
        self._path = _filepath_from_path_or_filelike(filename)
        self.name: str = str(self._path)
        self._mode: str = mode
        self._stderr = tempfile.TemporaryFile("w+b")
        self._threads_flag: Optional[str] = program_settings.threads_flag

        if threads is None:
            if "r" in mode:
                # Reading occurs single threaded by default. This has the least
                # amount of overhead and is fast enough for most use cases.
                threads = 1
            else:
                threads = min(_available_cpu_count(), 4)
        self._threads = threads

        self._open_process()

    def _open_process(self):
        if self._threads != 0 and self._threads_flag is not None:
            self._program_args += [f"{self._threads_flag}{self._threads}"]

        # Setting close_fds to True in the Popen arguments is necessary due to
        # <http://bugs.python.org/issue12786>.
        # However, close_fds is not supported on Windows. See
        # <https://github.com/marcelm/cutadapt/issues/315>.
        close_fds = False
        if sys.platform != "win32":
            close_fds = True

        self.in_pipe = None
        self.in_thread = None
        self._feeding = True
        if "r" in self._mode:
            self._program_args += ["-c", "-d"]  # type: ignore
            stdout = subprocess.PIPE
        else:
            if self._compresslevel is not None:
                self._program_args += ["-" + str(self._compresslevel)]
            stdout = self.fileobj  # type: ignore
        try:
            self.process = subprocess.Popen(
                self._program_args,
                stderr=self._stderr,
                stdout=stdout,
                stdin=subprocess.PIPE,
                close_fds=close_fds,
            )  # type: ignore
        except OSError:
            if self.closefd:
                self.fileobj.close()
            raise
        assert self.process.stdin is not None
        if "r" in self._mode:
            self.in_pipe = self.process.stdin
            # A python subprocess can read and write from pipes, but not from
            # Python in-memory objects. In order for a program to read from an
            # in-memory object, a pipe must be created. This pipe must be fed
            # data from the in-memory object. This must be done in a separate
            # thread, because IO operations will block when the pipe is full
            # when writing, or empty when reading. Since the quantity of output
            # data generated by a certain amount of input data is unknown, the
            # only way to prevent a blocking application is to write
            # data continuously to the process stdin on another thread.
            self.in_thread = threading.Thread(target=self._feed_pipe)
            self.in_thread.start()
            self._process_explicitly_terminated = False
            self._file: BinaryIO = self.process.stdout  # type: ignore
            self._wait_for_output_or_process_exit()
            self._raise_if_error()
        else:
            self._file = self.process.stdin  # type: ignore

        _set_pipe_size_to_max(self._file.fileno())

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"('{self.name}', mode='{self._mode}', "
            f"program='{' '.join(self._program_args)}', "
            f"threads={self._threads})"
        )

    def _feed_pipe(self):
        try:
            while self._feeding:
                chunk = self.fileobj.read(BUFFER_SIZE)
                if chunk == b"":
                    self.in_pipe.close()
                    return
                try:
                    self.in_pipe.write(chunk)
                except BrokenPipeError:
                    if not self._process_explicitly_terminated:
                        raise
        finally:
            self.in_pipe.close()

    def write(self, arg: bytes) -> int:
        return self._file.write(arg)

    def read(self, *args) -> bytes:
        return self._file.read(*args)

    def readinto(self, *args):
        return self._file.readinto(*args)

    def readline(self, *args) -> bytes:
        return self._file.readline(*args)

    def seekable(self) -> bool:
        return self._file.seekable()

    def tell(self) -> int:
        return self._file.tell()

    def peek(self, n: Optional[int] = None):
        return self._file.peek(n)  # type: ignore

    def seek(self, offset, whence=0) -> int:
        return self._file.seek(offset, whence)

    def close(self) -> None:
        if self.closed:
            return
        super().close()
        if not hasattr(self, "process"):
            # Exception was raised during __init__
            if hasattr(self, "_stderr"):
                self._stderr.close()
            return
        check_allowed_code_and_message = False
        if "r" in self._mode:
            retcode = self.process.poll()
            if retcode is None:
                # still running
                self._process_explicitly_terminated = True
                self.process.terminate()
                check_allowed_code_and_message = True
                self.process.wait()
            self._feeding = False
            self._file.read()
            if self.in_thread:
                self.in_thread.join()
            self._file.close()
        else:
            self._file.close()
            self.process.wait()
        if self.closefd:
            self.fileobj.close()
        stderr_message = self._read_error_message()
        self._stderr.close()
        if not self._error_raised:
            # Only check for errors if none have been found earlier.
            self._raise_if_error(check_allowed_code_and_message, stderr_message)

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
            exit_code = self.process.poll()
            if first_output or exit_code is not None:
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

        if not stderr_message:
            stderr_message = self._read_error_message()

        self._file.close()
        self._error_raised = True
        raise OSError(f"{stderr_message!r} (exit code {retcode})")

    def _read_error_message(self):
        if self._stderr.closed:
            return b""
        self._stderr.flush()
        self._stderr.seek(0)
        return self._stderr.read()

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        return self._file.__next__()

    def readable(self):
        return self._file.readable()

    def writable(self):
        return self._file.writable()

    def flush(self) -> None:
        return None


def _open_stdin_or_out(mode: str) -> BinaryIO:
    assert mode in ("rb", "ab", "wb")
    std = sys.stdin if mode == "rb" else sys.stdout
    return open(std.fileno(), mode=mode, closefd=False)  # type: ignore


def _open_bz2(
    filename: FileOrPath,
    mode: str,
    compresslevel: Optional[int],
    threads: Optional[int],
):
    assert mode in ("rb", "ab", "wb")
    if compresslevel is None:
        compresslevel = XOPEN_DEFAULT_BZ2_COMPRESSION
    if threads != 0:
        try:
            # pbzip2 can compress using multiple cores.
            return _PipedCompressionProgram(
                filename,
                mode,
                compresslevel,
                threads=threads,
                program_settings=_PROGRAM_SETTINGS["pbzip2"],
            )
        except OSError:
            pass  # We try without threads.

    bz2_file = bz2.open(filename, mode, compresslevel)
    if "r" in mode:
        return bz2_file
    # Buffer writes on bz2.open to mitigate overhead of small writes
    return io.BufferedWriter(bz2_file)  # type: ignore


def _open_xz(
    filename: FileOrPath,
    mode: str,
    compresslevel: Optional[int],
    threads: Optional[int],
):
    assert mode in ("rb", "ab", "wb")
    if compresslevel is None:
        compresslevel = XOPEN_DEFAULT_XZ_COMPRESSION

    if threads != 0:
        try:
            # xz can compress using multiple cores.
            return _PipedCompressionProgram(
                filename,
                mode,
                compresslevel,
                threads,
                _PROGRAM_SETTINGS["xz"],
            )
        except OSError:
            pass  # We try without threads.

    if "r" in mode:
        return lzma.open(filename, mode)
    # Buffer writes on lzma.open to mitigate overhead of small writes
    return io.BufferedWriter(lzma.open(filename, mode, preset=compresslevel))  # type: ignore


def _open_zst(
    filename: FileOrPath,
    mode: str,
    compresslevel: Optional[int],
    threads: Optional[int],
):
    assert mode in ("rb", "ab", "wb")
    assert compresslevel != 0
    if compresslevel is None:
        compresslevel = XOPEN_DEFAULT_ZST_COMPRESSION
    if zstandard:
        max_window_bits = zstandard.WINDOWLOG_MAX
    else:
        max_window_bits = 31
    if threads != 0:
        try:
            # zstd can compress using multiple cores
            program_args: Tuple[str, ...] = ("zstd",)
            if "r" in mode:
                # Only use --long=31 for decompression. Using it for
                # compression overrides level settings for window size and
                # forces other zstd users to use `--long=31` to decompress any
                # archive that has been compressed by xopen.
                program_args += (f"--long={max_window_bits}",)
            return _PipedCompressionProgram(
                filename,
                mode,
                compresslevel,
                threads,
                _ProgramSettings(program_args, tuple(range(1, 20)), "-T"),
            )
        except OSError:
            if zstandard is None:
                # No fallback available
                raise

    if zstandard is None:
        raise ImportError("zstandard module (python-zstandard) not available")
    dctx = zstandard.ZstdDecompressor(max_window_size=2**max_window_bits)
    cctx = zstandard.ZstdCompressor(level=compresslevel)
    f = zstandard.open(filename, mode, cctx=cctx, dctx=dctx)  # type: ignore
    if mode == "rb":
        return io.BufferedReader(f)
    return io.BufferedWriter(f)  # mode "ab" and "wb"


def _open_gz(
    filename: FileOrPath,
    mode: str,
    compresslevel: Optional[int],
    threads: Optional[int],
):
    """
    Open a gzip file. The ISA-L library is preferred when applicable because
    it is the fastest. Then zlib-ng which is not as fast, but supports all
    compression levels. After that comes pigz, which can utilize multiple
    threads and is more efficient than gzip, even on one core. gzip is chosen
    when none of the alternatives are available. Despite it being able to use
    only one core, it still finishes faster than using the builtin gzip library
    as the (de)compression is moved to another thread.
    """
    assert mode in ("rb", "ab", "wb")
    if compresslevel is None:
        # Force the same compression level on every tool regardless of
        # library defaults
        compresslevel = XOPEN_DEFAULT_GZIP_COMPRESSION
    if compresslevel not in range(10):
        # Level 0-9 are supported regardless of backend support
        # (zlib_ng supports -1, pigz supports 11 etc.)
        raise ValueError(
            f"gzip compresslevel must be in range 0-9, got {compresslevel}."
        )

    if threads != 0:
        # Igzip level 0 does not output uncompressed deflate blocks as zlib does
        # and level 3 is slower but does not compress better than level 1 and 2.
        if igzip_threaded and (compresslevel in (1, 2) or "r" in mode):
            return igzip_threaded.open(  # type: ignore
                filename,
                mode,
                compresslevel,
                threads=1,
            )
        if gzip_ng_threaded and zlib_ng:
            return gzip_ng_threaded.open(
                filename,
                mode,
                # zlib-ng level 1 is 50% bigger than zlib level 1. Level
                # 2 gives a size close to expectations.
                compresslevel=2 if compresslevel == 1 else compresslevel,
                threads=threads or max(_available_cpu_count(), 4),
            )

        for program in ("pigz", "gzip"):
            try:
                return _PipedCompressionProgram(
                    filename,
                    mode,
                    compresslevel,
                    threads,
                    _PROGRAM_SETTINGS[program],
                )
            # ValueError when compresslevel is not supported. i.e. gzip and level 0
            except (OSError, ValueError):
                pass  # We try without threads.
    return _open_reproducible_gzip(filename, mode=mode, compresslevel=compresslevel)


def _open_reproducible_gzip(filename, mode: str, compresslevel: int):
    """
    Open a gzip file for writing (without external processes)
    that has neither mtime nor the file name in the header
    (equivalent to gzip --no-name)
    """
    assert mode in ("rb", "wb", "ab")
    assert compresslevel is not None
    fileobj, closefd = _file_or_path_to_binary_stream(filename, mode)
    # Neither gzip.open nor igzip.open have an mtime option, and they will
    # always write the file name, so we need to open the file separately
    # and pass it to gzip.GzipFile/igzip.IGzipFile.
    kwargs = dict(
        fileobj=fileobj,
        filename="",
        mode=mode,
        mtime=0,
    )
    # Igzip level 0 does not output uncompressed deflate blocks as zlib does
    # and level 3 is slower but does not compress better than level 1 and 2.
    if igzip is not None and (compresslevel in (1, 2) or "r" in mode):
        gzip_file = igzip.IGzipFile(**kwargs, compresslevel=compresslevel)
    elif gzip_ng is not None:
        # Zlib-ng level 1 creates much bigger files than zlib level 1.
        gzip_file = gzip_ng.GzipNGFile(
            **kwargs, compresslevel=2 if compresslevel == 1 else compresslevel
        )
    else:
        gzip_file = gzip.GzipFile(**kwargs, compresslevel=compresslevel)  # type: ignore
    # When (I)GzipFile is created with a fileobj instead of a filename,
    # the passed file object is not closed when (I)GzipFile.close()
    # is called. This forces it to be closed.
    if closefd:
        gzip_file.myfileobj = fileobj
    if sys.version_info < (3, 12) and "r" not in mode:
        # From version 3.12 onwards, gzip is properly internally buffered for writing.
        return io.BufferedWriter(gzip_file)  # type: ignore
    return gzip_file


def _detect_format_from_content(filename: FileOrPath) -> Optional[str]:
    """
    Attempts to detect file format from the content by reading the first
    6 bytes. Returns None if no format could be detected.
    """
    fileobj, closefd = _file_or_path_to_binary_stream(filename, "rb")
    try:
        if not fileobj.readable():
            return None
        if hasattr(fileobj, "peek"):
            bs = fileobj.peek(6)
        elif hasattr(fileobj, "seekable") and fileobj.seekable():
            current_pos = fileobj.tell()
            bs = fileobj.read(6)
            fileobj.seek(current_pos)
        else:
            return None

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
        return None
    finally:
        if closefd:
            fileobj.close()


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


def _file_or_path_to_binary_stream(
    file_or_path: FileOrPath, binary_mode: str
) -> Tuple[BinaryIO, bool]:
    assert binary_mode in ("rb", "wb", "ab")
    if isinstance(file_or_path, (str, bytes)) or hasattr(file_or_path, "__fspath__"):
        return open(os.fspath(file_or_path), binary_mode), True  # type: ignore
    if isinstance(file_or_path, io.TextIOWrapper):
        return file_or_path.buffer, False
    if hasattr(file_or_path, "readinto") or hasattr(file_or_path, "write"):
        # Very lenient fallback for all filelike objects. If the filelike
        # object is not binary, this will crash at a later point.
        return file_or_path, False  # type: ignore
    raise TypeError(
        f"Unsupported type for {file_or_path}, " f"{file_or_path.__class__.__name__}."
    )


def _filepath_from_path_or_filelike(fileorpath: FileOrPath) -> str:
    try:
        return os.fspath(fileorpath)  # type: ignore
    except TypeError:
        pass
    if hasattr(fileorpath, "name"):
        name = fileorpath.name
        if isinstance(name, str):
            return name
        elif isinstance(name, bytes):
            return name.decode()
    return ""


def _file_is_a_socket_or_pipe(filepath):
    try:
        mode = os.stat(filepath).st_mode
        # Treat anything that is not a regular file as special
        return not stat.S_ISREG(mode)
    except (OSError, TypeError):  # Type error for unexpected types in stat.
        return False


@overload
def xopen(
    filename: FileOrPath,
    mode: Literal["r", "w", "a", "rt", "wt", "at"] = ...,
    compresslevel: Optional[int] = ...,
    threads: Optional[int] = ...,
    *,
    encoding: str = ...,
    errors: Optional[str] = ...,
    newline: Optional[str] = ...,
    format: Optional[str] = ...,
) -> io.TextIOWrapper:
    ...


@overload
def xopen(
    filename: FileOrPath,
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


def xopen(  # noqa: C901
    filename: FileOrPath,
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
    standard input (mode 'r') is returned. Filename can be a string or a
    file object. (See https://docs.python.org/3/glossary.html#term-file-object.)

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
    using a pipe to a subprocess running an external tool such as,
    ``pbzip2``, ``gzip`` etc., see PipedGzipWriter, PipedGzipReader etc.
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
    binary_mode = mode[0] + "b"
    filepath = _filepath_from_path_or_filelike(filename)

    # Open non-regular files such as pipes and sockets here to force opening
    # them once.
    if filename == "-":
        filename = _open_stdin_or_out(binary_mode)
    elif _file_is_a_socket_or_pipe(filename):
        filename = open(filename, binary_mode)  # type: ignore

    if format not in (None, "gz", "xz", "bz2", "zst"):
        raise ValueError(
            f"Format not supported: {format}. "
            f"Choose one of: 'gz', 'xz', 'bz2', 'zst'"
        )
    detected_format = format or _detect_format_from_extension(filepath)
    if detected_format is None and "r" in mode:
        detected_format = _detect_format_from_content(filename)

    if detected_format == "gz":
        opened_file = _open_gz(filename, binary_mode, compresslevel, threads)
    elif detected_format == "xz":
        opened_file = _open_xz(filename, binary_mode, compresslevel, threads)
    elif detected_format == "bz2":
        opened_file = _open_bz2(filename, binary_mode, compresslevel, threads)
    elif detected_format == "zst":
        opened_file = _open_zst(filename, binary_mode, compresslevel, threads)
    else:
        opened_file, _ = _file_or_path_to_binary_stream(filename, binary_mode)

    if "t" in mode:
        return io.TextIOWrapper(opened_file, encoding, errors, newline)
    return opened_file
