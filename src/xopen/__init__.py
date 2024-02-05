"""
Open compressed files transparently.
"""

__all__ = [
    "xopen",
    "_PipedCompressionProgram",
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
import typing
from subprocess import Popen, PIPE
from typing import (
    Any,
    Dict,
    Optional,
    Union,
    TextIO,
    IO,
    Sequence,
    Container,
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


# Rather than using a dict, use a NamedTuple with _asdict to enforce presence
# of certain members and type checking.
class _ProgramSettings(typing.NamedTuple):
    program_args: Tuple[str, ...]
    acceptable_compression_levels: Tuple[int, ...] = tuple(range(1, 10))
    threads_flag: Optional[str] = None
    # This exit code is not interpreted as an error when terminating the process
    allowed_exit_code: Optional[int] = -signal.SIGTERM
    # If this message is printed on stderr on terminating the process,
    # it is not interpreted as an error
    allowed_exit_message: Optional[bytes] = None


PROGRAM_SETTINGS: Dict[str, _ProgramSettings] = {
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


def _program_settings(program: str) -> Dict[str, Any]:
    return PROGRAM_SETTINGS[program]._asdict()


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


class _PipedCompressionProgram(io.IOBase):
    """
    Read and write compressed files by running an external process and piping into it.
    """

    def __init__(  # noqa: C901
        self,
        path: FilePath,
        mode="rb",
        compresslevel: Optional[int] = None,
        threads: Optional[int] = None,
        program_args: Optional[Sequence[str]] = None,
        threads_flag: Optional[str] = None,
        # This exit code is not interpreted as an error when terminating the process
        allowed_exit_code: Optional[int] = -signal.SIGTERM,
        # If this message is printed on stderr on terminating the process,
        # it is not interpreted as an error
        allowed_exit_message: Optional[bytes] = None,
        acceptable_compression_levels: Container[int] = tuple(range(0, 10)),
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
        if program_args is None:
            program_args = ("gzip", "--no-name")
        self._error_raised = False
        self._program_args = list(program_args)
        self._allowed_exit_code = allowed_exit_code
        self._allowed_exit_message = allowed_exit_message
        if mode not in ("r", "rb", "w", "wb", "a", "ab"):
            raise ValueError(
                f"Mode is '{mode}', but it must be 'r', 'rb', 'w', 'wb', 'a', or 'ab'"
            )
        if (
            compresslevel is not None
            and compresslevel not in acceptable_compression_levels
        ):
            raise ValueError(
                f"compresslevel must be in {acceptable_compression_levels}."
            )
        path = os.fspath(path)
        if isinstance(path, bytes) and sys.platform == "win32":
            path = path.decode()
        self.name: str = str(path)
        self._mode: str = mode
        self._stderr = tempfile.TemporaryFile("w+b")
        self._threads_flag: Optional[str] = threads_flag

        if threads is None:
            if "r" in mode:
                # Reading occurs single threaded by default. This has the least
                # amount of overhead and is fast enough for most use cases.
                threads = 1
            else:
                threads = min(_available_cpu_count(), 4)
        self._threads = threads

        if threads != 0 and self._threads_flag is not None:
            self._program_args += [f"{self._threads_flag}{self._threads}"]

        # Setting close_fds to True in the Popen arguments is necessary due to
        # <http://bugs.python.org/issue12786>.
        # However, close_fds is not supported on Windows. See
        # <https://github.com/marcelm/cutadapt/issues/315>.
        close_fds = False
        if sys.platform != "win32":
            close_fds = True

        if "r" in mode:
            self._program_args += ["-c", "-d", path]  # type: ignore
            self.outfile = None
            self.process = subprocess.Popen(
                self._program_args,
                stderr=self._stderr,
                stdout=PIPE,
                close_fds=close_fds,
            )  # type: ignore
            self._file: BinaryIO = self.process.stdout  # type: ignore
            self._wait_for_output_or_process_exit()
            self._raise_if_error()
        else:
            if compresslevel is not None:
                self._program_args += ["-" + str(compresslevel)]
            self.outfile = open(path, mode[0] + "b")
            try:
                self.process = Popen(
                    self._program_args,
                    stderr=self._stderr,
                    stdin=PIPE,
                    stdout=self.outfile,
                    close_fds=close_fds,
                )  # type: ignore
            except OSError:
                self.outfile.close()
                raise
            assert self.process.stdin is not None
            self._file = self.process.stdin  # type: ignore

        _set_pipe_size_to_max(self._file.fileno())

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"('{self.name}', mode='{self._mode}', "
            f"program='{' '.join(self._program_args)}', "
            f"threads={self._threads})"
        )

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
        if self.outfile:  # Opened for writing.
            self._file.close()
            self.process.wait()
            self.outfile.close()
        else:
            retcode = self.process.poll()
            if retcode is None:
                # still running
                self.process.terminate()
                check_allowed_code_and_message = True
                self.process.wait()
            self._file.close()
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


def _open_stdin_or_out(mode: str):
    assert "b" in mode
    std = sys.stdout if "w" in mode else sys.stdin
    return open(std.fileno(), mode=mode, closefd=False)


def _open_bz2(filename, mode: str, threads: Optional[int]):
    assert "b" in mode
    if threads != 0:
        try:
            return _PipedCompressionProgram(
                filename, mode, threads=threads, **_program_settings("pbzip2")
            )
        except OSError:
            pass  # We try without threads.

    return bz2.open(filename, mode)


def _open_xz(
    filename,
    mode: str,
    compresslevel: Optional[int],
    threads: Optional[int],
):
    assert "b" in mode
    if compresslevel is None:
        compresslevel = 6

    if threads != 0:
        try:
            return _PipedCompressionProgram(
                filename, mode, compresslevel, threads, **_program_settings("xz")
            )
        except OSError:
            pass  # We try without threads.

    return lzma.open(
        filename,
        mode,
        preset=compresslevel if "w" in mode else None,
    )


def _open_zst(  # noqa: C901
    filename,
    mode: str,
    compresslevel: Optional[int],
    threads: Optional[int],
):
    assert "b" in mode
    assert compresslevel != 0
    if compresslevel is None:
        compresslevel = 3
    if threads != 0:
        try:
            return _PipedCompressionProgram(
                filename, mode, compresslevel, threads, **_program_settings("zstd")
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
    f = zstandard.open(filename, mode, cctx=cctx)
    if mode == "rb":
        return io.BufferedReader(f)
    elif mode == "wb":
        return io.BufferedWriter(f)
    return f


def _open_gz(  # noqa: C901
    filename, mode: str, compresslevel, threads, **text_mode_kwargs
):
    assert "b" in mode
    if compresslevel is None:
        # Force the same compression level on every tool regardless of
        # library defaults
        compresslevel = XOPEN_DEFAULT_GZIP_COMPRESSION

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
            try:
                return gzip_ng_threaded.open(
                    filename,
                    mode,
                    # zlib-ng level 1 is 50% bigger than zlib level 1. Level
                    # 2 gives a size close to expectations.
                    compresslevel=2 if compresslevel == 1 else compresslevel,
                    threads=threads or max(_available_cpu_count(), 4),
                )
            except zlib_ng.error:  # Bad compression level
                pass
        try:
            try:
                return _PipedCompressionProgram(
                    filename, mode, compresslevel, threads, **_program_settings("pigz")
                )
            except OSError:
                return _PipedCompressionProgram(
                    filename, mode, compresslevel, threads, **_program_settings("gzip")
                )
        except OSError:
            pass  # We try without threads.

    return _open_reproducible_gzip(
        filename,
        mode=mode,
        compresslevel=compresslevel,
    )


def _open_reproducible_gzip(filename, mode: str, compresslevel: int):
    """
    Open a gzip file for writing (without external processes)
    that has neither mtime nor the file name in the header
    (equivalent to gzip --no-name)
    """
    assert mode in ("rb", "wb", "ab")
    assert compresslevel is not None
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
    filename = os.fspath(filename)

    if format not in (None, "gz", "xz", "bz2", "zst"):
        raise ValueError(
            f"Format not supported: {format}. "
            f"Choose one of: 'gz', 'xz', 'bz2', 'zst'"
        )
    detected_format = format or _detect_format_from_extension(filename)
    if detected_format is None and "w" not in mode:
        detected_format = _detect_format_from_content(filename)

    if filename == "-":
        opened_file = _open_stdin_or_out(binary_mode)
    elif detected_format == "gz":
        opened_file = _open_gz(filename, binary_mode, compresslevel, threads)
    elif detected_format == "xz":
        opened_file = _open_xz(filename, binary_mode, compresslevel, threads)
    elif detected_format == "bz2":
        opened_file = _open_bz2(filename, binary_mode, threads)
    elif detected_format == "zst":
        opened_file = _open_zst(filename, binary_mode, compresslevel, threads)
    else:
        opened_file = open(filename, binary_mode)  # type: ignore

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
    if "t" in mode:
        return io.TextIOWrapper(opened_file, encoding, errors, newline)
    return opened_file
