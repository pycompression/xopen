"""
Tests for the PipedCompression classes
"""
import gzip
import io
import os
import shutil
import sys
import time
import pytest
from pathlib import Path
from itertools import cycle

from xopen import (
    xopen,
    _PipedCompressionProgram,
    ProgramSettings,
    _MAX_PIPE_SIZE,
    PROGRAM_SETTINGS,
)

extensions = ["", ".gz", ".bz2", ".xz", ".zst"]

try:
    import fcntl

    if not hasattr(fcntl, "F_GETPIPE_SZ") and sys.platform == "linux":
        setattr(fcntl, "F_GETPIPE_SZ", 1032)
except ImportError:
    fcntl = None

base = os.path.join(os.path.dirname(__file__), "file.txt")
files = [base + ext for ext in extensions]
TEST_DIR = Path(__file__).parent
CONTENT_LINES = [b"Testing, testing ...\n", b"The second line.\n"]
CONTENT = b"".join(CONTENT_LINES)


def available_gzip_programs():
    return [prog for prog in ("gzip", "pigz") if shutil.which(prog)]


def available_bzip2_programs():
    if shutil.which("pbzip2"):
        return ["pbzip2"]
    return []


def available_xz_programs():
    if shutil.which("xz"):
        return ["xz"]
    return []


def available_zstd_programs():
    if shutil.which("zstd"):
        return ["zstd"]
    return []


PIPED_GZIP_PROGRAMS = available_gzip_programs()
PIPED_BZIP2_PROGRAMS = available_bzip2_programs()
PIPED_XZ_PROGRAMS = available_xz_programs()
PIPED_ZST_PROGRAMS = available_zstd_programs()

ALL_PROGRAMS_WITH_EXTENSION = (
    list(zip(PIPED_GZIP_PROGRAMS, cycle([".gz"])))
    + list(zip(PIPED_BZIP2_PROGRAMS, cycle([".bz2"])))
    + list(zip(PIPED_XZ_PROGRAMS, cycle([".xz"])))
    + list(zip(PIPED_ZST_PROGRAMS, cycle([".zst"])))
)


THREADED_PROGRAMS = {("pigz", ".gz"), ("pbzip2", ".bz2")} & set(
    ALL_PROGRAMS_WITH_EXTENSION
)


@pytest.fixture(params=PIPED_GZIP_PROGRAMS)
def gzip_writer(request):
    return request.param


@pytest.fixture(params=ALL_PROGRAMS_WITH_EXTENSION)
def reader(request):
    return request.param


@pytest.fixture(params=THREADED_PROGRAMS)
def threaded_reader(request):
    return request.param


@pytest.fixture(params=ALL_PROGRAMS_WITH_EXTENSION)
def writer(request):
    return request.param


def test_reader_readinto(reader):
    program, extension = reader
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], TEST_DIR / f"file.txt{extension}", "rb"
    ) as f:
        b = bytearray(len(CONTENT) + 100)
        length = f.readinto(b)
        assert length == len(CONTENT)
        assert b[:length] == CONTENT


def test_reader_textiowrapper(reader):
    program, extension = reader
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], TEST_DIR / f"file.txt{extension}", "rb"
    ) as f:
        wrapped = io.TextIOWrapper(f, encoding="utf-8")
        assert wrapped.read() == CONTENT.decode("utf-8")


def test_reader_readline(reader):
    program, extension = reader
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], TEST_DIR / f"file.txt{extension}", "rb"
    ) as f:
        assert f.readline() == CONTENT_LINES[0]


def test_reader_readlines(reader):
    program, extension = reader
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], TEST_DIR / f"file.txt{extension}", "r"
    ) as f:
        assert f.readlines() == CONTENT_LINES


@pytest.mark.parametrize("threads", [None, 1, 2])
def test_piped_reader_iter(threads, threaded_reader):
    program, extension = threaded_reader
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], TEST_DIR / f"file.txt{extension}", "rb"
    ) as f:
        lines = list(f)
        assert lines[0] == CONTENT_LINES[0]


def test_writer(tmp_path, writer):
    program, extension = writer
    path = tmp_path / f"out{extension}"
    with _PipedCompressionProgram(PROGRAM_SETTINGS[program], path, mode="wb") as f:
        f.write(b"hello")
    with xopen(path, mode="rb") as f:
        assert f.read() == b"hello"


def test_writer_has_iter_method(tmp_path, writer):
    program, extension = writer
    path = tmp_path / f"out{extension}"
    with _PipedCompressionProgram(PROGRAM_SETTINGS[program], path, mode="wb") as f:
        f.write(b"hello")
        assert hasattr(f, "__iter__")


def test_reader_iter_without_with(reader):
    program, extension = reader
    f = _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], TEST_DIR / f"file.txt{extension}"
    )
    it = iter(f)
    assert CONTENT_LINES[0] == next(it)
    f.close()


def test_reader_close(reader, create_large_file):
    program, extension = reader
    large_file = create_large_file(extension)
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], large_file, mode="rb"
    ) as f:
        f.readline()
        time.sleep(0.2)
    # The subprocess should be properly terminated now


def test_invalid_gzip_compression_level(gzip_writer, tmp_path):
    with pytest.raises(ValueError) as e:
        with _PipedCompressionProgram(
            PROGRAM_SETTINGS[gzip_writer],
            tmp_path / "out.gz",
            mode="w",
            compresslevel=17,
        ) as f:
            f.write(b"hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_invalid_xz_compression_level(tmp_path):
    with pytest.raises(ValueError) as e:
        with _PipedCompressionProgram(
            PROGRAM_SETTINGS["xz"],
            tmp_path / "out.xz",
            mode="w",
            compresslevel=17,
        ) as f:
            f.write(b"hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_invalid_zstd_compression_level(tmp_path):
    with pytest.raises(ValueError) as e:
        with _PipedCompressionProgram(
            PROGRAM_SETTINGS["zstd"], tmp_path / "out.zst", mode="w", compresslevel=25
        ) as f:
            f.write(b"hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_readers_read(reader):
    program, extension = reader
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], TEST_DIR / f"file.txt{extension}", "rb"
    ) as f:
        assert f.read() == CONTENT


@pytest.mark.skipif(
    not hasattr(fcntl, "F_GETPIPE_SZ") or _MAX_PIPE_SIZE is None,
    reason="Pipe size modifications not available on this platform.",
)
def test_pipesize_changed(tmp_path, monkeypatch):
    # Higher compression level to avoid opening with threaded opener
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS["gzip"], tmp_path / "hello.gz", "wb", compresslevel=5
    ) as f:
        assert fcntl.fcntl(f._file.fileno(), fcntl.F_GETPIPE_SZ) == _MAX_PIPE_SIZE


def test_pipedcompressionwriter_wrong_mode(tmp_path):
    with pytest.raises(ValueError) as error:
        _PipedCompressionProgram(PROGRAM_SETTINGS["gzip"], tmp_path / "test", "xb")
    error.match("Mode is 'xb', but it must be")


def test_pipedcompressionwriter_wrong_program(tmp_path):
    with pytest.raises(OSError):
        _PipedCompressionProgram(
            ProgramSettings(("XVXCLSKDLA",)), tmp_path / "test", "wb"
        )


def test_compression_level(tmp_path, gzip_writer):
    # Currently only the gzip writers handle compression levels.
    path = tmp_path / "test.gz"
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[gzip_writer], path, "wb", 2
    ) as test_h:
        test_h.write(b"test")
    assert gzip.decompress(path.read_bytes()) == b"test"


def test_iter_method_writers(writer, tmp_path):
    program, extension = writer
    writer = _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], tmp_path / f"test{extension}", "wb"
    )
    assert iter(writer) == writer
    writer.close()


def test_next_method_writers(writer, tmp_path):
    program, extension = writer
    writer = _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], tmp_path / f"test{extension}", "wb"
    )
    with pytest.raises(io.UnsupportedOperation) as error:
        next(writer)
    error.match("read")
    writer.close()


def test_pipedcompressionprogram_wrong_mode():
    with pytest.raises(ValueError) as error:
        _PipedCompressionProgram(PROGRAM_SETTINGS["gzip"], "test", "xb")
    error.match("Mode is 'xb', but it must be")


def test_piped_compression_reader_peek_binary(reader):
    program, extension = reader
    filegz = TEST_DIR / f"file.txt{extension}"
    with _PipedCompressionProgram(PROGRAM_SETTINGS[program], filegz, "rb") as read_h:
        # Peek returns at least the amount of characters but maybe more
        # depending on underlying stream. Hence startswith not ==.
        assert read_h.peek(1).startswith(b"T")


@pytest.mark.skipif(
    sys.platform != "win32", reason="seeking only works on Windows for now"
)
def test_piped_compression_reader_seek_and_tell(reader):
    program, extension = reader
    filegz = TEST_DIR / f"file.txt{extension}"
    with _PipedCompressionProgram(PROGRAM_SETTINGS[program], filegz, "rb") as f:
        original_position = f.tell()
        assert f.read(4) == b"Test"
        f.seek(original_position)
        assert f.read(8) == b"Testing,"


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_piped_compression_reader_peek_text(reader, mode):
    program, extension = reader
    compressed_file = TEST_DIR / f"file.txt{extension}"
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[program], compressed_file, mode
    ) as read_h:
        assert read_h.peek(1)[0] == CONTENT[0]


def writers_and_levels():
    for writer in PIPED_GZIP_PROGRAMS:
        if writer == "gzip":
            # Levels 1-9 are supported
            yield from ((writer, i) for i in range(1, 10))
        elif writer == "pigz":
            # Levels 0-9 + 11 are supported
            yield from ((writer, i) for i in list(range(10)) + [11])
        else:
            raise NotImplementedError(
                f"Test should be implemented for " f"{writer}"
            )  # pragma: no cover


@pytest.mark.parametrize(["writer", "level"], writers_and_levels())
def test_valid_compression_levels(writer, level, tmp_path):
    path = tmp_path / "test.gz"
    with _PipedCompressionProgram(
        PROGRAM_SETTINGS[writer], path, "wb", level
    ) as handle:
        handle.write(b"test")
    assert gzip.decompress(path.read_bytes()) == b"test"


def test_reproducible_gzip_compression(gzip_writer, tmp_path):
    path = tmp_path / "file.gz"
    with _PipedCompressionProgram(PROGRAM_SETTINGS[gzip_writer], path, mode="wb") as f:
        f.write(b"hello")

    data = path.read_bytes()
    assert (data[3] & gzip.FNAME) == 0, "gzip header contains file name"
    assert data[4:8] == b"\0\0\0\0", "gzip header contains mtime"


def test_piped_tool_fails_on_close(tmp_path):
    # This test exercises the retcode != 0 case in PipedCompressionWriter.close()
    with pytest.raises(OSError) as e:
        with _PipedCompressionProgram(
            ProgramSettings(
                (
                    sys.executable,
                    "-c",
                    "import sys\nfor line in sys.stdin: pass\nprint()\nsys.exit(5)",
                )
            ),
            tmp_path / "out.txt",
            "wb",
        ) as f:
            f.write(b"Hello")
    assert "exit code 5" in e.value.args[0]
