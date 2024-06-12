"""
Tests for the PipedCompression classes
"""
import gzip
import io
import os
import shutil
import sys
import pytest
from pathlib import Path
from itertools import cycle

from xopen import (
    xopen,
    _PipedCompressionProgram,
    _MAX_PIPE_SIZE,
    _PROGRAM_SETTINGS,
    _ProgramSettings,
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
    return [_PROGRAM_SETTINGS[prog] for prog in ("gzip", "pigz") if shutil.which(prog)]


def available_bzip2_programs():
    if shutil.which("pbzip2"):
        return [_PROGRAM_SETTINGS["pbzip2"]]
    return []


def available_xz_programs():
    if shutil.which("xz"):
        return [_PROGRAM_SETTINGS["xz"]]
    return []


def available_zstd_programs():
    if shutil.which("zstd"):
        return [_PROGRAM_SETTINGS["zstd"]]
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


THREADED_PROGRAMS = [
    settings
    for settings in ALL_PROGRAMS_WITH_EXTENSION
    if "pbzip2" in settings[0].program_args or "pigz" in settings[0].program_args
]


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
    program_settings, extension = reader
    content = CONTENT
    with _PipedCompressionProgram(
        TEST_DIR / f"file.txt{extension}", "rb", program_settings=program_settings
    ) as f:
        b = bytearray(len(content) + 100)
        length = f.readinto(b)
        assert length == len(content)
        assert b[:length] == content


def test_reader_textiowrapper(reader):
    program_settings, extension = reader
    with _PipedCompressionProgram(
        TEST_DIR / f"file.txt{extension}", "rb", program_settings=program_settings
    ) as f:
        wrapped = io.TextIOWrapper(f, encoding="utf-8")
        assert wrapped.read() == CONTENT.decode("utf-8")


def test_reader_readline(reader):
    program_settings, extension = reader
    with _PipedCompressionProgram(
        TEST_DIR / f"file.txt{extension}",
        "rb",
        program_settings=program_settings,
    ) as f:
        assert f.readline() == CONTENT_LINES[0]


def test_reader_readlines(reader):
    program_settings, extension = reader
    with _PipedCompressionProgram(
        TEST_DIR / f"file.txt{extension}", "rb", program_settings=program_settings
    ) as f:
        assert f.readlines() == CONTENT_LINES


@pytest.mark.parametrize("threads", [None, 1, 2])
def test_piped_reader_iter(threads, threaded_reader):
    program_settings, extension = threaded_reader
    with _PipedCompressionProgram(
        TEST_DIR / f"file.txt{extension}",
        "rb",
        program_settings=program_settings,
    ) as f:
        lines = list(f)
        assert lines[0] == CONTENT_LINES[0]


def test_writer(tmp_path, writer):
    program_settings, extension = writer
    path = tmp_path / f"out{extension}"
    with _PipedCompressionProgram(
        path, mode="wb", program_settings=program_settings
    ) as f:
        f.write(b"hello")
    with xopen(path, mode="rb") as f:
        assert f.read() == b"hello"


def test_writer_has_iter_method(tmp_path, writer):
    program_settings, extension = writer
    path = tmp_path / f"out{extension}"
    with _PipedCompressionProgram(
        path,
        mode="wb",
        program_settings=program_settings,
    ) as f:
        f.write(b"hello")
        assert hasattr(f, "__iter__")


def test_reader_iter_without_with(reader):
    program_settings, extension = reader
    f = _PipedCompressionProgram(
        TEST_DIR / f"file.txt{extension}", program_settings=program_settings
    )
    it = iter(f)
    assert CONTENT_LINES[0] == next(it)
    f.close()


def test_reader_close(reader, create_large_file):
    program_settings, extension = reader
    large_file = create_large_file(extension)
    with _PipedCompressionProgram(
        large_file, "rb", program_settings=program_settings
    ) as f:
        f.readline()


def test_invalid_gzip_compression_level(gzip_writer, tmp_path):
    with pytest.raises(ValueError) as e:
        with _PipedCompressionProgram(
            tmp_path / "out.gz",
            mode="w",
            compresslevel=17,
            program_settings=gzip_writer,
        ) as f:
            f.write(b"hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_invalid_xz_compression_level(tmp_path):
    with pytest.raises(ValueError) as e:
        with _PipedCompressionProgram(
            tmp_path / "out.xz",
            mode="w",
            compresslevel=17,
            program_settings=_PROGRAM_SETTINGS["xz"],
        ) as f:
            f.write(b"hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_invalid_zstd_compression_level(tmp_path):
    with pytest.raises(ValueError) as e:
        with _PipedCompressionProgram(
            tmp_path / "out.zst",
            mode="w",
            compresslevel=25,
            program_settings=_PROGRAM_SETTINGS["zstd"],
        ) as f:
            f.write(b"hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_readers_read(reader):
    program_settings, extension = reader
    with _PipedCompressionProgram(
        TEST_DIR / f"file.txt{extension}", "rb", program_settings=program_settings
    ) as f:
        assert f.read() == CONTENT


@pytest.mark.skipif(
    not hasattr(fcntl, "F_GETPIPE_SZ") or _MAX_PIPE_SIZE is None,
    reason="Pipe size modifications not available on this platform.",
)
def test_pipesize_changed(tmp_path):
    # Higher compression level to avoid opening with threaded opener
    with _PipedCompressionProgram(tmp_path / "hello.gz", "wb", compresslevel=5) as f:
        assert fcntl.fcntl(f._file.fileno(), fcntl.F_GETPIPE_SZ) == _MAX_PIPE_SIZE


def test_pipedcompressionwriter_wrong_mode(tmp_path):
    with pytest.raises(ValueError) as error:
        _PipedCompressionProgram(tmp_path / "test", "xb")
    error.match("Mode is 'xb', but it must be")


def test_pipedcompressionwriter_wrong_program(tmp_path):
    with pytest.raises(OSError):
        _PipedCompressionProgram(
            tmp_path / "test", "wb", program_settings=_ProgramSettings(("XVXCLSKDLA",))
        )


def test_compression_level(tmp_path, gzip_writer):
    # Currently only the gzip writers handle compression levels.
    path = tmp_path / "test.gz"
    with _PipedCompressionProgram(
        path, "wb", 2, program_settings=gzip_writer
    ) as test_h:
        test_h.write(b"test")
    assert gzip.decompress(path.read_bytes()) == b"test"


def test_iter_method_writers(writer, tmp_path):
    program_settings, extension = writer
    writer = _PipedCompressionProgram(
        tmp_path / f"test{extension}", "wb", program_settings=program_settings
    )
    assert iter(writer) == writer
    writer.close()


def test_next_method_writers(writer, tmp_path):
    program_settings, extension = writer
    writer = _PipedCompressionProgram(
        tmp_path / f"test{extension}", "wb", program_settings=program_settings
    )
    with pytest.raises(io.UnsupportedOperation) as error:
        next(writer)
    error.match("read")
    writer.close()


def test_pipedcompressionprogram_wrong_mode():
    with pytest.raises(ValueError) as error:
        _PipedCompressionProgram("test", "xb")
    error.match("Mode is 'xb', but it must be")


def test_piped_compression_reader_peek_binary(reader):
    program_settings, extension = reader
    filegz = TEST_DIR / f"file.txt{extension}"
    with _PipedCompressionProgram(
        filegz, "rb", program_settings=program_settings
    ) as read_h:
        # Peek returns at least the amount of characters but maybe more
        # depending on underlying stream. Hence startswith not ==.
        assert read_h.peek(1).startswith(b"T")


@pytest.mark.skipif(
    sys.platform != "win32", reason="seeking only works on Windows for now"
)
def test_piped_compression_reader_seek_and_tell(reader):
    program_settings, extension = reader
    filegz = TEST_DIR / f"file.txt{extension}"
    with _PipedCompressionProgram(filegz, "rb", program_settings=program_settings) as f:
        original_position = f.tell()
        assert f.read(4) == b"Test"
        f.seek(original_position)
        assert f.read(8) == b"Testing,"


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_piped_compression_reader_peek_text(reader, mode):
    program_settings, extension = reader
    compressed_file = TEST_DIR / f"file.txt{extension}"
    with _PipedCompressionProgram(
        compressed_file, mode, program_settings=program_settings
    ) as read_h:
        assert read_h.peek(1)[0] == CONTENT[0]


def writers_and_levels():
    for writer in PIPED_GZIP_PROGRAMS:
        if "gzip" in writer.program_args:
            # Levels 1-9 are supported
            yield from ((writer, i) for i in range(1, 10))
        elif "pigz" in writer.program_args:
            # Levels 0-9 + 11 are supported
            yield from ((writer, i) for i in list(range(10)) + [11])
        else:
            raise NotImplementedError(
                f"Test should be implemented for " f"{writer}"
            )  # pragma: no cover


@pytest.mark.parametrize(["writer", "level"], writers_and_levels())
def test_valid_compression_levels(writer, level, tmp_path):
    path = tmp_path / "test.gz"
    with _PipedCompressionProgram(path, "wb", level, program_settings=writer) as handle:
        handle.write(b"test")
    assert gzip.decompress(path.read_bytes()) == b"test"


def test_reproducible_gzip_compression(gzip_writer, tmp_path):
    path = tmp_path / "file.gz"
    with _PipedCompressionProgram(path, mode="wb", program_settings=gzip_writer) as f:
        f.write(b"hello")

    data = path.read_bytes()
    assert (data[3] & gzip.FNAME) == 0, "gzip header contains file name"
    assert data[4:8] == b"\0\0\0\0", "gzip header contains mtime"


def test_piped_tool_fails_on_close(tmp_path):
    # This test exercises the retcode != 0 case in PipedCompressionWriter.close()
    with pytest.raises(OSError) as e:
        with _PipedCompressionProgram(
            tmp_path / "out.txt",
            "wb",
            program_settings=_ProgramSettings(
                (
                    sys.executable,
                    "-c",
                    "import sys\nfor line in sys.stdin: pass\nprint()\nsys.exit(5)",
                ),
            ),
        ) as f:
            f.write(b"Hello")
    assert "exit code 5" in e.value.args[0]
