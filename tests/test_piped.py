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
    PipedCompressionReader,
    PipedCompressionWriter,
    PipedGzipReader,
    PipedGzipWriter,
    PipedPBzip2Reader,
    PipedPBzip2Writer,
    PipedPigzReader,
    PipedPigzWriter,
    PipedIGzipReader,
    PipedIGzipWriter,
    PipedPythonIsalReader,
    PipedPythonIsalWriter,
    PipedXzReader,
    PipedXzWriter,
    PipedZstdReader,
    PipedZstdWriter,
    _MAX_PIPE_SIZE,
    _can_read_concatenated_gz,
    igzip,
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


def available_gzip_readers_and_writers():
    readers = [
        klass
        for prog, klass in [
            ("gzip", PipedGzipReader),
            ("pigz", PipedPigzReader),
            ("igzip", PipedIGzipReader),
        ]
        if shutil.which(prog)
    ]
    if PipedIGzipReader in readers and not _can_read_concatenated_gz("igzip"):
        readers.remove(PipedIGzipReader)

    writers = [
        klass
        for prog, klass in [
            ("gzip", PipedGzipWriter),
            ("pigz", PipedPigzWriter),
            ("igzip", PipedIGzipWriter),
        ]
        if shutil.which(prog)
    ]
    if igzip is not None:
        readers.append(PipedPythonIsalReader)
        writers.append(PipedPythonIsalWriter)
    return readers, writers


PIPED_GZIP_READERS, PIPED_GZIP_WRITERS = available_gzip_readers_and_writers()


def available_bzip2_readers_and_writers():
    if shutil.which("pbzip2"):
        return [PipedPBzip2Reader], [PipedPBzip2Writer]
    return [], []


def available_xz_readers_and_writers():
    result = [], []
    if shutil.which("xz"):
        result = [PipedXzReader], [PipedXzWriter]
    return result


def available_zstd_readers_and_writers():
    result = [], []
    if shutil.which("zstd"):
        result = [PipedZstdReader], [PipedZstdWriter]
    return result


PIPED_BZIP2_READERS, PIPED_BZIP2_WRITERS = available_bzip2_readers_and_writers()
PIPED_XZ_READERS, PIPED_XZ_WRITERS = available_xz_readers_and_writers()
PIPED_ZST_READERS, PIPED_ZST_WRITERS = available_zstd_readers_and_writers()

ALL_READERS_WITH_EXTENSION = (
    list(zip(PIPED_GZIP_READERS, cycle([".gz"])))
    + list(zip(PIPED_BZIP2_READERS, cycle([".bz2"])))
    + list(zip(PIPED_XZ_READERS, cycle([".xz"])))
    + list(zip(PIPED_ZST_READERS, cycle([".zst"])))
)
ALL_WRITERS_WITH_EXTENSION = (
    list(zip(PIPED_GZIP_WRITERS, cycle([".gz"])))
    + list(zip(PIPED_BZIP2_WRITERS, cycle([".bz2"])))
    + list(zip(PIPED_XZ_WRITERS, cycle([".xz"])))
    + list(zip(PIPED_ZST_WRITERS, cycle([".zst"])))
)


THREADED_READERS = set([(PipedPigzReader, ".gz"), (PipedPBzip2Reader, ".bz2")]) & set(
    ALL_READERS_WITH_EXTENSION
)


@pytest.fixture(params=PIPED_GZIP_WRITERS)
def gzip_writer(request):
    return request.param


@pytest.fixture(params=ALL_READERS_WITH_EXTENSION)
def reader(request):
    return request.param


@pytest.fixture(params=THREADED_READERS)
def threaded_reader(request):
    return request.param


@pytest.fixture(params=ALL_WRITERS_WITH_EXTENSION)
def writer(request):
    return request.param


def test_reader_readinto(reader):
    opener, extension = reader
    content = CONTENT
    with opener(TEST_DIR / f"file.txt{extension}", "rb") as f:
        b = bytearray(len(content) + 100)
        length = f.readinto(b)
        assert length == len(content)
        assert b[:length] == content


def test_reader_textiowrapper(reader):
    opener, extension = reader
    with opener(TEST_DIR / f"file.txt{extension}", "rb") as f:
        wrapped = io.TextIOWrapper(f, encoding="utf-8")
        assert wrapped.read() == CONTENT.decode("utf-8")


def test_reader_readline(reader):
    opener, extension = reader
    first_line = CONTENT_LINES[0]
    with opener(TEST_DIR / f"file.txt{extension}", "rb") as f:
        assert f.readline() == first_line


def test_reader_readlines(reader):
    opener, extension = reader
    with opener(TEST_DIR / f"file.txt{extension}", "r") as f:
        assert f.readlines() == CONTENT_LINES


@pytest.mark.parametrize("threads", [None, 1, 2])
def test_piped_reader_iter(threads, threaded_reader):
    opener, extension = threaded_reader
    with opener(TEST_DIR / f"file.txt{extension}", mode="r", threads=threads) as f:
        lines = list(f)
        assert lines[0] == CONTENT_LINES[0]


def test_writer(tmp_path, writer):
    opener, extension = writer
    print(opener, writer)
    print(repr(opener))
    path = tmp_path / f"out{extension}"
    with opener(path, mode="wb") as f:
        print(f)
        f.write(b"hello")
    with xopen(path, mode="rb") as f:
        assert f.read() == b"hello"


def test_writer_has_iter_method(tmp_path, writer):
    opener, extension = writer
    with opener(tmp_path / f"out{extension}") as f:
        f.write(b"hello")
        assert hasattr(f, "__iter__")


def test_reader_iter_without_with(reader):
    opener, extension = reader
    f = opener(TEST_DIR / f"file.txt{extension}")
    it = iter(f)
    assert CONTENT_LINES[0] == next(it)
    f.close()


def test_reader_close(reader, create_large_file):
    reader, extension = reader
    large_file = create_large_file(extension)
    with reader(large_file, mode="rb") as f:
        f.readline()
        time.sleep(0.2)
    # The subprocess should be properly terminated now


def test_invalid_gzip_compression_level(gzip_writer, tmp_path):
    with pytest.raises(ValueError) as e:
        with gzip_writer(tmp_path / "out.gz", mode="w", compresslevel=17) as f:
            f.write("hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_invalid_xz_compression_level(tmp_path):
    with pytest.raises(ValueError) as e:
        with PipedXzWriter(tmp_path / "out.xz", mode="w", compresslevel=10) as f:
            f.write("hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_invalid_zstd_compression_level(tmp_path):
    with pytest.raises(ValueError) as e:
        with PipedZstdWriter(tmp_path / "out.zst", mode="w", compresslevel=25) as f:
            f.write("hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_readers_read(reader):
    opener, extension = reader
    with opener(TEST_DIR / f"file.txt{extension}", "rb") as f:
        assert f.read() == CONTENT


@pytest.mark.skipif(
    sys.platform.startswith("win"),
    reason="Windows does not have a gzip application by default.",
)
def test_concatenated_gzip_function():
    assert _can_read_concatenated_gz("gzip") is True
    assert _can_read_concatenated_gz("pigz") is True
    assert _can_read_concatenated_gz("cat") is False


@pytest.mark.skipif(
    not hasattr(fcntl, "F_GETPIPE_SZ") or _MAX_PIPE_SIZE is None,
    reason="Pipe size modifications not available on this platform.",
)
def test_pipesize_changed(tmp_path, monkeypatch):
    # Higher compression level to avoid opening with threaded opener
    with PipedGzipWriter(tmp_path / "hello.gz", "wb", compresslevel=5) as f:
        assert isinstance(f, PipedCompressionWriter)
        assert fcntl.fcntl(f._file.fileno(), fcntl.F_GETPIPE_SZ) == _MAX_PIPE_SIZE


def test_pipedcompressionwriter_wrong_mode(tmp_path):
    with pytest.raises(ValueError) as error:
        PipedCompressionWriter(tmp_path / "test", ["gzip"], "xb")
    error.match("Mode is 'xb', but it must be")


def test_pipedcompressionwriter_wrong_program(tmp_path):
    with pytest.raises(OSError):
        PipedCompressionWriter(tmp_path / "test", ["XVXCLSKDLA"], "wb")


def test_compression_level(tmp_path, gzip_writer):
    # Currently only the gzip writers handle compression levels.
    path = tmp_path / "test.gz"
    with gzip_writer(path, "wb", 2) as test_h:
        test_h.write(b"test")
    assert gzip.decompress(path.read_bytes()) == b"test"


def test_iter_method_writers(writer, tmp_path):
    opener, extension = writer
    writer = opener(tmp_path / f"test{extension}", "wb")
    assert iter(writer) == writer
    writer.close()


def test_next_method_writers(writer, tmp_path):
    opener, extension = writer
    writer = opener(tmp_path / f"test{extension}", "wb")
    with pytest.raises(io.UnsupportedOperation) as error:
        next(writer)
    error.match("not readable")
    writer.close()


def test_pipedcompressionreader_wrong_mode():
    with pytest.raises(ValueError) as error:
        PipedCompressionReader("test", ["gzip"], "xb")
    error.match("Mode is 'xb', but it must be")


def test_piped_compression_reader_peek_binary(reader):
    opener, extension = reader
    filegz = TEST_DIR / f"file.txt{extension}"
    with opener(filegz, "rb") as read_h:
        # Peek returns at least the amount of characters but maybe more
        # depending on underlying stream. Hence startswith not ==.
        assert read_h.peek(1).startswith(b"T")


@pytest.mark.skipif(
    sys.platform != "win32", reason="seeking only works on Windows for now"
)
def test_piped_compression_reader_seek_and_tell(reader):
    opener, extension = reader
    filegz = TEST_DIR / f"file.txt{extension}"
    with opener(filegz, "rb") as f:
        original_position = f.tell()
        assert f.read(4) == b"Test"
        f.seek(original_position)
        assert f.read(8) == b"Testing,"


@pytest.mark.parametrize("mode", ["r", "rb"])
def test_piped_compression_reader_peek_text(reader, mode):
    opener, extension = reader
    compressed_file = TEST_DIR / f"file.txt{extension}"
    with opener(compressed_file, mode) as read_h:
        assert read_h.peek(1)[0] == CONTENT[0]


def writers_and_levels():
    for writer in PIPED_GZIP_WRITERS:
        if writer == PipedGzipWriter:
            # Levels 1-9 are supported
            yield from ((writer, i) for i in range(1, 10))
        elif writer == PipedPigzWriter:
            # Levels 0-9 + 11 are supported
            yield from ((writer, i) for i in list(range(10)) + [11])
        elif writer == PipedIGzipWriter or writer == PipedPythonIsalWriter:
            # Levels 0-3 are supported
            yield from ((writer, i) for i in range(4))
        else:
            raise NotImplementedError(
                f"Test should be implemented for " f"{writer}"
            )  # pragma: no cover


@pytest.mark.parametrize(["writer", "level"], writers_and_levels())
def test_valid_compression_levels(writer, level, tmp_path):
    path = tmp_path / "test.gz"
    with writer(path, "wb", level) as handle:
        handle.write(b"test")
    assert gzip.decompress(path.read_bytes()) == b"test"


def test_reproducible_gzip_compression(gzip_writer, tmp_path):
    path = tmp_path / "file.gz"
    with gzip_writer(path, mode="wb") as f:
        f.write(b"hello")

    data = path.read_bytes()
    assert (data[3] & gzip.FNAME) == 0, "gzip header contains file name"
    assert data[4:8] == b"\0\0\0\0", "gzip header contains mtime"


def test_piped_tool_fails_on_close(tmp_path):
    # This test exercises the retcode != 0 case in PipedCompressionWriter.close()
    with pytest.raises(OSError) as e:
        with PipedCompressionWriter(
            tmp_path / "out.txt",
            [
                sys.executable,
                "-c",
                "import sys\nfor line in sys.stdin: pass\nprint()\nsys.exit(5)",
            ],
        ) as f:
            f.write(b"Hello")
    assert "terminated with exit code 5" in e.value.args[0]
