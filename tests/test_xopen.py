import functools
import gzip
import bz2
import itertools
import lzma
import io
import os
import random
import shutil
import sys
import time
import pytest
from pathlib import Path
from contextlib import contextmanager
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
    _MAX_PIPE_SIZE,
    _can_read_concatenated_gz,
    igzip,
)
extensions = ["", ".gz", ".bz2", ".xz"]

try:
    import fcntl
    if not hasattr(fcntl, "F_GETPIPE_SZ") and sys.platform == "linux":
        setattr(fcntl, "F_GETPIPE_SZ", 1032)
except ImportError:
    fcntl = None

base = os.path.join(os.path.dirname(__file__), "file.txt")
files = [base + ext for ext in extensions]
TEST_DIR = Path(__file__).parent
CONTENT_LINES = ['Testing, testing ...\n', 'The second line.\n']
CONTENT = ''.join(CONTENT_LINES)


def available_gzip_readers_and_writers():
    readers = [
        klass for prog, klass in [
            ("gzip", PipedGzipReader),
            ("pigz", PipedPigzReader),
            ("igzip", PipedIGzipReader),
        ]
        if shutil.which(prog)
    ]
    if PipedIGzipReader in readers and not _can_read_concatenated_gz("igzip"):
        readers.remove(PipedIGzipReader)

    writers = [
        klass for prog, klass in [
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


PIPED_BZIP2_READERS, PIPED_BZIP2_WRITERS = available_bzip2_readers_and_writers()

ALL_READERS_WITH_EXTENSION = list(zip(PIPED_GZIP_READERS, cycle([".gz"]))) + \
                             list(zip(PIPED_BZIP2_READERS, cycle([".bz2"])))
ALL_WRITERS_WITH_EXTENSION = list(zip(PIPED_GZIP_WRITERS, cycle([".gz"]))) + \
                             list(zip(PIPED_BZIP2_WRITERS, cycle([".bz2"])))


THREADED_READERS = set([(PipedPigzReader, ".gz"), (PipedPBzip2Reader, ".bz2")]) & \
                   set(ALL_READERS_WITH_EXTENSION)


@pytest.fixture(params=PIPED_GZIP_WRITERS)
def gzip_writer(request):
    return request.param


@pytest.fixture(params=extensions)
def ext(request):
    return request.param


@pytest.fixture(params=files)
def fname(request):
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


@contextmanager
def disable_binary(tmp_path, binary_name):
    """
    Find the location of the binary by its name, then set PATH to a directory that contains
    the binary with permissions set to 000. If no suitable binary could be found,
    PATH is set to an empty directory
    """
    try:
        binary_path = shutil.which(binary_name)
        if binary_path:
            shutil.copy(binary_path, str(tmp_path))
            os.chmod(str(tmp_path / binary_name), 0)
        path = os.environ["PATH"]
        os.environ["PATH"] = str(tmp_path)
        yield
    finally:
        os.environ["PATH"] = path


@pytest.fixture
def lacking_pigz_permissions(tmp_path):
    with disable_binary(tmp_path, "pigz"):
        yield


@pytest.fixture
def lacking_pbzip2_permissions(tmp_path):
    with disable_binary(tmp_path, "pbzip2"):
        yield


@pytest.fixture(params=[1024, 2048, 4096])
def create_large_file(tmpdir, request):
    def _create_large_file(extension):
        path = str(tmpdir.join(f"large{extension}"))
        random_text = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ\n') for _ in range(1024))
        # Make the text a lot bigger in order to ensure that it is larger than the
        # pipe buffer size.
        random_text *= request.param
        with xopen(path, 'w') as f:
            f.write(random_text)
        return path
    return _create_large_file


@pytest.fixture
def create_truncated_file(create_large_file):
    def _create_truncated_file(extension):
        large_file = create_large_file(extension)
        with open(large_file, 'a') as f:
            f.truncate(os.stat(large_file).st_size - 10)
        return large_file
    return _create_truncated_file


@pytest.fixture
def xopen_without_igzip(monkeypatch):
    import xopen  # xopen local overrides xopen global variable
    monkeypatch.setattr(xopen, "igzip", None)
    return xopen.xopen


def test_xopen_text(fname):
    with xopen(fname, 'rt') as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == 'The second line.\n', fname


def test_xopen_binary(fname):
    with xopen(fname, 'rb') as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == b'The second line.\n', fname


def test_xopen_binary_no_isal_no_threads(fname, xopen_without_igzip):
    with xopen_without_igzip(fname, 'rb', threads=0) as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == b'The second line.\n', fname


def test_xopen_binary_no_isal(fname, xopen_without_igzip):
    with xopen_without_igzip(fname, 'rb', threads=1) as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == b'The second line.\n', fname


def test_no_context_manager_text(fname):
    f = xopen(fname, 'rt')
    lines = list(f)
    assert len(lines) == 2
    assert lines[1] == 'The second line.\n', fname
    f.close()
    assert f.closed


def test_no_context_manager_binary(fname):
    f = xopen(fname, 'rb')
    lines = list(f)
    assert len(lines) == 2
    assert lines[1] == b'The second line.\n', fname
    f.close()
    assert f.closed


def test_xopen_bytes_path(fname):
    path = fname.encode('utf-8')
    with xopen(path, 'rt') as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == 'The second line.\n', fname


def test_readinto(fname):
    content = CONTENT.encode('utf-8')
    with xopen(fname, 'rb') as f:
        b = bytearray(len(content) + 100)
        length = f.readinto(b)
        assert length == len(content)
        assert b[:length] == content


def test_reader_readinto(reader):
    opener, extension = reader
    content = CONTENT.encode('utf-8')
    with opener(TEST_DIR / f"file.txt{extension}", "rb") as f:
        b = bytearray(len(content) + 100)
        length = f.readinto(b)
        assert length == len(content)
        assert b[:length] == content


def test_reader_textiowrapper(reader):
    opener, extension = reader
    with opener(TEST_DIR / f"file.txt{extension}", "rb") as f:
        wrapped = io.TextIOWrapper(f)
        assert wrapped.read() == CONTENT


def test_detect_file_format_from_content(ext, tmp_path):
    path = str(tmp_path / f"file.txt{ext}.test")
    shutil.copy(TEST_DIR / f"file.txt{ext}", path)
    with xopen(path, "rb") as fh:
        assert fh.readline() == CONTENT_LINES[0].encode("utf-8")


def test_readline(fname):
    first_line = CONTENT_LINES[0].encode('utf-8')
    with xopen(fname, 'rb') as f:
        assert f.readline() == first_line


def test_readline_text(fname):
    with xopen(fname, 'r') as f:
        assert f.readline() == CONTENT_LINES[0]


def test_reader_readline(reader):
    opener, extension = reader
    first_line = CONTENT_LINES[0].encode('utf-8')
    with opener(TEST_DIR / f"file.txt{extension}", "rb") as f:
        assert f.readline() == first_line


def test_reader_readline_text(reader):
    opener, extension = reader
    with opener(TEST_DIR / f"file.txt{extension}", "r") as f:
        assert f.readline() == CONTENT_LINES[0]


@pytest.mark.parametrize("threads", [None, 1, 2])
def test_piped_reader_iter(threads, threaded_reader):
    opener, extension = threaded_reader
    with opener(TEST_DIR / f"file.txt{extension}", mode="r", threads=threads) as f:
        lines = list(f)
        assert lines[0] == CONTENT_LINES[0]


def test_next(fname):
    with xopen(fname, "rt") as f:
        _ = next(f)
        line2 = next(f)
        assert line2 == 'The second line.\n', fname


def test_xopen_has_iter_method(ext, tmpdir):
    path = str(tmpdir.join("out" + ext))
    with xopen(path, mode='w') as f:
        assert hasattr(f, '__iter__')


def test_writer_has_iter_method(tmpdir, writer):
    opener, extension = writer
    with opener(str(tmpdir.join(f"out.{extension}"))) as f:
        assert hasattr(f, '__iter__')


def test_iter_without_with(fname):
    f = xopen(fname, "rt")
    it = iter(f)
    assert CONTENT_LINES[0] == next(it)
    f.close()


def test_reader_iter_without_with(reader):
    opener, extension = reader
    it = iter(opener(TEST_DIR / f"file.txt{extension}"))
    assert CONTENT_LINES[0] == next(it)


@pytest.mark.parametrize("mode", ["rb", "rt"])
def test_reader_close(mode, reader, create_large_file):
    reader, extension = reader
    large_file = create_large_file(extension)
    with reader(large_file, mode=mode) as f:
        f.readline()
        time.sleep(0.2)
    # The subprocess should be properly terminated now


@pytest.mark.parametrize("extension", [".gz", ".bz2"])
def test_partial_iteration_closes_correctly(extension, create_large_file):
    class LineReader:
        def __init__(self, file):
            self.file = xopen(file, "rb")

        def __iter__(self):
            wrapper = io.TextIOWrapper(self.file)
            yield from wrapper
    large_file = create_large_file(extension)
    f = LineReader(large_file)
    next(iter(f))
    f.file.close()


def test_nonexisting_file(ext):
    with pytest.raises(IOError):
        with xopen('this-file-does-not-exist' + ext):
            pass  # pragma: no cover


def test_write_to_nonexisting_dir(ext):
    with pytest.raises(IOError):
        with xopen('this/path/does/not/exist/file.txt' + ext, 'w'):
            pass  # pragma: no cover


def test_invalid_mode(ext):
    with pytest.raises(ValueError):
        with xopen(TEST_DIR / f"file.txt.{ext}", mode="hallo"):
            pass  # pragma: no cover


def test_filename_not_a_string():
    with pytest.raises(TypeError):
        with xopen(123, mode="r"):
            pass  # pragma: no cover


def test_invalid_compression_level(tmpdir):
    path = str(tmpdir.join("out.gz"))
    with pytest.raises(ValueError) as e:
        with xopen(path, mode="w", compresslevel=17) as f:
            f.write("hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


def test_invalid_compression_level_writers(gzip_writer, tmpdir):
    # Currently only gzip writers handle compression levels
    path = str(tmpdir.join("out.gz"))
    with pytest.raises(ValueError) as e:
        with gzip_writer(path, mode="w", compresslevel=17) as f:
            f.write("hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


@pytest.mark.parametrize("ext", extensions)
def test_append(ext, tmpdir):
    text = b"AB"
    reference = text + text
    path = str(tmpdir.join("the-file" + ext))
    with xopen(path, "ab") as f:
        f.write(text)
    with xopen(path, "ab") as f:
        f.write(text)
    with xopen(path, "r") as f:
        for appended in f:
            pass
        reference = reference.decode("utf-8")
        assert appended == reference


@pytest.mark.parametrize("ext", extensions)
def test_append_text(ext, tmpdir):
    text = "AB"
    reference = text + text
    path = str(tmpdir.join("the-file" + ext))
    with xopen(path, "at") as f:
        f.write(text)
    with xopen(path, "at") as f:
        f.write(text)
    with xopen(path, "rt") as f:
        for appended in f:
            pass
        assert appended == reference


class TookTooLongError(Exception):
    pass


@pytest.mark.timeout(5)
@pytest.mark.parametrize("extension", [".gz", ".bz2"])
def test_truncated_file(extension, create_truncated_file):
    truncated_file = create_truncated_file(extension)
    with pytest.raises((EOFError, IOError)):
        f = xopen(truncated_file, "r")
        f.read()
        f.close()  # pragma: no cover


@pytest.mark.timeout(5)
@pytest.mark.parametrize("extension", [".gz", ".bz2"])
def test_truncated_iter(extension, create_truncated_file):
    truncated_file = create_truncated_file(extension)
    with pytest.raises((EOFError, IOError)):
        f = xopen(truncated_file, 'r')
        for line in f:
            pass
        f.close()  # pragma: no cover


@pytest.mark.timeout(5)
@pytest.mark.parametrize("extension", [".gz", ".bz2"])
def test_truncated_with(extension, create_truncated_file):
    truncated_file = create_truncated_file(extension)
    with pytest.raises((EOFError, IOError)):
        with xopen(truncated_file, 'r') as f:
            f.read()


@pytest.mark.timeout(5)
@pytest.mark.parametrize("extension", [".gz", ".bz2"])
def test_truncated_iter_with(extension, create_truncated_file):
    truncated_file = create_truncated_file(extension)
    with pytest.raises((EOFError, IOError)):
        with xopen(truncated_file, 'r') as f:
            for line in f:
                pass


def test_bare_read_from_gz():
    hello_file = Path(__file__).parent / "hello.gz"
    with xopen(hello_file, 'rt') as f:
        assert f.read() == 'hello'


def test_readers_read(reader):
    opener, extension = reader
    with opener(TEST_DIR / f'file.txt{extension}', 'rt') as f:
        assert f.read() == CONTENT


def test_write_threads(tmpdir, ext):
    path = str(tmpdir.join(f'out.{ext}'))
    with xopen(path, mode='w', threads=3) as f:
        f.write('hello')
    with xopen(path) as f:
        assert f.read() == 'hello'


def test_write_pigz_threads_no_isal(tmpdir, xopen_without_igzip):
    path = str(tmpdir.join('out.gz'))
    with xopen_without_igzip(path, mode='w', threads=3) as f:
        f.write('hello')
    with xopen_without_igzip(path) as f:
        assert f.read() == 'hello'


def test_read_no_threads(ext):
    klasses = {
        ".bz2": bz2.BZ2File,
        ".gz": gzip.GzipFile,
        ".xz": lzma.LZMAFile,
        "": io.BufferedReader,
    }
    klass = klasses[ext]
    with xopen(TEST_DIR / f"file.txt{ext}", "rb", threads=0) as f:
        assert isinstance(f, klass), f


def test_write_no_threads(tmpdir, ext):
    klasses = {
        ".bz2": bz2.BZ2File,
        ".gz": gzip.GzipFile,
        ".xz": lzma.LZMAFile,
        "": io.BufferedWriter,
    }
    klass = klasses[ext]
    path = str(tmpdir.join(f"out.{ext}"))
    with xopen(path, "wb", threads=0) as f:
        assert isinstance(f, io.BufferedWriter)
        if ext:
            assert isinstance(f.raw, klass), f


def test_write_gzip_no_threads_no_isal(tmpdir, xopen_without_igzip):
    import gzip
    path = str(tmpdir.join("out.gz"))
    with xopen_without_igzip(path, "wb", threads=0) as f:
        assert isinstance(f.raw, gzip.GzipFile), f


def test_write_stdout():
    f = xopen('-', mode='w')
    print("Hello", file=f)
    f.close()
    # ensure stdout is not closed
    print("Still there?")


def test_write_stdout_contextmanager():
    # Do not close stdout
    with xopen('-', mode='w') as f:
        print("Hello", file=f)
    # ensure stdout is not closed
    print("Still there?")


def test_read_pathlib(fname):
    path = Path(fname)
    with xopen(path, mode='rt') as f:
        assert f.read() == CONTENT


def test_read_pathlib_binary(fname):
    path = Path(fname)
    with xopen(path, mode='rb') as f:
        assert f.read() == bytes(CONTENT, 'ascii')


def test_write_pathlib(ext, tmpdir):
    path = Path(str(tmpdir)) / ('hello.txt' + ext)
    with xopen(path, mode='wt') as f:
        f.write('hello')
    with xopen(path, mode='rt') as f:
        assert f.read() == 'hello'


def test_write_pathlib_binary(ext, tmpdir):
    path = Path(str(tmpdir)) / ('hello.txt' + ext)
    with xopen(path, mode='wb') as f:
        f.write(b'hello')
    with xopen(path, mode='rb') as f:
        assert f.read() == b'hello'


@pytest.mark.skipif(sys.platform.startswith("win"),
                    reason="Windows does not have a gzip application by default.")
def test_concatenated_gzip_function():
    assert _can_read_concatenated_gz("gzip") is True
    assert _can_read_concatenated_gz("pigz") is True
    assert _can_read_concatenated_gz("xz") is False


@pytest.mark.skipif(
    not hasattr(fcntl, "F_GETPIPE_SZ") or _MAX_PIPE_SIZE is None,
    reason="Pipe size modifications not available on this platform.")
def test_pipesize_changed(tmpdir):
    path = Path(str(tmpdir), "hello.gz")
    with xopen(path, "wb") as f:
        assert isinstance(f, PipedCompressionWriter)
        assert fcntl.fcntl(f._file.fileno(),
                           fcntl.F_GETPIPE_SZ) == _MAX_PIPE_SIZE


def test_xopen_falls_back_to_gzip_open(lacking_pigz_permissions):
    with xopen(TEST_DIR / "file.txt.gz", "rb") as f:
        assert f.readline() == CONTENT_LINES[0].encode("utf-8")


def test_xopen_falls_back_to_gzip_open_no_isal(lacking_pigz_permissions,
                                               xopen_without_igzip):
    with xopen_without_igzip(TEST_DIR / "file.txt.gz", "rb") as f:
        assert f.readline() == CONTENT_LINES[0].encode("utf-8")


def test_xopen_fals_back_to_gzip_open_write_no_isal(lacking_pigz_permissions,
                                                    xopen_without_igzip,
                                                    tmp_path):
    tmp = tmp_path / "test.gz"
    with xopen_without_igzip(tmp, "wb") as f:
        f.write(b"hello")
    assert gzip.decompress(tmp.read_bytes()) == b"hello"


def test_xopen_falls_back_to_bzip2_open(lacking_pbzip2_permissions):
    with xopen(TEST_DIR / "file.txt.bz2", "rb") as f:
        assert f.readline() == CONTENT_LINES[0].encode("utf-8")


def test_open_many_writers(tmp_path, ext):
    files = []
    # Because lzma.open allocates a lot of memory,
    # open fewer files to avoid MemoryError on 32-bit architectures
    n = 21 if ext == ".xz" else 61
    for i in range(1, n):
        path = tmp_path / f"{i:03d}.txt{ext}"
        f = xopen(path, "wb", threads=2)
        f.write(b"hello")
        files.append(f)
    for f in files:
        f.close()


def test_pipedcompressionwriter_wrong_mode(tmpdir):
    with pytest.raises(ValueError) as error:
        PipedCompressionWriter(tmpdir.join("test"), ["gzip"], "xb")
    error.match("Mode is 'xb', but it must be")


def test_pipedcompressionwriter_wrong_program(tmpdir):
    with pytest.raises(OSError):
        PipedCompressionWriter(tmpdir.join("test"), ["XVXCLSKDLA"], "wb")


def test_compression_level(tmpdir, gzip_writer):
    # Currently only the gzip writers handle compression levels.
    with gzip_writer(tmpdir.join("test.gz"), "wt", 2) as test_h:
        test_h.write("test")
    assert gzip.decompress(Path(tmpdir.join("test.gz")).read_bytes()) == b"test"


def test_iter_method_writers(writer, tmpdir):
    opener, extension = writer
    test_path = tmpdir.join(f"test{extension}")
    writer = opener(test_path, "wb")
    assert iter(writer) == writer


def test_next_method_writers(writer, tmpdir):
    opener, extension = writer
    test_path = tmpdir.join(f"test.{extension}")
    writer = opener(test_path, "wb")
    with pytest.raises(io.UnsupportedOperation) as error:
        next(writer)
    error.match('not readable')


def test_pipedcompressionreader_wrong_mode():
    with pytest.raises(ValueError) as error:
        PipedCompressionReader("test", ["gzip"], "xb")
    error.match("Mode is 'xb', but it must be")


def test_piped_compression_reader_peek_binary(reader):
    opener, extension = reader
    filegz = Path(__file__).parent / f"file.txt{extension}"
    with opener(filegz, "rb") as read_h:
        # Peek returns at least the amount of characters but maybe more
        # depending on underlying stream. Hence startswith not ==.
        assert read_h.peek(1).startswith(b"T")


@pytest.mark.parametrize("mode", ["r", "rt"])
def test_piped_compression_reader_peek_text(reader, mode):
    opener, extension = reader
    compressed_file = Path(__file__).parent / f"file.txt{extension}"
    with opener(compressed_file, mode) as read_h:
        with pytest.raises(AttributeError):
            read_h.peek(1)


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
            raise NotImplementedError(f"Test should be implemented for "
                                      f"{writer}")  # pragma: no cover


@pytest.mark.parametrize(["writer", "level"], writers_and_levels())
def test_valid_compression_levels(writer, level, tmpdir):
    test_file = tmpdir.join("test.gz")
    with writer(test_file, "wb", level) as handle:
        handle.write(b"test")
    assert gzip.decompress(Path(test_file).read_bytes()) == b"test"


def test_override_output_format(tmp_path):
    test_file = tmp_path / "test_gzip_compressed"
    with xopen(test_file, mode="wb", format="gz") as f:
        f.write(b"test")
    test_contents = test_file.read_bytes()
    assert test_contents.startswith(b"\x1f\x8b")  # Gzip magic
    assert gzip.decompress(test_contents) == b"test"


def test_override_output_format_unsupported_format(tmp_path):
    test_file = tmp_path / "test_fairy_format_compressed"
    with pytest.raises(ValueError) as error:
        xopen(test_file, mode="wb", format="fairy")
    error.match("not supported")
    error.match("fairy")


def test_override_output_format_wrong_format(tmp_path):
    test_file = tmp_path / "not_compressed"
    test_file.write_text("I am not compressed.")
    with pytest.raises(OSError):  # BadGzipFile is a subclass of OSError
        with xopen(test_file, "rt", format="gz") as opened_file:
            opened_file.read()


# Test for threaded and non-threaded.
OPENERS = (xopen, functools.partial(xopen, threads=0))


@pytest.mark.parametrize(["opener", "extension"], itertools.product(OPENERS, extensions))
def test_text_encoding_newline_passtrough(opener, extension, tmp_path):
    # "Eén ree\nTwee reeën\n" latin-1 encoded with \r for as line separator.
    encoded_text = b"E\xe9n ree\rTwee ree\xebn\r"
    test_file = tmp_path / f"test.txt{extension}"
    with opener(test_file, "wb") as f:
        f.write(encoded_text)
    with opener(test_file, "rt", encoding="latin-1", newline="\r") as f:
        result = f.read()
    assert result == "Eén ree\rTwee reeën\r"


@pytest.mark.parametrize(["opener", "extension"], itertools.product(OPENERS, extensions))
def test_text_encoding_errors(opener, extension, tmp_path):
    # "Eén ree\nTwee reeën\n" latin-1 encoded. This is not valid ascii.
    encoded_text = b"E\xe9n ree\nTwee ree\xebn\n"
    test_file = tmp_path / f"test.txt{extension}"
    with opener(test_file, "wb") as f:
        f.write(encoded_text)
    with opener(test_file, "rt", encoding="ascii", errors="replace") as f:
        result = f.read()
    assert result == 'E�n ree\nTwee ree�n\n'
