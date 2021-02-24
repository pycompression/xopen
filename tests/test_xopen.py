import gzip
import io
import os
import random
import shutil
import signal
import sys
import time
import pytest
from pathlib import Path

from xopen import (
    xopen,
    PipedCompressionReader,
    PipedCompressionWriter,
    PipedGzipReader,
    PipedGzipWriter,
    PipedPigzReader,
    PipedPigzWriter,
    PipedIGzipReader,
    PipedIGzipWriter,
    PipedPythonIsalReader,
    PipedPythonIsalWriter,
    _MAX_PIPE_SIZE,
    _can_read_concatenated_gz,
    igzip
)
extensions = ["", ".gz", ".bz2"]

try:
    import lzma
    extensions.append(".xz")
except ImportError:
    lzma = None

try:
    import fcntl
    if not hasattr(fcntl, "F_GETPIPE_SZ") and sys.platform == "linux":
        setattr(fcntl, "F_GETPIPE_SZ", 1032)
except ImportError:
    fcntl = None

base = "tests/file.txt"
files = [base + ext for ext in extensions]
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


@pytest.fixture(params=PIPED_GZIP_READERS)
def gzip_reader(request):
    return request.param


@pytest.fixture(params=PIPED_GZIP_WRITERS)
def gzip_writer(request):
    return request.param


@pytest.fixture(params=extensions)
def ext(request):
    return request.param


@pytest.fixture(params=files)
def fname(request):
    return request.param


@pytest.fixture
def lacking_pigz_permissions(tmp_path):
    """
    Set PATH to a directory that contains a pigz binary with permissions set to 000.
    If no suitable pigz binary could be found, PATH is set to an empty directory
    """
    pigz_path = shutil.which("pigz")
    if pigz_path:
        shutil.copy(pigz_path, str(tmp_path))
        os.chmod(str(tmp_path / "pigz"), 0)

    path = os.environ["PATH"]
    os.environ["PATH"] = str(tmp_path)
    yield
    os.environ["PATH"] = path


@pytest.fixture
def large_gzip(tmpdir):
    path = str(tmpdir.join("large.gz"))
    random_text = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ\n') for _ in range(1024))
    # Make the text a lot bigger in order to ensure that it is larger than the
    # pipe buffer size.
    random_text *= 1024
    with xopen(path, 'w') as f:
        f.write(random_text)
    return path


@pytest.fixture
def truncated_gzip(large_gzip):
    with open(large_gzip, 'a') as f:
        f.truncate(os.stat(large_gzip).st_size - 10)
    return large_gzip


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


def test_readinto(fname):
    content = CONTENT.encode('utf-8')
    with xopen(fname, 'rb') as f:
        b = bytearray(len(content) + 100)
        length = f.readinto(b)
        assert length == len(content)
        assert b[:length] == content


def test_gzip_reader_readinto(gzip_reader):
    content = CONTENT.encode('utf-8')
    with gzip_reader("tests/file.txt.gz", "rb") as f:
        b = bytearray(len(content) + 100)
        length = f.readinto(b)
        assert length == len(content)
        assert b[:length] == content


def test_gzip_reader_textiowrapper(gzip_reader):
    with gzip_reader("tests/file.txt.gz", "rb") as f:
        wrapped = io.TextIOWrapper(f)
        assert wrapped.read() == CONTENT


def test_detect_gzip_file_format_from_content():
    with xopen("tests/file.txt.gz.test", "rb") as fh:
        assert fh.readline() == CONTENT_LINES[0].encode("utf-8")


def test_detect_bz2_file_format_from_content():
    with xopen("tests/file.txt.bz2.test", "rb") as fh:
        assert fh.readline() == CONTENT_LINES[0].encode("utf-8")


def test_readline(fname):
    first_line = CONTENT_LINES[0].encode('utf-8')
    with xopen(fname, 'rb') as f:
        assert f.readline() == first_line


def test_readline_text(fname):
    with xopen(fname, 'r') as f:
        assert f.readline() == CONTENT_LINES[0]


def test_gzip_reader_readline(gzip_reader):
    first_line = CONTENT_LINES[0].encode('utf-8')
    with gzip_reader("tests/file.txt.gz", "rb") as f:
        assert f.readline() == first_line


def test_gzip_reader_readline_text(gzip_reader):
    with gzip_reader("tests/file.txt.gz", "r") as f:
        assert f.readline() == CONTENT_LINES[0]


@pytest.mark.parametrize("threads", [None, 1, 2])
def test_pipedpigzpreader_iter(threads):
    with PipedPigzReader("tests/file.txt.gz", mode="r", threads=threads) as f:
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


def test_gzip_writer_has_iter_method(tmpdir, gzip_writer):
    with gzip_writer(str(tmpdir.join("out.gz"))) as f:
        assert hasattr(f, '__iter__')


def test_iter_without_with(fname):
    f = xopen(fname, "rt")
    it = iter(f)
    assert CONTENT_LINES[0] == next(it)
    f.close()


def test_gzip_reader_iter_without_with(gzip_reader):
    it = iter(gzip_reader("tests/file.txt.gz"))
    assert CONTENT_LINES[0] == next(it)


@pytest.mark.parametrize("mode", ["rb", "rt"])
def test_gzipreader_close(large_gzip, mode, gzip_reader):
    with gzip_reader(large_gzip, mode=mode) as f:
        f.readline()
        time.sleep(0.2)
    # The subprocess should be properly terminated now


def test_partial_gzip_iteration_closes_correctly(large_gzip):
    class LineReader:
        def __init__(self, file):
            self.file = xopen(file, "rb")

        def __iter__(self):
            wrapper = io.TextIOWrapper(self.file)
            yield from wrapper

    f = LineReader(large_gzip)
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


def test_invalid_mode():
    with pytest.raises(ValueError):
        with xopen("tests/file.txt.gz", mode="hallo"):
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
    assert "between 1 and 9" in e.value.args[0]


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


class timeout:
    # copied from https://stackoverflow.com/a/22348885/715090
    def __init__(self, seconds=1):
        self.seconds = seconds

    def handle_timeout(self, signum, frame):
        raise TookTooLongError()  # pragma: no cover

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


def test_truncated_gz(truncated_gzip):
    with timeout(seconds=2):
        with pytest.raises((EOFError, IOError)):
            f = xopen(truncated_gzip, "r")
            f.read()
            f.close()  # pragma: no cover


def test_truncated_gz_iter(truncated_gzip):
    with timeout(seconds=2):
        with pytest.raises((EOFError, IOError)):
            f = xopen(truncated_gzip, 'r')
            for line in f:
                pass
            f.close()  # pragma: no cover


def test_truncated_gz_with(truncated_gzip):
    with timeout(seconds=2):
        with pytest.raises((EOFError, IOError)):
            with xopen(truncated_gzip, 'r') as f:
                f.read()


def test_truncated_gz_iter_with(truncated_gzip):
    with timeout(seconds=2):
        with pytest.raises((EOFError, IOError)):
            with xopen(truncated_gzip, 'r') as f:
                for line in f:
                    pass


def test_bare_read_from_gz():
    with xopen('tests/hello.gz', 'rt') as f:
        assert f.read() == 'hello'


def test_gzip_readers_read(gzip_reader):
    with gzip_reader('tests/hello.gz', 'rt') as f:
        assert f.read() == 'hello'


def test_write_pigz_threads(tmpdir):
    path = str(tmpdir.join('out.gz'))
    with xopen(path, mode='w', threads=3) as f:
        f.write('hello')
    with xopen(path) as f:
        assert f.read() == 'hello'


def test_read_gzip_no_threads():
    import gzip
    with xopen("tests/hello.gz", "rb", threads=0) as f:
        assert isinstance(f, gzip.GzipFile), f


def test_write_gzip_no_threads(tmpdir):
    import gzip
    path = str(tmpdir.join("out.gz"))
    with xopen(path, "wb", threads=0) as f:
        assert isinstance(f, gzip.GzipFile), f


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


# lzma doesn’t work on PyPy3 at the moment
if lzma is not None:
    def test_detect_xz_file_format_from_content():
        with xopen("tests/file.txt.xz.test", "rb") as fh:
            assert fh.readline() == CONTENT_LINES[0].encode("utf-8")


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
    with xopen("tests/file.txt.gz", "rb") as f:
        assert f.readline() == CONTENT_LINES[0].encode("utf-8")


def test_open_many_gzip_writers(tmp_path):
    files = []
    for i in range(1, 61):
        path = tmp_path / "{:03d}.txt.gz".format(i)
        f = xopen(path, "wb", threads=2)
        f.write(b"hello")
        files.append(f)
    for f in files:
        f.close()


def test_pipedcompressionwriter_wrong_mode():
    with pytest.raises(ValueError) as error:
        PipedCompressionWriter("test", ["gzip"], "xb")
    error.match("Mode is 'xb', but it must be")


def test_pipedcompressionwriter_wrong_program():
    with pytest.raises(OSError):
        PipedCompressionWriter("test", ["XVXCLSKDLA"], "wb")


def test_compression_level(tmpdir, gzip_writer):
    with gzip_writer(tmpdir.join("test.gz"), "wt", 2) as test_h:
        test_h.write("test")
    assert gzip.decompress(Path(tmpdir.join("test.gz")).read_bytes()) == b"test"


def test_iter_method_writers(gzip_writer, tmpdir):
    test_path = tmpdir.join("test.gz")
    writer = gzip_writer(test_path, "wb")
    assert iter(writer) == writer


def test_next_method_writers(gzip_writer, tmpdir):
    test_path = tmpdir.join("test.gz")
    writer = gzip_writer(test_path, "wb")
    with pytest.raises(io.UnsupportedOperation) as error:
        next(writer)
    error.match('not readable')


def test_pipedcompressionreader_wrong_mode():
    with pytest.raises(ValueError) as error:
        PipedCompressionReader("test", ["gzip"], "xb")
    error.match("Mode is 'xb', but it must be")


def test_piped_compression_reader_peek_binary(gzip_reader):
    filegz = Path(__file__).parent / "file.txt.gz"
    with gzip_reader(filegz, "rb") as read_h:
        # Peek returns at least the amount of characters but maybe more
        # depending on underlying stream. Hence startswith not ==.
        assert read_h.peek(1).startswith(b"T")


@pytest.mark.parametrize("mode", ["r", "rt"])
def test_piped_compression_reader_peek_text(gzip_reader, mode):
    filegz = Path(__file__).parent / "file.txt.gz"
    with gzip_reader(filegz, mode) as read_h:
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
                                      f"{writer}")


@pytest.mark.parametrize(["writer", "level"], writers_and_levels())
def test_valid_compression_levels(writer, level, tmpdir):
    test_file = tmpdir.join("test.gz")
    with writer(test_file, "wb", level) as handle:
        handle.write(b"test")
    assert gzip.decompress(Path(test_file).read_bytes()) == b"test"
