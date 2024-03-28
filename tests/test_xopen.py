"""
Tests for the xopen.xopen function
"""
import bz2
import subprocess
import sys
import tempfile
from contextlib import contextmanager
import functools
import gzip
import io
import lzma
import os
from pathlib import Path
import shutil

import pytest

from xopen import xopen, _detect_format_from_content

try:
    import zstandard
except ImportError:
    zstandard = None


# TODO this is duplicated in test_piped.py
TEST_DIR = Path(__file__).parent
CONTENT_LINES = ["Testing, testing ...\n", "The second line.\n"]
CONTENT = "".join(CONTENT_LINES)
extensions = ["", ".gz", ".bz2", ".xz"]
if shutil.which("zstd") or zstandard:
    extensions += [".zst"]
base = os.path.join(os.path.dirname(__file__), "file.txt")
files = [base + ext for ext in extensions]


@contextmanager
def disable_binary(tmp_path, binary_name):
    """
    Find the location of the binary by its name, then set PATH to a directory that contains
    the binary with permissions set to 000. If no suitable binary could be found,
    PATH is set to an empty directory
    """
    binary_path = shutil.which(binary_name)
    if binary_path:
        shutil.copy(binary_path, tmp_path)
        os.chmod(tmp_path / Path(binary_path).name, 0)
    path = os.environ["PATH"]
    try:
        os.environ["PATH"] = str(tmp_path)
        yield
    finally:
        os.environ["PATH"] = path


@pytest.fixture(params=extensions)
def ext(request):
    return request.param


@pytest.fixture(params=files)
def fname(request):
    return request.param


@pytest.fixture
def lacking_pigz_permissions(tmp_path):
    with disable_binary(tmp_path, "pigz"):
        yield


@pytest.fixture
def lacking_pbzip2_permissions(tmp_path):
    with disable_binary(tmp_path, "pbzip2"):
        yield


@pytest.fixture
def lacking_xz_permissions(tmp_path):
    with disable_binary(tmp_path, "xz"):
        yield


@pytest.fixture
def xopen_without_igzip(monkeypatch):
    import xopen  # xopen local overrides xopen global variable

    monkeypatch.setattr(xopen, "igzip", None)
    return xopen.xopen


def test_text(fname):
    with xopen(fname, "rt") as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == "The second line.\n", fname


def test_binary(fname):
    with xopen(fname, "rb") as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == b"The second line.\n", fname


@pytest.mark.parametrize("mode", ["b", "", "t"])
@pytest.mark.parametrize("threads", [None, 0])
def test_roundtrip(ext, tmp_path, threads, mode):
    if ext == ".zst" and threads == 0 and zstandard is None:
        return
    path = tmp_path / f"file{ext}"
    data = b"Hello" if mode == "b" else "Hello"
    with xopen(path, "w" + mode, threads=threads) as f:
        f.write(data)
    with xopen(path, "r" + mode, threads=threads) as f:
        assert f.read() == data


def test_binary_no_isal_no_threads(fname, xopen_without_igzip):
    if fname.endswith(".zst") and zstandard is None:
        return
    with xopen_without_igzip(fname, "rb", threads=0) as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == b"The second line.\n", fname


def test_binary_no_isal(fname, xopen_without_igzip):
    with xopen_without_igzip(fname, "rb", threads=1) as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == b"The second line.\n", fname


def test_no_context_manager_text(fname):
    f = xopen(fname, "rt")
    lines = list(f)
    assert len(lines) == 2
    assert lines[1] == "The second line.\n", fname
    f.close()
    assert f.closed


def test_no_context_manager_binary(fname):
    f = xopen(fname, "rb")
    lines = list(f)
    assert len(lines) == 2
    assert lines[1] == b"The second line.\n", fname
    f.close()
    assert f.closed


def test_bytes_path(fname):
    path = fname.encode("utf-8")
    with xopen(path, "rt") as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == "The second line.\n", fname


def test_readinto(fname):
    content = CONTENT.encode("utf-8")
    with xopen(fname, "rb") as f:
        b = bytearray(len(content) + 100)
        length = f.readinto(b)
        assert length == len(content)
        assert b[:length] == content


def test_detect_format_from_content(ext):
    with open(Path(__file__).parent / f"file.txt{ext}", "rb") as f:
        detected = _detect_format_from_content(f)
    if ext == "":
        assert detected is None
    else:
        assert ext[1:] == detected


def test_detect_file_format_from_content(ext, tmp_path):
    path = tmp_path / f"file.txt{ext}.test"
    shutil.copy(TEST_DIR / f"file.txt{ext}", path)
    with xopen(path, "rb") as fh:
        assert fh.readline() == CONTENT_LINES[0].encode("utf-8")


def test_readline(fname):
    first_line = CONTENT_LINES[0].encode("utf-8")
    with xopen(fname, "rb") as f:
        assert f.readline() == first_line


def test_readline_text(fname):
    with xopen(fname, "r") as f:
        assert f.readline() == CONTENT_LINES[0]


def test_next(fname):
    with xopen(fname, "rt") as f:
        _ = next(f)
        line2 = next(f)
        assert line2 == "The second line.\n", fname


def test_has_iter_method(ext, tmp_path):
    path = tmp_path / f"out{ext}"
    with xopen(path, mode="w") as f:
        # Writing anything isn’t strictly necessary, but if we don’t, then
        # pbzip2 causes a delay of one second
        f.write("hello")
        assert hasattr(f, "__iter__")


def test_iter_without_with(fname):
    f = xopen(fname, "rt")
    it = iter(f)
    assert CONTENT_LINES[0] == next(it)
    f.close()


@pytest.mark.parametrize("extension", [".gz", ".bz2"])
def test_partial_iteration_closes_correctly(extension, create_large_file):
    class LineReader:
        def __init__(self, file):
            self.file = xopen(file, "rb")

        def __iter__(self):
            wrapper = io.TextIOWrapper(self.file, encoding="utf-8")
            yield from wrapper

    large_file = create_large_file(extension)
    f = LineReader(large_file)
    next(iter(f))
    f.file.close()


def test_nonexisting_file(ext):
    with pytest.raises(IOError):
        with xopen("this-file-does-not-exist" + ext):
            pass  # pragma: no cover


def test_write_to_nonexisting_dir(ext):
    with pytest.raises(IOError):
        with xopen("this/path/does/not/exist/file.txt" + ext, "w"):
            pass  # pragma: no cover


def test_invalid_mode(ext):
    with pytest.raises(ValueError):
        with xopen(TEST_DIR / f"file.txt.{ext}", mode="hallo"):
            pass  # pragma: no cover


def test_filename_invalid_type():
    with pytest.raises(TypeError):
        with xopen(123, mode="r"):
            pass  # pragma: no cover


def test_invalid_compression_level(tmp_path):
    with pytest.raises(ValueError) as e:
        with xopen(tmp_path / "out.gz", mode="w", compresslevel=17) as f:
            f.write("hello")  # pragma: no cover
    assert "compresslevel must be" in e.value.args[0]


@pytest.mark.parametrize("ext", extensions)
@pytest.mark.parametrize("threads", (0, 1))
def test_append(ext, threads, tmp_path):
    if ext == ".zst" and zstandard is None and threads == 0:
        pytest.skip("No zstandard installed")
    text = b"AB"
    reference = text + text
    path = tmp_path / f"the-file{ext}"
    with xopen(path, "ab", threads=threads) as f:
        f.write(text)
    with xopen(path, "ab", threads=threads) as f:
        f.write(text)
    with xopen(path, "r") as f:
        for appended in f:
            pass
        reference = reference.decode("utf-8")
        assert appended == reference


@pytest.mark.parametrize("ext", extensions)
def test_append_text(ext, tmp_path):
    text = "AB"
    reference = text + text
    path = tmp_path / f"the-file{ext}"
    with xopen(path, "at") as f:
        f.write(text)
    with xopen(path, "at") as f:
        f.write(text)
    with xopen(path, "rt") as f:
        for appended in f:
            pass
        assert appended == reference


@pytest.mark.timeout(5)
@pytest.mark.parametrize("extension", [".gz", ".bz2", ".xz"])
def test_truncated_file(extension, create_truncated_file):
    truncated_file = create_truncated_file(extension)
    with pytest.raises((EOFError, IOError)):
        f = xopen(truncated_file, "r")
        f.read()
        f.close()  # pragma: no cover


@pytest.mark.timeout(5)
@pytest.mark.parametrize("extension", [".gz", ".bz2", ".xz"])
def test_truncated_iter(extension, create_truncated_file):
    truncated_file = create_truncated_file(extension)
    with pytest.raises((EOFError, IOError)):
        f = xopen(truncated_file, "r")
        for line in f:
            pass
        f.close()  # pragma: no cover


@pytest.mark.timeout(5)
@pytest.mark.parametrize("extension", [".gz", ".bz2", ".xz"])
def test_truncated_with(extension, create_truncated_file):
    truncated_file = create_truncated_file(extension)
    with pytest.raises((EOFError, IOError)):
        with xopen(truncated_file, "r") as f:
            f.read()


@pytest.mark.timeout(5)
@pytest.mark.parametrize("extension", [".gz", ".bz2", ".xz"])
def test_truncated_iter_with(extension, create_truncated_file):
    truncated_file = create_truncated_file(extension)
    with pytest.raises((EOFError, IOError)):
        with xopen(truncated_file, "r") as f:
            for line in f:
                pass


def test_bare_read_from_gz():
    hello_file = TEST_DIR / "hello.gz"
    with xopen(hello_file, "rt") as f:
        assert f.read() == "hello"


def test_read_no_threads(ext):
    klasses = {
        ".bz2": bz2.BZ2File,
        ".gz": gzip.GzipFile,
        ".xz": lzma.LZMAFile,
        ".zst": io.BufferedReader,
        "": io.BufferedReader,
    }
    if ext == ".zst" and zstandard is None:
        return
    klass = klasses[ext]
    with xopen(TEST_DIR / f"file.txt{ext}", "rb", threads=0) as f:
        assert isinstance(f, klass), f


def test_write_threads(tmp_path, ext):
    path = tmp_path / f"out.{ext}"
    with xopen(path, mode="w", threads=3) as f:
        f.write("hello")
    with xopen(path) as f:
        assert f.read() == "hello"


def test_write_pigz_threads_no_isal(tmp_path, xopen_without_igzip):
    path = tmp_path / "out.gz"
    with xopen_without_igzip(path, mode="w", threads=3) as f:
        f.write("hello")
    with xopen_without_igzip(path) as f:
        assert f.read() == "hello"


def test_write_no_threads(tmp_path, ext):
    klasses = {
        ".bz2": bz2.BZ2File,
        ".gz": gzip.GzipFile,
        ".xz": lzma.LZMAFile,
        "": io.BufferedWriter,
    }
    if ext == ".zst":
        # Skip zst because if python-zstandard is not installed,
        # we fall back to an external process even when threads=0
        return
    klass = klasses[ext]
    with xopen(tmp_path / f"out{ext}", "wb", threads=0) as f:
        if isinstance(f, io.BufferedWriter):
            if ext:
                assert isinstance(f.raw, klass), f
        else:
            if ext:
                assert isinstance(f, klass)


def test_write_gzip_no_threads_no_isal(tmp_path, xopen_without_igzip):
    import gzip

    with xopen_without_igzip(tmp_path / "out.gz", "wb", threads=0) as f:
        if sys.version_info.major == 3 and sys.version_info.minor >= 12:
            assert isinstance(f, gzip.GzipFile), f
        else:
            assert isinstance(f.raw, gzip.GzipFile)


def test_write_stdout():
    f = xopen("-", mode="w")
    print("Hello", file=f)
    f.close()
    # ensure stdout is not closed
    print("Still there?")


def test_write_stdout_contextmanager():
    # Do not close stdout
    with xopen("-", mode="w") as f:
        print("Hello", file=f)
    # ensure stdout is not closed
    print("Still there?")


def test_read_pathlib(fname):
    path = Path(fname)
    with xopen(path, mode="rt") as f:
        assert f.read() == CONTENT


def test_read_pathlib_binary(fname):
    path = Path(fname)
    with xopen(path, mode="rb") as f:
        assert f.read() == bytes(CONTENT, "ascii")


def test_write_pathlib(ext, tmp_path):
    path = tmp_path / f"hello.txt{ext}"
    with xopen(path, mode="wt") as f:
        f.write("hello")
    with xopen(path, mode="rt") as f:
        assert f.read() == "hello"


def test_write_pathlib_binary(ext, tmp_path):
    path = tmp_path / f"hello.txt{ext}"
    with xopen(path, mode="wb") as f:
        f.write(b"hello")
    with xopen(path, mode="rb") as f:
        assert f.read() == b"hello"


def test_falls_back_to_gzip_open(lacking_pigz_permissions):
    with xopen(TEST_DIR / "file.txt.gz", "rb") as f:
        assert f.readline() == CONTENT_LINES[0].encode("utf-8")


def test_falls_back_to_gzip_open_no_isal(lacking_pigz_permissions, xopen_without_igzip):
    with xopen_without_igzip(TEST_DIR / "file.txt.gz", "rb") as f:
        assert f.readline() == CONTENT_LINES[0].encode("utf-8")


def test_fals_back_to_gzip_open_write_no_isal(
    lacking_pigz_permissions, xopen_without_igzip, tmp_path
):
    tmp = tmp_path / "test.gz"
    with xopen_without_igzip(tmp, "wb") as f:
        f.write(b"hello")
    assert gzip.decompress(tmp.read_bytes()) == b"hello"


def test_falls_back_to_bzip2_open(lacking_pbzip2_permissions):
    with xopen(TEST_DIR / "file.txt.bz2", "rb") as f:
        assert f.readline() == CONTENT_LINES[0].encode("utf-8")


def test_falls_back_to_lzma_open(lacking_xz_permissions):
    with xopen(TEST_DIR / "file.txt.xz", "rb") as f:
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


def test_override_output_format(tmp_path):
    path = tmp_path / "test_gzip_compressed"
    with xopen(path, mode="wb", format="gz") as f:
        f.write(b"test")
    test_contents = path.read_bytes()
    assert test_contents.startswith(b"\x1f\x8b")  # Gzip magic
    assert gzip.decompress(test_contents) == b"test"


def test_override_output_format_unsupported_format(tmp_path):
    path = tmp_path / "test_fairy_format_compressed"
    with pytest.raises(ValueError) as error:
        xopen(path, mode="wb", format="fairy")
    error.match("not supported")
    error.match("fairy")


def test_override_output_format_wrong_format(tmp_path):
    path = tmp_path / "not_compressed"
    path.write_text("I am not compressed.", encoding="utf-8")
    with pytest.raises(OSError):  # BadGzipFile is a subclass of OSError
        with xopen(path, "rt", format="gz") as opened_file:
            opened_file.read()


# Test for threaded and non-threaded.
OPENERS = (xopen, functools.partial(xopen, threads=0))


@pytest.mark.parametrize("opener", OPENERS)
@pytest.mark.parametrize("extension", extensions)
def test_text_encoding_newline_passthrough(opener, extension, tmp_path):
    if extension == ".zst" and zstandard is None:
        return
    # "Eén ree\nTwee reeën\n" latin-1 encoded with \r for as line separator.
    encoded_text = b"E\xe9n ree\rTwee ree\xebn\r"
    path = tmp_path / f"test.txt{extension}"
    with opener(path, "wb") as f:
        f.write(encoded_text)
    with opener(path, "rt", encoding="latin-1", newline="\r") as f:
        result = f.read()
    assert result == "Eén ree\rTwee reeën\r"


@pytest.mark.parametrize("opener", OPENERS)
@pytest.mark.parametrize("extension", extensions)
def test_text_encoding_errors(opener, extension, tmp_path):
    if extension == ".zst" and zstandard is None:
        return
    # "Eén ree\nTwee reeën\n" latin-1 encoded. This is not valid ascii.
    encoded_text = b"E\xe9n ree\nTwee ree\xebn\n"
    path = tmp_path / f"test.txt{extension}"
    with opener(path, "wb") as f:
        f.write(encoded_text)
    with opener(path, "rt", encoding="ascii", errors="replace") as f:
        result = f.read()
    assert result == "E�n ree\nTwee ree�n\n"


@pytest.mark.parametrize("compresslevel", [1, 6])
def test_gzip_compression_is_reproducible_without_piping(tmp_path, compresslevel):
    # compresslevel 1 should give us igzip and 6 should give us regular gzip
    path = tmp_path / "test.gz"
    with xopen(path, mode="wb", compresslevel=compresslevel, threads=0) as f:
        f.write(b"hello")
    data = path.read_bytes()
    assert (data[3] & gzip.FNAME) == 0, "gzip header contains file name"
    assert data[4:8] == b"\0\0\0\0", "gzip header contains mtime"


def test_read_devnull():
    with xopen(os.devnull):
        pass


def test_xopen_zst_fails_when_zstandard_not_available(monkeypatch):
    import xopen

    monkeypatch.setattr(xopen, "zstandard", None)
    with pytest.raises(ImportError):
        with xopen.xopen(TEST_DIR / "file.txt.zst", mode="rb", threads=0) as f:
            f.read()


@pytest.mark.parametrize("threads", (0, 1))
def test_xopen_zst_long_window_size(threads):
    if threads == 0 and zstandard is None:
        return
    elif threads == 1 and not shutil.which("zstd"):
        return
    # File created with:
    # cat /dev/zero | head -c 2147483648 > only_zeroes
    # Then compressed with
    # zstd --long=31 -19 only_zeroes
    test_zst = Path(__file__).parent / "only_zeroes.zst"
    with xopen(test_zst, "rb", threads=threads) as f:
        data = f.read(1024)
    assert data == bytes(1024)


@pytest.mark.parametrize("threads", (0, 1))
@pytest.mark.parametrize("ext", extensions)
def test_pass_file_object_for_reading(ext, threads):
    if ext == ".zst" and zstandard is None:
        return

    with open(TEST_DIR / f"file.txt{ext}", "rb") as fh:
        with xopen(fh, mode="rb", threads=threads) as f:
            assert f.readline() == CONTENT_LINES[0].encode("utf-8")


@pytest.mark.parametrize("threads", (0, 1))
@pytest.mark.parametrize("ext", extensions)
def test_pass_file_object_for_writing(tmp_path, ext, threads):
    if ext == ".zst" and zstandard is None:
        return
    first_line = CONTENT_LINES[0].encode("utf-8")
    with open(tmp_path / "out{ext}", "wb") as fh:
        with xopen(fh, "wb", threads=threads) as f:
            f.write(first_line)
    with xopen(tmp_path / "out{ext}", "rb", threads=threads) as fh:
        assert fh.readline() == first_line


@pytest.mark.parametrize("threads", (0, 1))
@pytest.mark.parametrize("ext", extensions)
def test_pass_bytesio_for_reading_and_writing(ext, threads):
    filelike = io.BytesIO()
    format = ext[1:]
    if ext == "":
        format = None
    if ext == ".zst" and zstandard is None:
        return
    first_line = CONTENT_LINES[0].encode("utf-8")
    writer = xopen(filelike, "wb", format=format, threads=threads)
    writer.write(first_line)
    if writer is not filelike:
        writer.close()
    assert not filelike.closed
    filelike.seek(0)
    with xopen(filelike, "rb", format=format, threads=threads) as fh:
        assert fh.readline() == first_line


@pytest.mark.parametrize("threads", (0, 1))
def test_xopen_stdin(monkeypatch, ext, threads):
    if ext == ".zst" and zstandard is None:
        return
    # Add encoding to suppress encoding warnings
    with open(TEST_DIR / f"file.txt{ext}", "rt", encoding="latin-1") as in_file:
        monkeypatch.setattr("sys.stdin", in_file)
        with xopen("-", "rt", threads=threads) as f:
            data = f.read()
        assert data == CONTENT


def test_xopen_stdout(monkeypatch):
    # Add encoding to suppress encoding warnings
    with tempfile.TemporaryFile(mode="w+t", encoding="latin-1") as raw:
        monkeypatch.setattr("sys.stdout", raw)
        with xopen("-", "wt") as f:
            f.write("Hello world!")
        raw.seek(0)
        data = raw.read()
    assert data == "Hello world!"


@pytest.mark.parametrize("threads", (0, 1))
def test_xopen_read_from_pipe(ext, threads):
    if ext == ".zst" and zstandard is None:
        return
    in_file = TEST_DIR / f"file.txt{ext}"
    process = subprocess.Popen(("cat", str(in_file)), stdout=subprocess.PIPE)
    with xopen(process.stdout, "rt", threads=threads) as f:
        data = f.read()
    process.wait()
    process.stdout.close()
    assert data == CONTENT


@pytest.mark.parametrize("threads", (0, 1))
def test_xopen_write_to_pipe(threads, ext):
    if ext == ".zst" and zstandard is None:
        return
    format = ext.lstrip(".")
    if format == "":
        format = None
    process = subprocess.Popen(("cat",), stdout=subprocess.PIPE, stdin=subprocess.PIPE)
    with xopen(process.stdin, "wt", threads=threads, format=format) as f:
        f.write(CONTENT)
    process.stdin.close()
    with xopen(process.stdout, "rt", threads=threads) as f:
        data = f.read()
    process.wait()
    process.stdout.close()
    assert data == CONTENT


@pytest.mark.skipif(
    not os.path.exists("/dev/stdin"), reason="/dev/stdin does not exist"
)
@pytest.mark.parametrize("threads", (0, 1))
def test_xopen_dev_stdin_read(threads, ext):
    if ext == ".zst" and zstandard is None:
        return
    file = str(Path(__file__).parent / f"file.txt{ext}")
    result = subprocess.run(
        f"cat {file} | python -c 'import xopen; "
        f'f=xopen.xopen("/dev/stdin", "rt", threads={threads});print(f.read())\'',
        shell=True,
        stdout=subprocess.PIPE,
        encoding="ascii",
    )
    assert result.stdout == CONTENT + "\n"
