# coding: utf-8
from __future__ import print_function, division, absolute_import

import os
import random
import sys
import signal
from contextlib import contextmanager
import pytest
from xopen import xopen, PipedGzipReader


extensions = ["", ".gz", ".bz2"]

try:
    import lzma
    extensions.append(".xz")
except ImportError:
    lzma = None

base = "tests/file.txt"
files = [base + ext for ext in extensions]
CONTENT = 'Testing, testing ...\nThe second line.\n'

# File extensions for which appending is supported
append_extensions = extensions[:]
if sys.version_info[0] == 2:
    append_extensions.remove(".bz2")


@contextmanager
def temporary_path(name):
    directory = os.path.join(os.path.dirname(__file__), 'testtmp')
    if not os.path.isdir(directory):
        os.mkdir(directory)
    path = os.path.join(directory, name)
    yield path
    os.remove(path)


@pytest.mark.parametrize("name", files)
def test_xopen_text(name):
    with xopen(name, 'rt') as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == 'The second line.\n', name


@pytest.mark.parametrize("name", files)
def test_xopen_binary(name):
    with xopen(name, 'rb') as f:
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == b'The second line.\n', name


@pytest.mark.parametrize("name", files)
def test_no_context_manager_text(name):
    f = xopen(name, 'rt')
    lines = list(f)
    assert len(lines) == 2
    assert lines[1] == 'The second line.\n', name
    f.close()
    assert f.closed


@pytest.mark.parametrize("name", files)
def test_no_context_manager_binary(name):
    f = xopen(name, 'rb')
    lines = list(f)
    assert len(lines) == 2
    assert lines[1] == b'The second line.\n', name
    f.close()
    assert f.closed


@pytest.mark.parametrize("ext", extensions)
def test_nonexisting_file(ext):
    with pytest.raises(IOError):
        with xopen('this-file-does-not-exist' + ext) as f:
            pass


@pytest.mark.parametrize("ext", extensions)
def test_write_to_nonexisting_dir(ext):
    with pytest.raises(IOError):
        with xopen('this/path/does/not/exist/file.txt' + ext, 'w') as f:
            pass


@pytest.mark.parametrize("ext", append_extensions)
def test_append(ext):
    text = "AB".encode("utf-8")
    reference = text + text
    with temporary_path('truncated.fastq' + ext) as path:
        try:
            os.unlink(path)
        except OSError:
            pass
        with xopen(path, 'ab') as f:
            f.write(text)
        with xopen(path, 'ab') as f:
            f.write(text)
        with xopen(path, 'r') as f:
            for appended in f:
                pass
            try:
                reference = reference.decode("utf-8")
            except AttributeError:
                pass
            assert appended == reference


@pytest.mark.parametrize("ext", append_extensions)
def test_append_text(ext):
    text = "AB"
    reference = text + text
    with temporary_path('truncated.fastq' + ext) as path:
        try:
            os.unlink(path)
        except OSError:
            pass
        with xopen(path, 'at') as f:
            f.write(text)
        with xopen(path, 'at') as f:
            f.write(text)
        with xopen(path, 'rt') as f:
            for appended in f:
                pass
            assert appended == reference


def create_truncated_file(path):
    # Random text
    random_text = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(1024))
    # Make the text a lot bigger in order to ensure that it is larger than the
    # pipe buffer size.
    random_text *= 1024  # 1MB
    with xopen(path, 'w') as f:
        f.write(random_text)
    with open(path, 'a') as f:
        f.truncate(os.stat(path).st_size - 10)


class TookTooLongError(Exception):
    pass


class timeout:
    # copied from https://stackoverflow.com/a/22348885/715090
    def __init__(self, seconds=1):
        self.seconds = seconds

    def handle_timeout(self, signum, frame):
        raise TookTooLongError()

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


if sys.version_info[:2] != (3, 3):
    def test_truncated_gz():
        with temporary_path('truncated.gz') as path:
            create_truncated_file(path)
            with timeout(seconds=2):
                with pytest.raises((EOFError, IOError)):
                    f = xopen(path, 'r')
                    f.read()
                    f.close()


    def test_truncated_gz_iter():
        with temporary_path('truncated.gz') as path:
            create_truncated_file(path)
            with timeout(seconds=2):
                with pytest.raises((EOFError, IOError)):
                    f = xopen(path, 'r')
                    for line in f:
                        pass
                    f.close()


def test_bare_read_from_gz():
    with xopen('tests/hello.gz', 'rt') as f:
        assert f.read() == 'hello'


def test_read_piped_gzip():
    with PipedGzipReader('tests/hello.gz', 'rt') as f:
        assert f.read() == 'hello'


def test_write_pigz_threads(tmpdir):
    path = str(tmpdir.join('out.gz'))
    with xopen(path, mode='w', threads=3) as f:
        f.write('hello')
    with xopen(path) as f:
        assert f.read() == 'hello'


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


if sys.version_info[:2] >= (3, 4):
    # pathlib was added in Python 3.4
    from pathlib import Path

    @pytest.mark.parametrize("file", files)
    def test_read_pathlib(file):
        path = Path(file)
        with xopen(path, mode='rt') as f:
            assert f.read() == CONTENT

    @pytest.mark.parametrize("file", files)
    def test_read_pathlib_binary(file):
        path = Path(file)
        with xopen(path, mode='rb') as f:
            assert f.read() == bytes(CONTENT, 'ascii')

    @pytest.mark.parametrize("ext", extensions)
    def test_write_pathlib(ext, tmpdir):
        path = Path(str(tmpdir)) / ('hello.txt' + ext)
        with xopen(path, mode='wt') as f:
            f.write('hello')
        with xopen(path, mode='rt') as f:
            assert f.read() == 'hello'

    @pytest.mark.parametrize("ext", extensions)
    def test_write_pathlib_binary(ext, tmpdir):
        path = Path(str(tmpdir)) / ('hello.txt' + ext)
        with xopen(path, mode='wb') as f:
            f.write(b'hello')
        with xopen(path, mode='rb') as f:
            assert f.read() == b'hello'
