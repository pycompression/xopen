# coding: utf-8
from __future__ import print_function, division, absolute_import

import os
import random
import sys
import signal
from contextlib import contextmanager
import pytest
from xopen import xopen, PipedGzipReader


base = "tests/file.txt"
files = [base + ext for ext in ['', '.gz', '.bz2']]
try:
    import lzma
    files.append(base + '.xz')
except ImportError:
    lzma = None

try:
    import bz2
except ImportError:
    bz2 = None

major, minor = sys.version_info[0:2]


@contextmanager
def temporary_path(name):
    directory = os.path.join(os.path.dirname(__file__), 'testtmp')
    if not os.path.isdir(directory):
        os.mkdir(directory)
    path = os.path.join(directory, name)
    yield path
    os.remove(path)


def test_xopen_text():
    for name in files:
        with xopen(name, 'rt') as f:
            lines = list(f)
            assert len(lines) == 2
            assert lines[1] == 'The second line.\n', name


def test_xopen_binary():
    for name in files:
        with xopen(name, 'rb') as f:
            lines = list(f)
            assert len(lines) == 2
            assert lines[1] == b'The second line.\n', name


def test_no_context_manager_text():
    for name in files:
        f = xopen(name, 'rt')
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == 'The second line.\n', name
        f.close()
        assert f.closed


def test_no_context_manager_binary():
    for name in files:
        f = xopen(name, 'rb')
        lines = list(f)
        assert len(lines) == 2
        assert lines[1] == b'The second line.\n', name
        f.close()
        assert f.closed


def test_nonexisting_file():
    with pytest.raises(IOError):
        with xopen('this-file-does-not-exist') as f:
            pass


def test_nonexisting_file_gz():
    with pytest.raises(IOError):
        with xopen('this-file-does-not-exist.gz') as f:
            pass


def test_nonexisting_file_bz2():
    with pytest.raises(IOError):
        with xopen('this-file-does-not-exist.bz2') as f:
            pass


if lzma:
    def test_nonexisting_file_xz():
        with pytest.raises(IOError):
            with xopen('this-file-does-not-exist.xz') as f:
                pass


def test_write_to_nonexisting_dir():
    with pytest.raises(IOError):
        with xopen('this/path/does/not/exist/file.txt', 'w') as f:
            pass


def test_write_to_nonexisting_dir_gz():
    with pytest.raises(IOError):
        with xopen('this/path/does/not/exist/file.gz', 'w') as f:
            pass


def test_write_to_nonexisting_dir_bz2():
    with pytest.raises(IOError):
        with xopen('this/path/does/not/exist/file.bz2', 'w') as f:
            pass


if lzma:
    def test_write_to_nonexisting_dir():
        with pytest.raises(IOError):
            with xopen('this/path/does/not/exist/file.xz', 'w') as f:
                pass


def test_append():
    cases = ["", ".gz"]
    if bz2 and sys.version_info > (3,):
        # BZ2 does NOT support append in Py 2.
        cases.append(".bz2")
    if lzma:
        cases.append(".xz")
    for ext in cases:
        # On Py3, need to send BYTES, not unicode. Let's do it for all.
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


def test_append_text():
    cases = ["", ".gz"]
    if bz2 and sys.version_info > (3,):
        # BZ2 does NOT support append in Py 2.
        cases.append(".bz2")
    if lzma:
        cases.append(".xz")
    for ext in cases:  # BZ2 does NOT support append
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
