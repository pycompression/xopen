# coding: utf-8
from __future__ import print_function, division, absolute_import
import gzip
import os
import random
import sys
from contextlib import contextmanager
from nose.tools import raises
from xopen import xopen


base = "tests/file.txt"
files = [ base + ext for ext in ['', '.gz', '.bz2' ] ]
try:
	import lzma
	files.append(base + '.xz')
except ImportError:
	pass


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


def test_append():
	for ext in ["", ".gz"]:  # BZ2 does NOT support append
		text = "AB"
		if ext != "":
			text = text.encode("utf-8")  # On Py3, need to send BYTES, not unicode
		reference = text + text
		with temporary_path('truncated.fastq' + ext) as path:
			try:
				os.unlink(path)
			except OSError:
				pass
			with xopen(path, 'a') as f:
				f.write(text)
			with xopen(path, 'a') as f:
				f.write(text)
			with xopen(path, 'r') as f:
				for appended in f:
					pass
				try:
					reference = reference.decode("utf-8")
				except AttributeError:
					pass
				assert appended == reference


def create_truncated_file(path):
	# Random text
	random_text = ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ') for _ in range(200))
	with xopen(path, 'w') as f:
		f.write(random_text)
	with open(path, 'a') as f:
		f.truncate(os.stat(path).st_size - 10)


# Disable these tests in Python 3.2 and 3.3
if not ((3, 2) <= sys.version_info[:2] <= (3, 3)):
	@raises(EOFError)
	def test_truncated_gz():
		with temporary_path('truncated.gz') as path:
			create_truncated_file(path)
			f = xopen(path, 'r')
			f.read()
			f.close()


	@raises(EOFError)
	def test_truncated_gz_iter():
		with temporary_path('truncated.gz') as path:
			create_truncated_file(path)
			f = xopen(path, 'r')
			for line in f:
				pass
			f.close()
