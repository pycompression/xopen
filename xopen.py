"""
Open compressed files transparently.
"""
from __future__ import print_function, division, absolute_import

import gzip
import sys
import io
import os
from subprocess import Popen, PIPE

_PY3 = sys.version > '3'


try:
	import bz2
except ImportError:
	bz2 = None

try:
	import lzma
except ImportError:
	lzma = None


if _PY3:
	basestring = str
else:
	basestring = basestring


if sys.version_info < (2, 7):
	buffered_reader = lambda x: x
	buffered_writer = lambda x: x
else:
	buffered_reader = io.BufferedReader
	buffered_writer = io.BufferedWriter


class PipedGzipWriter(object):
	"""
	Write gzip-compressed files by running an external gzip process and piping
	into it. On Python 2, this is faster than using gzip.open. If pigz is
	available, that is used instead of gzip.
	"""

	def __init__(self, path, mode='w'):
		self.outfile = open(path, mode)
		self.devnull = open(os.devnull, 'w')
		self.closed = False

		# Setting close_fds to True in the Popen arguments is necessary due to
		# <http://bugs.python.org/issue12786>.
		kwargs = dict(stdin=PIPE, stdout=self.outfile, stderr=self.devnull, close_fds=True)
		try:
			self.process = Popen(['pigz'], **kwargs)
			self.program = 'pigz'
		except OSError as e:
			# binary not found, try regular gzip
			try:
				self.process = Popen(['gzip'], **kwargs)
				self.program = 'gzip'
			except (IOError, OSError) as e:
				self.outfile.close()
				self.devnull.close()
				raise
		except IOError as e:
			self.outfile.close()
			self.devnull.close()
			raise

	def write(self, arg):
		self.process.stdin.write(arg)

	def close(self):
		self.closed = True
		self.process.stdin.close()
		retcode = self.process.wait()
		self.outfile.close()
		self.devnull.close()
		if retcode != 0:
			raise IOError("Output {0} process terminated with exit code {1}".format(self.program, retcode))

	def __enter__(self):
		return self

	def __exit__(self, *exc_info):
		self.close()


class PipedGzipReader(object):
	def __init__(self, path):
		self.process = Popen(['gzip', '-cd', path], stdout=PIPE, stderr=PIPE)
		self.closed = False
		stderr = self.process.stderr.read()
		if stderr:
			# if gzip outputs *anything* on stderr, we assume something went wrong
			self.close()
			raise IOError(stderr)

	def close(self):
		self.closed = True
		retcode = self.process.poll()
		if retcode is None:
			# still running
			self.process.terminate()
		self._raise_if_error()

	def __iter__(self):
		for line in self.process.stdout:
			yield line
		self.process.wait()
		self._raise_if_error()

	def _raise_if_error(self):
		"""
		Raise IOError if process is not running anymore and the
		exit code is nonzero.
		"""
		retcode = self.process.poll()
		if retcode is not None and retcode != 0:
			raise IOError("gzip process returned non-zero exit code {0}. Is "
				"the input file truncated or corrupt?".format(retcode))

	def read(self, *args):
		data = self.process.stdout.read(*args)
		if len(args) == 0 or args[0] <= 0:
			# wait for process to terminate until we check the exit code
			self.process.wait()
		self._raise_if_error()

	def __enter__(self):
		return self

	def __exit__(self, *exc_info):
		self.close()


class Closing(object):
	def __enter__(self):
		return self

	def __exit__(self, *exc_info):
		self.close()


if bz2 is not None:
	class ClosingBZ2File(bz2.BZ2File, Closing):
		"""
		A better BZ2File that supports the context manager protocol.
		This is relevant only for Python 2.6.
		"""


def xopen(filename, mode='r'):
	"""
	Replacement for the "open" function that can also open files that have
	been compressed with gzip, bzip2 or xz. If the filename is '-', standard
	output (mode 'w') or input (mode 'r') is returned. If the filename ends
	with .gz, the file is opened with a pipe to the gzip program. If that
	does not work, then gzip.open() is used (the gzip module is slower than
	the pipe to the gzip program). If the filename ends with .bz2, it's
	opened as a bz2.BZ2File. Otherwise, the regular open() is used.

	mode can be: 'rt', 'rb', 'a', 'wt', or 'wb'
	Instead of 'rt' and 'wt', 'r' and 'w' can be used as abbreviations.

	In Python 2, the 't' and 'b' characters are ignored.

	Append mode ('a') is unavailable with BZ2 compression and will raise an error.
	"""
	if mode == 'r':
		mode = 'rt'
	elif mode == 'w':
		mode = 'wt'
	if mode not in ('rt', 'rb', 'wt', 'wb', 'a'):
		raise ValueError("mode '{0}' not supported".format(mode))
	if not _PY3:
		mode = mode[0]
	if not isinstance(filename, basestring):
		raise ValueError("the filename must be a string")

	# standard input and standard output handling
	if filename == '-':
		if not _PY3:
			return sys.stdin if 'r' in mode else sys.stdout
		return dict(
			rt=sys.stdin,
			wt=sys.stdout,
			rb=sys.stdin.buffer,
			wb=sys.stdout.buffer)[mode]

	if filename.endswith('.bz2'):
		if bz2 is None:
			raise ImportError("Cannot open bz2 files: The bz2 module is not available")
		if _PY3:
			if 't' in mode:
				return io.TextIOWrapper(bz2.BZ2File(filename, mode[0]))
			else:
				return bz2.BZ2File(filename, mode)
		elif sys.version_info[:2] <= (2, 6):
			return ClosingBZ2File(filename, mode)
		else:
			return bz2.BZ2File(filename, mode)
	elif filename.endswith('.xz'):
		if lzma is None:
			raise ImportError("Cannot open xz files: The lzma module is not available (use Python 3.3 or newer)")
		return lzma.open(filename, mode)
	elif filename.endswith('.gz'):
		if _PY3:
			if 't' in mode:
				# gzip.open in Python 3.2 does not support modes 'rt' and 'wt''
				return io.TextIOWrapper(gzip.open(filename, mode[0]))
			else:
				if 'r' in mode:
					return io.BufferedReader(gzip.open(filename, mode))
				else:
					return io.BufferedWriter(gzip.open(filename, mode))
		else:
			# rb/rt are equivalent in Py2
			if 'r' in mode:
				try:
					return PipedGzipReader(filename)
				except OSError:
					# gzip not installed
					return buffered_reader(gzip.open(filename, mode))
			else:
				try:
					return PipedGzipWriter(filename, mode)
				except OSError:
					return buffered_writer(gzip.open(filename, mode))
	else:
		return open(filename, mode)
