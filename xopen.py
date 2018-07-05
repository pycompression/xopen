"""
Open compressed files transparently.
"""
from __future__ import print_function, division, absolute_import

import gzip
import sys
import io
import os
import time
from subprocess import Popen, PIPE

__version__ = '0.3.5'


_PY3 = sys.version > '3'

if not _PY3:
	import bz2file as bz2
else:
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


class Closing(object):
	"""
	Inherit from this class and implement a close() method to offer context
	manager functionality.
	"""
	def __enter__(self):
		return self

	def __exit__(self, *exc_info):
		self.close()

	def __del__(self):
		try:
			self.close()
		except:
			pass


class PipedGzipWriter(Closing):
	"""
	Write gzip-compressed files by running an external gzip or pigz process and
	piping into it. On Python 2, this is faster than using gzip.open(). On
	Python 3, it allows to run the compression in a separate process and can
	therefore also be faster.
	"""

	def __init__(self, path, mode='wt'):
		if mode not in ('w', 'wt', 'wb', 'a', 'at', 'ab'):
			raise ValueError("Mode is '{0}', but it must be 'w', 'wt', 'wb', 'a', 'at' or 'ab'".format(mode))
		self.outfile = open(path, mode)
		self.devnull = open(os.devnull, mode)
		self.closed = False
		self.name = path

		kwargs = dict(stdin=PIPE, stdout=self.outfile, stderr=self.devnull)
		# Setting close_fds to True in the Popen arguments is necessary due to
		# <http://bugs.python.org/issue12786>.
		# However, close_fds is not supported on Windows. See
		# <https://github.com/marcelm/cutadapt/issues/315>.
		if sys.platform != 'win32':
			kwargs['close_fds'] = True
		try:
			self.process = Popen(['pigz'], **kwargs)
			self.program = 'pigz'
		except OSError as e:
			# pigz not found, try regular gzip
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
		if _PY3 and 'b' not in mode:
			self._file = io.TextIOWrapper(self.process.stdin)
		else:
			self._file = self.process.stdin

	def write(self, arg):
		self._file.write(arg)

	def close(self):
		self.closed = True
		self._file.close()
		retcode = self.process.wait()
		self.outfile.close()
		self.devnull.close()
		if retcode != 0:
			raise IOError("Output {0} process terminated with exit code {1}".format(self.program, retcode))


class PipedGzipReader(Closing):
	def __init__(self, path, mode='r'):
		if mode not in ('r', 'rt', 'rb'):
			raise ValueError("Mode is '{0}', but it must be 'r', 'rt' or 'rb'".format(mode))
		self.process = Popen(['gzip', '-cd', path], stdout=PIPE, stderr=PIPE)
		self.name = path
		if _PY3 and not 'b' in mode:
			self._file = io.TextIOWrapper(self.process.stdout)
		else:
			self._file = self.process.stdout
		if _PY3:
			self._stderr = io.TextIOWrapper(self.process.stderr)
		else:
			self._stderr = self.process.stderr
		self.closed = False
		# Give gzip a little bit of time to report any errors (such as
		# a non-existing file)
		time.sleep(0.01)
		self._raise_if_error()

	def close(self):
		self.closed = True
		retcode = self.process.poll()
		if retcode is None:
			# still running
			self.process.terminate()
		self._raise_if_error()

	def __iter__(self):
		for line in self._file:
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
			message = self._stderr.read().strip()
			raise IOError(message)

	def read(self, *args):
		data = self._file.read(*args)
		if len(args) == 0 or args[0] <= 0:
			# wait for process to terminate until we check the exit code
			self.process.wait()
		self._raise_if_error()
		return data


if bz2 is not None:
	class ClosingBZ2File(bz2.BZ2File, Closing):
		"""
		A better BZ2File that supports the context manager protocol.
		This is relevant only for Python 2.6.
		"""


def xopen(filename, mode='r', compresslevel=6):
	"""
	Replacement for the "open" function that can also open files that have
	been compressed with gzip, bzip2 or xz. If the filename is '-', standard
	output (mode 'w') or input (mode 'r') is returned. If the filename ends
	with .gz, the file is opened with a pipe to the gzip program. If that
	does not work, then gzip.open() is used (the gzip module is slower than
	the pipe to the gzip program). If the filename ends with .bz2, it's
	opened as a bz2.BZ2File. Otherwise, the regular open() is used.

	mode can be: 'rt', 'rb', 'at', 'ab', 'wt', or 'wb'
	Instead of 'rt', 'wt' and 'at', 'r', 'w' and 'a' can be used as
	abbreviations.

	In Python 2, the 't' and 'b' characters are ignored.

	Append mode ('a', 'at', 'ab') is unavailable with BZ2 compression and
	will raise an error.

	compresslevel is the gzip compression level. It is not used for bz2 and xz.
	"""
	if mode in ('r', 'w', 'a'):
		mode += 't'
	if mode not in ('rt', 'rb', 'wt', 'wb', 'at', 'ab'):
		raise ValueError("mode '{0}' not supported".format(mode))
	if not _PY3:
		mode = mode[0]
	if not isinstance(filename, basestring):
		raise ValueError("the filename must be a string")

	# standard input and standard output handling
	if filename == '-':
		if not _PY3:
			return dict(r=sys.stdin, w=sys.stdout)[mode]
		else:
			return dict(
				r=sys.stdin,
				rt=sys.stdin,
				rb=sys.stdin.buffer,
				w=sys.stdout,
				wt=sys.stdout,
				wb=sys.stdout.buffer)[mode]

	if filename.endswith('.bz2'):
		if bz2 is None:
			raise ImportError("Cannot open bz2 files: The bz2 module is not available")
		if _PY3:
			return bz2.open(filename, mode)
		else:
			if mode[0] == 'a':
				raise ValueError("mode '{0}' not supported with BZ2 compression".format(mode))
			if sys.version_info[:2] <= (2, 6):
				return ClosingBZ2File(filename, mode)
			else:
				return bz2.BZ2File(filename, mode)
	elif filename.endswith('.xz'):
		if lzma is None:
			raise ImportError("Cannot open xz files: The lzma module is not available (use Python 3.3 or newer)")
		return lzma.open(filename, mode)
	elif filename.endswith('.gz'):
		if _PY3 and 'r' in mode:
			return gzip.open(filename, mode)
		if sys.version_info[:2] == (2, 7):
			buffered_reader = io.BufferedReader
			buffered_writer = io.BufferedWriter
		else:
			buffered_reader = lambda x: x
			buffered_writer = lambda x: x
		if 'r' in mode:
			try:
				return PipedGzipReader(filename, mode)
			except OSError:
				# gzip not installed
				return buffered_reader(gzip.open(filename, mode))
		else:
			try:
				return PipedGzipWriter(filename, mode)
			except OSError:
				return buffered_writer(gzip.open(filename, mode, compresslevel=compresslevel))
	else:
		# Python 2.6 and 2.7 have io.open, which we could use to make the returned
		# object consistent with the one returned in Python 3, but reading a file
		# with io.open() is 100 times slower (!) on Python 2.6, and still about
		# three times slower on Python 2.7 (tested with "for _ in io.open(path): pass")
		return open(filename, mode)
