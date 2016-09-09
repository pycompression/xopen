import sys
from setuptools import setup

if sys.version_info < (2, 6):
	sys.stdout.write("At least Python 2.6 is required.\n")
	sys.exit(1)


setup(
	name = 'xopen',
	version = '0.1.0',
	author = 'Marcel Martin',
	author_email = 'mail@marcelm.net',
	url = 'https://github.com/marcelm/xopen/',
	description = 'Open compressed files transparently',
	license = 'MIT',
	py_modules = ['xopen'],
	classifiers = [
		"Development Status :: 4 - Beta",
		"License :: OSI Approved :: MIT License",
		"Programming Language :: Cython",
		"Programming Language :: Python :: 2.6",
		"Programming Language :: Python :: 2.7",
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: 3.3",
		"Programming Language :: Python :: 3.4",
		"Programming Language :: Python :: 3.5",
	]
)
