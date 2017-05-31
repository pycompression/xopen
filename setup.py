import sys
from setuptools import setup

if sys.version_info < (2, 6):
	sys.stdout.write("At least Python 2.6 is required.\n")
	sys.exit(1)

with open('README.rst') as f:
	long_description = f.read()

setup(
	name = 'xopen',
	version = '0.2.1',
	author = 'Marcel Martin',
	author_email = 'mail@marcelm.net',
	url = 'https://github.com/marcelm/xopen/',
	description = 'Open compressed files transparently',
	long_description = long_description,
	license = 'MIT',
	py_modules = ['xopen'],
	classifiers = [
		"Development Status :: 4 - Beta",
		"License :: OSI Approved :: MIT License",
		"Programming Language :: Python :: 2.6",
		"Programming Language :: Python :: 2.7",
		"Programming Language :: Python :: 3",
		"Programming Language :: Python :: 3.3",
		"Programming Language :: Python :: 3.4",
		"Programming Language :: Python :: 3.5",
	]
)
