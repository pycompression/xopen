.. image:: https://github.com/pycompression/xopen/workflows/CI/badge.svg
  :target: https://github.com/pycompression/xopen
  :alt: 
  
.. image:: https://img.shields.io/pypi/v/xopen.svg?branch=master
  :target: https://pypi.python.org/pypi/xopen

.. image:: https://img.shields.io/conda/v/conda-forge/xopen.svg
  :target: https://anaconda.org/conda-forge/xopen
  :alt:

.. image:: https://codecov.io/gh/pycompression/xopen/branch/main/graph/badge.svg
  :target: https://codecov.io/gh/pycompression/xopen
  :alt:

=====
xopen
=====

This small Python module provides an ``xopen`` function that works like the
built-in ``open`` function, but can also deal with compressed files.
Supported compression formats are gzip, bzip2 and xz. They are automatically
recognized by their file extensions `.gz`, `.bz2` or `.xz`.

The focus is on being as efficient as possible on all supported Python versions.
For example, ``xopen`` uses ``pigz``, which is a parallel version of ``gzip``,
to open ``.gz`` files, which is faster than using the built-in ``gzip.open``
function. ``pigz`` can use multiple threads when compressing, but is also faster
when reading ``.gz`` files, so it is used both for reading and writing if it is
available. For gzip compression levels 1 to 3,
`igzip <https://github.com/intel/isa-l/>`_ is used for an even greater speedup.

For use cases where using only the main thread is desired xopen can be used
with ``threads=0``. This will use `python-isal
<https://github.com/pycompression/python-isal>`_ (which binds isa-l) if
python-isal is installed (automatic on Linux systems, as it is a requirement).
For installation instructions for python-isal please
checkout the `python-isal homepage
<https://github.com/pycompression/python-isal>`_. If python-isal is not
available ``gzip.open`` is used.

This module has originally been developed as part of the `Cutadapt
tool <https://cutadapt.readthedocs.io/>`_ that is used in bioinformatics to
manipulate sequencing data. It has been in successful use within that software
for a few years.

``xopen`` is compatible with Python versions 3.6 and later.


Usage
-----

Open a file for reading::

    from xopen import xopen

    with xopen('file.txt.xz') as f:
        content = f.read()

Or without context manager::

    from xopen import xopen

    f = xopen('file.txt.xz')
    content = f.read()
    f.close()

Open a file in binary mode for writing::

    from xopen import xopen

    with xopen('file.txt.gz', mode='wb') as f:
        f.write(b'Hello')


Credits
-------

The name ``xopen`` was taken from the C function of the same name in the
`utils.h file which is part of
BWA <https://github.com/lh3/bwa/blob/83662032a2192d5712996f36069ab02db82acf67/utils.h>`_.

Kyle Beauchamp <https://github.com/kyleabeauchamp/> has contributed support for
appending to files.

Ruben Vorderman <https://github.com/rhpvorderman/> contributed improvements to
make reading and writing gzipped files faster.

Benjamin Vaisvil <https://github.com/bvaisvil> contributed support for
format detection from content.

Dries Schaumont <https://github.com/DriesSchaumont> contributed support for
faster bz2 reading and writing using pbzip2.

Some ideas were taken from the `canopener project <https://github.com/selassid/canopener>`_.
If you also want to open S3 files, you may want to use that module instead.


Changes
-------

v1.2.0
~~~~~~

* `pbzip2 <http://compression.ca/pbzip2/>`_ is now used to open ``.bz2`` files if
  ``threads`` is greater than zero.

v1.1.0
~~~~~~
* Python 3.5 support is dropped.
* On Linux systems, `python-isal <https://github.com/pycompression/python-isal>`_
  is now added as a requirement. This will speed up the reading of gzip files
  significantly when no external processes are used.

v1.0.0
~~~~~~
* If installed, the ``igzip`` program (part of
  `Intel ISA-L <https://github.com/intel/isa-l/>`_) is now used for reading
  and writing gzip-compressed files at compression levels 1-3, which results
  in a significant speedup.

v0.9.0
~~~~~~
* When the file name extension of a file to be opened for reading is not
  available, the content is inspected (if possible) and used to determine
  which compression format applies.
* This release drops Python 2.7 and 3.4 support. Python 3.5 or later is
  now required.

v0.8.4
~~~~~~
* When reading gzipped files, force ``pigz`` to use only a single process.
  ``pigz`` cannot use multiple cores anyway when decompressing. By default,
  it would use extra I/O processes, which slightly reduces wall-clock time,
  but increases CPU time. Single-core decompression with ``pigz`` is still
  about twice as fast as regular ``gzip``.
* Allow ``threads=0`` for specifying that no external ``pigz``/``gzip``
  process should be used (then regular ``gzip.open()`` is used instead).

v0.8.3
~~~~~~
* When reading gzipped files, let ``pigz`` use at most four threads by default.
  This limit previously only applied when writing to a file.
* Support Python 3.8

v0.8.0
~~~~~~
* Speed improvements when iterating over gzipped files.

v0.6.0
~~~~~~
* For reading from gzipped files, xopen will now use a ``pigz`` subprocess.
  This is faster than using ``gzip.open``.
* Python 2 support will be dropped in one of the next releases.

v0.5.0
~~~~~~
* By default, pigz is now only allowed to use at most four threads. This hopefully reduces
  problems some users had with too many threads when opening many files at the same time.
* xopen now accepts pathlib.Path objects.


Contributors
------------

* Marcel Martin
* Ruben Vorderman
* For more contributors, see <https://github.com/pycompression/xopen/graphs/contributors>


Links
-----

* `Source code <https://github.com/pycompression/xopen/>`_
* `Report an issue <https://github.com/pycompression/xopen/issues>`_
* `Project page on PyPI (Python package index) <https://pypi.python.org/pypi/xopen/>`_
