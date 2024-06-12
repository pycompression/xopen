.. image:: https://github.com/pycompression/xopen/workflows/CI/badge.svg
  :target: https://github.com/pycompression/xopen
  :alt:

.. image:: https://img.shields.io/pypi/v/xopen.svg?branch=main
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

This Python module provides an ``xopen`` function that works like Python’s
built-in ``open`` function but also transparently deals with compressed files.
``xopen`` selects the most efficient method for reading or writing a compressed file.

Supported compression formats are:

- gzip (``.gz``)
- bzip2 (``.bz2``)
- xz (``.xz``)
- Zstandard (``.zst``) (optional)

``xopen`` is compatible with Python versions 3.8 and later.


Example usage
-------------

Open a file for reading::

    from xopen import xopen

    with xopen("file.txt.gz") as f:
        content = f.read()

Write to a file in binary mode,
set the compression level
and avoid using an external process::

    from xopen import xopen

    with xopen("file.txt.xz", mode="wb", threads=0, compresslevel=3) as f:
        f.write(b"Hello")


The ``xopen`` function
----------------------

The ``xopen`` module offers a single function named ``xopen`` with the following
signature::

  xopen(
    filename: str | bytes | os.PathLike,
    mode: Literal["r", "w", "a", "rt", "rb", "wt", "wb", "at", "ab"] = "r",
    compresslevel: Optional[int] = None,
    threads: Optional[int] = None,
    *,
    encoding: str = "utf-8",
    errors: Optional[str] = None,
    newline: Optional[str] = None,
    format: Optional[str] = None,
  ) -> IO

The function opens the file using a function suitable for the detected
file format and returns an open file-like object.

When writing, the file format is chosen based on the file name extension:
``.gz``, ``.bz2``, ``.xz``, ``.zst``. This can be overriden with ``format``.
If the extension is not recognized, no compression is used.

When reading and a file name extension is available, the format is detected
from the extension.
When reading and no file name extension is available,
the format is detected from the
`file signature <https://en.wikipedia.org/wiki/File_format#Magic_number>`.

Parameters
~~~~~~~~~~

**filename** (str, bytes, or `os.PathLike <https://docs.python.org/3/library/os.html#os.PathLike>`_):
Name of the file to open.

If set to ``"-"``, standard output (in mode ``"w"``) or
standard input (in mode ``"r"``) is returned.

**mode**, **encoding**, **errors**, **newline**:
These parameters have the same meaning as in Python’s built-in
`open function <https://docs.python.org/3/library/functions.html#open>`_
except that the default encoding is always UTF-8 instead of the
preferred locale encoding.
``encoding``, ``errors`` and ``newline`` are only used when opening a file in text mode.

**compresslevel**:
The compression level for writing to gzip, xz and Zstandard files.
If set to None, a default depending on the format is used:
gzip: 1, xz: 6, Zstandard: 3.

This parameter is ignored for other compression formats.

**format**:
Override the autodetection of the input or output format.
Possible values are: ``"gz"``, ``"xz"``, ``"bz2"``, ``"zst"``.

**threads**:
Set the number of additional threads spawned for compression or decompression.
May be ignored if the backend does not support threads.

If *threads* is None (the default), as many threads as available CPU cores are
used, but not more than four.

xopen tries to offload the (de)compression to other threads
to free up the main Python thread for the application.
This can either be done by using a subprocess to an external application or
using a library that supports threads.

Set threads to 0 to force xopen to use only the main Python thread.


Backends
--------

Opening of gzip files is delegated to one of these programs or libraries:

* `python-isal <https://github.com/pycompression/python-isal>`_.
  Supports multiple threads and compression levels up to 3.
* `python-zlib-ng <https://github.com/pycompression/python-zlib-ng>`_
* `pigz <https://zlib.net/pigz/>`_ (a parallel version of ``gzip``)
* `gzip <https://www.gnu.org/software/gzip/>`_

For xz files, a pipe to the ``xz`` program is used because it has
built-in support for multithreaded compression.

For bz2 files, `pbzip2 (parallel bzip2) <http://compression.ca/pbzip2/>`_ is used.

``xopen`` falls back to Python’s built-in functions
(``gzip.open``, ``lzma.open``, ``bz2.open``)
if none of the other methods can be used.


Reproducibility
---------------

xopen writes gzip files in a reproducible manner.

Normally, gzip files contain a timestamp in the file header,
which means that compressing the same data at different times results in different output files.
xopen disables this for all of the supported gzip compression backends.
For example, when using an external process, it sets the command-line option
``--no-name`` (same as ``-n``).

Note that different gzip compression backends typically do not produce
identical output, so reproducibility may no longer be given when the execution environment changes
from one ``xopen()`` invocation to the next.
This includes the CPU architecture as `igzip adjusts its algorithm
depending on it <https://github.com/intel/isa-l/issues/140#issuecomment-634877966>`_.

bzip2 and xz compression methods do not store timestamps in the file headers,
so output from them is also reproducible.


Optional Zstandard support
--------------------------

For reading and writing Zstandard (``.zst``) files, either the ``zstd`` command-line
program or the Python ``zstandard`` package needs to be installed.

* If the ``threads`` parameter to ``xopen()`` is ``None`` (the default) or any value greater than 0,
  ``xopen`` uses an external ``zstd`` process.
* If the above fails (because no ``zstd`` program is available) or if ``threads`` is 0,
  the ``zstandard`` package is used.

To ensure that you get the correct ``zstandard`` version, you can specify the ``zstd`` extra for
``xopen``, that is, install it using ``pip install xopen[zstd]``.


Changelog
---------

v2.0.2 (2024-06-12)
~~~~~~~~~~~~~~~~~~~
* #161: Fix a bug that was triggered when reading large compressed files with
  an external program.

v2.0.1 (2024-03-28)
~~~~~~~~~~~~~~~~~~~
+ #158: Fixed a bug where reading from stdin and other pipes would discard the
  first bytes from the input.
+ #156: Zstd files compressed with the ``--long=31`` files can now be opened
  without throwing errors.

v2.0.0 (2024-03-26)
~~~~~~~~~~~~~~~~~~~

* #154: Support for gzip levels has been made more consistent. Levels 0-9
  are supported. Level 11 which was only available when the ``pigz`` backend was
  present is not supported anymore. Level 0, gzip format without compression,
  lead to crashes when the ``gzip`` application backend was used as this does
  not have a ``-0`` flag. ``xopen()`` now defers to other backends in that case.
* #152: ``xopen()`` now accepts `file-like objects
  <https://docs.python.org/3/glossary.html#term-file-object>`_ for its filename
  argument.
* #146, #147, #148: Various refactors for better code size and readability:

    * PipedCompressionReader/Writer are now combined _PipedCompressionProgram
      class.
    * _PipedCompressionProgram is binary-only. For text reading and writing
      it is wrapped in an ``io.TextIOWrapper`` in the ``xopen()`` function.
    * Classes that derive from PipedCompressionReader/Writer have been removed.
* #148: xopen's classes, variables and functions pertaining to piped reading
  and writing are all made private by prefixing them with an underscore.
  These are not part of the API and may change between releases.

v1.9.0 (2024-01-31)
~~~~~~~~~~~~~~~~~~~
* #142: The python-isal compression backend is now only used for compression
  levels 1 and 2. Contrary to other backends, python-isal level 0 gave
  compressed rather than uncompressed data in gzip format. Level 3 on
  python-isal did not provide better compression than level 2.
* #140: PipedCompressionReader/Writer now derive from the `io.IOBase
  <https://docs.python.org/3/library/io.html#io.IOBase>`_ abstract class.
* #138: The gzip default compression level is now 1 when no value is provided
  by the calling function. The default used to be determined by the backend.
* #135: xopen now uses zlib-ng when available and applicable.
* #133: Piped ``igzip`` is no longer used as a (de)compression backend as
  python-isal's threaded mode is a better choice in all use cases.

v1.8.0 (2023-11-03)
~~~~~~~~~~~~~~~~~~~
* #131: xopen now defers to the ``isal.igzip_threaded`` module rather than
  piping to external programs in applicable cases. This makes reading and
  writing to gzip files using threads more efficient.
* Support for Python 3.7 is dropped and support for Python 3.12 is added.

v1.7.0 (2022-11-03)
~~~~~~~~~~~~~~~~~~~

* #91: Added optional support for Zstandard (``.zst``) files.
  This requires that the Python ``zstandard`` package is installed
  or that the ``zstd`` command-line program is available.

v1.6.0 (2022-08-10)
~~~~~~~~~~~~~~~~~~~

* #94: When writing gzip files, the timestamp and name of the original
  file is omitted (equivalent to using ``gzip --no-name`` (or ``-n``) on the
  command line). This allows files to be written in a reproducible manner.

v1.5.0 (2022-03-23)
~~~~~~~~~~~~~~~~~~~

* #100: Dropped Python 3.6 support
* #101: Added support for piping into and from an external ``xz`` process. Contributed by @fanninpm.
* #102: Support setting the xz compression level. Contributed by @tsibley.

v1.4.0 (2022-01-14)
~~~~~~~~~~~~~~~~~~~

* Add ``seek()`` and ``tell()`` to the ``PipedCompressionReader`` classes
  (for Windows compatibility)

v1.3.0 (2022-01-10)
~~~~~~~~~~~~~~~~~~~

* xopen is now available on Windows (in addition to Linux and macOS).
* For greater compatibility with `the built-in open()
  function <https://docs.python.org/3/library/functions.html#open>`_,
  ``xopen()`` has gained the parameters *encoding*, *errors* and *newlines*
  with the same meaning as in ``open()``. Unlike built-in ``open()``, though,
  encoding is UTF-8 by default.
* A parameter *format* has been added that allows to force the compression
  file format.

v1.2.0 (2021-09-21)
~~~~~~~~~~~~~~~~~~~

* `pbzip2 <http://compression.ca/pbzip2/>`_ is now used to open ``.bz2`` files if
  ``threads`` is greater than zero (contributed by @DriesSchaumont).

v1.1.0 (2021-01-20)
~~~~~~~~~~~~~~~~~~~

* Python 3.5 support is dropped.
* On Linux systems, `python-isal <https://github.com/pycompression/python-isal>`_
  is now added as a requirement. This will speed up the reading of gzip files
  significantly when no external processes are used.

v1.0.0 (2020-11-05)
~~~~~~~~~~~~~~~~~~~

* If installed, the ``igzip`` program (part of
  `Intel ISA-L <https://github.com/intel/isa-l/>`_) is now used for reading
  and writing gzip-compressed files at compression levels 1-3, which results
  in a significant speedup.

v0.9.0 (2020-04-02)
~~~~~~~~~~~~~~~~~~~

* #80: When the file name extension of a file to be opened for reading is not
  available, the content is inspected (if possible) and used to determine
  which compression format applies (contributed by @bvaisvil).
* This release drops Python 2.7 and 3.4 support. Python 3.5 or later is
  now required.

v0.8.4 (2019-10-24)
~~~~~~~~~~~~~~~~~~~

* When reading gzipped files, force ``pigz`` to use only a single process.
  ``pigz`` cannot use multiple cores anyway when decompressing. By default,
  it would use extra I/O processes, which slightly reduces wall-clock time,
  but increases CPU time. Single-core decompression with ``pigz`` is still
  about twice as fast as regular ``gzip``.
* Allow ``threads=0`` for specifying that no external ``pigz``/``gzip``
  process should be used (then regular ``gzip.open()`` is used instead).

v0.8.3 (2019-10-18)
~~~~~~~~~~~~~~~~~~~

* #20: When reading gzipped files, let ``pigz`` use at most four threads by default.
  This limit previously only applied when writing to a file. Contributed by @bernt-matthias.
* Support Python 3.8

v0.8.0 (2019-08-14)
~~~~~~~~~~~~~~~~~~~

* #14: Speed improvements when iterating over gzipped files.

v0.6.0 (2019-05-23)
~~~~~~~~~~~~~~~~~~~

* For reading from gzipped files, xopen will now use a ``pigz`` subprocess.
  This is faster than using ``gzip.open``.
* Python 2 support will be dropped in one of the next releases.

v0.5.0 (2019-01-30)
~~~~~~~~~~~~~~~~~~~

* By default, pigz is now only allowed to use at most four threads. This hopefully reduces
  problems some users had with too many threads when opening many files at the same time.
* xopen now accepts pathlib.Path objects.

v0.4.0 (2019-01-07)
~~~~~~~~~~~~~~~~~~~

* Drop Python 3.3 support
* Add a ``threads`` parameter (passed on to ``pigz``)

v0.3.2 (2017-11-22)
~~~~~~~~~~~~~~~~~~~

* #6: Make multi-block bz2 work on Python 2 by using external bz2file library.

v0.3.1 (2017-11-22)
~~~~~~~~~~~~~~~~~~~

* Drop Python 2.6 support
* #5: Fix PipedGzipReader.read() not returning anything

v0.3.0 (2017-11-15)
~~~~~~~~~~~~~~~~~~~

* Add gzip compression parameter

v0.2.1 (2017-05-31)
~~~~~~~~~~~~~~~~~~~

* #3: Allow appending to bz2 and lzma files where possible

v0.1.1 (2016-12-02)
~~~~~~~~~~~~~~~~~~~

* Fix a deadlock

v0.1.0 (2016-09-09)
~~~~~~~~~~~~~~~~~~~

* Initial release

Credits
-------

The name ``xopen`` was taken from the C function of the same name in the
`utils.h file that is part of
BWA <https://github.com/lh3/bwa/blob/83662032a2192d5712996f36069ab02db82acf67/utils.h>`_.

Some ideas were taken from the `canopener project <https://github.com/selassid/canopener>`_.
If you also want to open S3 files, you may want to use that module instead.

@kyleabeauchamp contributed support for appending to files before this repository was created.


Maintainers
-----------

* Marcel Martin
* Ruben Vorderman
* See also the `full list of contributors <https://github.com/pycompression/xopen/graphs/contributors>`_.


Links
-----

* `Source code <https://github.com/pycompression/xopen/>`_
* `Report an issue <https://github.com/pycompression/xopen/issues>`_
* `Project page on PyPI (Python package index) <https://pypi.python.org/pypi/xopen/>`_
