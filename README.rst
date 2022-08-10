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

This Python module provides an ``xopen`` function that works like the
built-in ``open`` function but also transparently deals with compressed files.
Supported compression formats are currently gzip, bzip2 and xz.

``xopen`` selects the most efficient method for reading or writing a compressed file.
This often means opening a pipe to an external tool, such as
`pigz <https://zlib.net/pigz/>`_, which is a parallel version of ``gzip``,
or `igzip <https://github.com/intel/isa-l/>`_, which is a highly optimized
version of ``gzip``.

If ``threads=0`` is passed to ``xopen()``, no external process is used.
For gzip files, this will then use `python-isal
<https://github.com/pycompression/python-isal>`_ (which binds isa-l) if
it is installed (since ``python-isal`` is a dependency of ``xopen``,
this should always be the case).
Neither ``igzip`` nor ``python-isal`` support compression levels
greater 3, so if no external tool is available or ``threads`` has been set to 0,
Python’s built-in ``gzip.open`` is used.

For xz files, a pipe to the ``xz`` program is used because it has built-in support for multithreaded compression.

For bz2 files, `pbzip2 (parallel bzip2) <http://compression.ca/pbzip2/>`_ is used.

``xopen`` falls back to Python’s built-in functions
(``gzip.open``, ``lzma.open``, ``bz2.open``)
if none of the other methods can be used.

The file format to use is determined from the file name if the extension is recognized
(``.gz``, ``.bz2`` or ``.xz``).
When reading a file without a recognized file extension, xopen attempts to detect the format
by reading the first couple of bytes from the file.

``xopen`` is compatible with Python versions 3.7 and later.


Usage
-----

Open a file for reading::

    from xopen import xopen

    with xopen("file.txt.gz") as f:
        content = f.read()

Write to a file in binary mode,
set the compression level
and avoid using an external process::

    from xopen import xopen

    with xopen("file.txt.xz", mode="wb", threads=0, compresslevel=3)
        f.write(b"Hello")


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


Changes
-------

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
`utils.h file which is part of
BWA <https://github.com/lh3/bwa/blob/83662032a2192d5712996f36069ab02db82acf67/utils.h>`_.

Some ideas were taken from the `canopener project <https://github.com/selassid/canopener>`_.
If you also want to open S3 files, you may want to use that module instead.

@kyleabeauchamp contributed support for appending to files before this repository was created.


Maintainers
-----------

* Marcel Martin
* Ruben Vorderman
* For a list of contributors, see <https://github.com/pycompression/xopen/graphs/contributors>


Links
-----

* `Source code <https://github.com/pycompression/xopen/>`_
* `Report an issue <https://github.com/pycompression/xopen/issues>`_
* `Project page on PyPI (Python package index) <https://pypi.python.org/pypi/xopen/>`_
