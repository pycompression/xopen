.. image:: https://travis-ci.org/marcelm/xopen.svg?branch=master
    :target: https://travis-ci.org/marcelm/xopen

.. image:: https://img.shields.io/pypi/v/xopen.svg?branch=master
    :target: https://pypi.python.org/pypi/xopen

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
available.

This module has originally been developed as part of the `cutadapt
tool <https://cutadapt.readthedocs.io/>`_ that is used in bioinformatics to
manipulate sequencing data. It has been in successful use within that software
for a few years.

``xopen`` is compatible with Python versions 2.7 and 3.4 to 3.7.


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
make reading gzipped files faster.

Some ideas were taken from the `canopener project <https://github.com/selassid/canopener>`_.
If you also want to open S3 files, you may want to use that module instead.


Changes
-------

v0.6.0
~~~~~~
* For reading from gzipped files, xopen will now use a ``pigz`` subprocess.
  This is faster than using ``gzip.open``.
* Python 2 supported will be dropped in one of the next releases.

v0.5.0
~~~~~~
* By default, pigz is now only allowed to use at most four threads. This hopefully reduces
  problems some users had with too many threads when opening many files at the same time.
* xopen now accepts pathlib.Path objects.


Author
------

Marcel Martin <mail@marcelm.net> (`@marcelm_ on Twitter <https://twitter.com/marcelm_>`_)

Links
-----

* `Source code <https://github.com/marcelm/xopen/>`_
* `Report an issue <https://github.com/marcelm/xopen/issues>`_
* `Project page on PyPI (Python package index) <https://pypi.python.org/pypi/xopen/>`_
