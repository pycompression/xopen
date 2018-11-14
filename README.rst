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
For example, simply using ``gzip.open`` is very slow in older Pythons, and
it is a lot faster to use a ``gzip`` subprocess. For writing to gzip files,
``xopen`` uses ``pigz`` when available.

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

Open a file for writing::

    from xopen import xopen

    with xopen('file.txt.gz', mode='w') as f:
        f.write('Hello')


Credits
-------

The name ``xopen`` was taken from the C function of the same name in the
`utils.h file which is part of BWA <https://github.com/lh3/bwa/blob/83662032a2192d5712996f36069ab02db82acf67/utils.h>`_.

Kyle Beauchamp <https://github.com/kyleabeauchamp/> has contributed support for appending to files.

Some ideas were taken from the `canopener project <https://github.com/selassid/canopener>`_.
If you also want to open S3 files, you may want to use that module instead.


Author
------

Marcel Martin <mail@marcelm.net> (`@marcelm_ on Twitter <https://twitter.com/marcelm_>`_)

Links
-----

* `Source code <https://github.com/marcelm/xopen/>`_
* `Report an issue <https://github.com/marcelm/xopen/issues>`_
* `Project page on PyPI (Python package index) <https://pypi.python.org/pypi/xopen/>`_
