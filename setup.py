import sys
from setuptools import setup, find_packages

if sys.version_info < (2, 7):
    sys.stdout.write("At least Python 2.7 is required.\n")
    sys.exit(1)

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='xopen',
    use_scm_version={'write_to': 'src/xopen/_version.py'},
    setup_requires=['setuptools_scm'],  # Support pip versions that don't know about pyproject.toml
    author='Marcel Martin',
    author_email='mail@marcelm.net',
    url='https://github.com/marcelm/xopen/',
    description='Open compressed files transparently',
    long_description=long_description,
    license='MIT',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    install_requires=[
        'bz2file; python_version=="2.7"',
    ],
    extras_require={
        'dev': ['pytest'],
    },
    python_requires='>=2.7, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, <4',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ]
)
