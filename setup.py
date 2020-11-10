import sys
from setuptools import setup, find_packages

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
    package_data={"xopen": ["py.typed"]},
    extras_require={
        'dev': ['pytest'],
    },
    python_requires='>=3.5',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ]
)
