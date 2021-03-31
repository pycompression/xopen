from setuptools import setup, find_packages

with open('README.rst') as f:
    long_description = f.read()

setup(
    name='xopen',
    use_scm_version={'write_to': 'src/xopen/_version.py'},
    setup_requires=['setuptools_scm'],  # Support pip versions that don't know about pyproject.toml
    author='Marcel Martin et al.',
    author_email='mail@marcelm.net',
    url='https://github.com/pycompression/xopen/',
    description='Open compressed files transparently',
    long_description=long_description,
    license='MIT',
    package_dir={'': 'src'},
    packages=find_packages('src'),
    package_data={"xopen": ["py.typed"]},
    extras_require={
        'dev': ['pytest'],
        # Install isa-l on 64 bit platforms. Python-isal wheels are provided for:
        # x86_64: Linux and MacOS x86_64 platforms
        # AMD64: Windows x86_64 platforms.
        # aarch64: Linux ARM 64-bit platforms.
        # Wheels are not provided for 'arm64'. The MacOS 64 bit platforms. 
        ':platform.machine in ["x86_64","AMD64", "aarch64"]': ['isal>=0.9.0'],
    },
    python_requires='>=3.6',
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
    ]
)
