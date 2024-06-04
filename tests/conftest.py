import os
import random
import string
import pytest

from xopen import xopen


@pytest.fixture
def create_large_file(tmp_path):
    def _create_large_file(extension):
        path = tmp_path / f"large{extension}"
        random.seed(0)
        chars = string.ascii_lowercase + "\n"
        # Do not decrease this length. The generated file needs to have
        # a certain length after compression to trigger some bugs
        # (in particular, 512 kB is not sufficient).
        random_text = "".join(random.choices(chars, k=1024 * 1024))
        with xopen(path, "w") as f:
            f.write(random_text)
        return path

    return _create_large_file


@pytest.fixture
def create_truncated_file(create_large_file):
    def _create_truncated_file(extension):
        large_file = create_large_file(extension)
        with open(large_file, "a", encoding="ascii") as f:
            f.truncate(os.stat(large_file).st_size - 10)
        return large_file

    return _create_truncated_file
