import os
import random
import string
import pytest

from xopen import xopen


@pytest.fixture
def create_large_file(tmp_path):
    def _create_large_file(extension):
        path = tmp_path / f"large{extension}"
        random_text = "".join(random.choices(string.ascii_lowercase, k=1024))
        # Make the text a lot bigger in order to ensure that it is larger than the
        # pipe buffer size.
        random_text *= 2048
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
