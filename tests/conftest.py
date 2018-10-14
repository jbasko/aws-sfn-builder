import json
from typing import Callable

import pytest


@pytest.fixture(scope="session")
def example() -> Callable:
    """
    Example JSON loader.
    Returns a function calling of which with the name of an example returns the parsed JSON (a dictionary).
    """

    from tests import aws_examples_dir

    def loader(example_name):
        with open(aws_examples_dir / f"{example_name}.json", "r") as f:
            return json.load(f)

    return loader
