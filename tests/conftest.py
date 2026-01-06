import os
import pytest


@pytest.fixture(autouse=True)
def _reset_env_after_each_test():
    """
    Prevent env leakage across tests.

    Some tests may toggle DISTRIBUTED_MODE (or ASYNC_MODE).
    If they do it via os.environ[...] directly, the value persists for the rest
    of the test session and can break unrelated tests.

    This fixture guarantees a clean slate after every test.
    """
    try:
        yield
    finally:
        os.environ.pop("DISTRIBUTED_MODE", None)
        os.environ.pop("ASYNC_MODE", None)
