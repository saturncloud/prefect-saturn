import re

from prefect_saturn.messages import Errors


def test_missing_env_var():
    expected_message = "Required environment variable SILLY_NONSENSE not set"
    error_message = Errors.missing_env_var("SILLY_NONSENSE")
    assert bool(re.search(expected_message, error_message))
