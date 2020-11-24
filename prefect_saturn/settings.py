"""
This module contains settings for ``prefect-saturn``.
"""

import os

from .messages import Errors

try:
    BASE_URL = os.environ["BASE_URL"]
    if BASE_URL.endswith("/"):
        raise RuntimeError(Errors.BASE_URL_NO_SLASH)
except KeyError as err:
    raise RuntimeError(Errors.missing_env_var("BASE_URL")) from err

try:
    SATURN_TOKEN = os.environ["SATURN_TOKEN"]
except KeyError as err:
    raise RuntimeError(Errors.missing_env_var("SATURN_TOKEN")) from err
