"""
This module contains settings for ``prefect-saturn``.
"""

import os

from .messages import Errors


class Settings:
    """Global settings"""

    def __init__(self):
        try:
            self.BASE_URL = os.environ["BASE_URL"]
            if self.BASE_URL.endswith("/"):
                raise RuntimeError(Errors.BASE_URL_NO_SLASH)
        except KeyError as err:
            raise RuntimeError(Errors.missing_env_var("BASE_URL")) from err

        try:
            self.SATURN_TOKEN = os.environ["SATURN_TOKEN"]
        except KeyError as err:
            raise RuntimeError(Errors.missing_env_var("SATURN_TOKEN")) from err
