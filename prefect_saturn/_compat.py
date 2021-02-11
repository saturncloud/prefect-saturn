# pylint: disable=unused-import

"""
This module is used to handle compatibility with multiple
versions of dependencies. For example, some import paths
were deprecated in ``prefect`` 0.14.x and will be removed in the
next major release of ``prefect``.
"""

# prefect.environments.storage was deprecated in prefect 0.14.x
try:
    from prefect.storage import Webhook  # noqa: F401
except (ImportError, ModuleNotFoundError):
    from prefect.environments.storage import Webhook  # noqa: F401

# prefect.engine.executors was depreacted in prefect 0.14.x
try:
    from prefect.executors import DaskExecutor  # noqa: F401
except (ImportError, ModuleNotFoundError):
    from prefect.engine.executors import DaskExecutor  # noqa: F401
