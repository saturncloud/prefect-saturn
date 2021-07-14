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
    from prefect.environments.storage import Webhook  # type: ignore # noqa: F401

# prefect.engine.executors was deprecated in prefect 0.14.x
try:
    from prefect.executors import DaskExecutor  # noqa: F401
except (ImportError, ModuleNotFoundError):
    from prefect.engine.executors import DaskExecutor  # noqa: F401

# prefect.run_configs was introduced in prefect 0.13.10
try:
    from prefect.run_configs import KubernetesRun  # noqa: F401

    RUN_CONFIG_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    RUN_CONFIG_AVAILABLE = False

# prefect.environments.KubernetesJobEnvironment was marked "deprecated" in prefect 0.14.x
try:
    from prefect.environments import KubernetesJobEnvironment  # noqa: F401

    KUBE_JOB_ENV_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    KUBE_JOB_ENV_AVAILABLE = False
