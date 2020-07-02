"""
Central location for messages printed in exceptions or logs.
"""


class Errors:
    """
    Collection of error messages used in
    ``prefect-saturn``. Centralized here to reduce the risk
    of mistakes in testing and to improve consistency in the case
    where multiple methods use the same error message.
    """

    NOT_REGISTERED = (
        "This flow has not been registered with Saturn yet. "
        "Please call register_flow_with_saturn()"
    )

    @classmethod
    def missing_env_var(cls, env_var_name: str) -> str:
        """error message for a missing environment variable"""
        return (
            f"Required environment variable {env_var_name} not set. "
            "dask-saturn code should only be run on Saturn Cloud infrastructure."
        )
