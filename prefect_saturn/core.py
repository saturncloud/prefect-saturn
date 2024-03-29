"""
This module contains the user-facing API for ``prefect-saturn``.
"""

import hashlib
from typing import Any, Dict, List, Optional, Union
from requests import Session
from requests.adapters import HTTPAdapter

import prefect
import cloudpickle
from prefect import Flow
from prefect.client import Client

from packaging.version import Version, parse

from ruamel.yaml import YAML

from ._compat import (
    DaskExecutor,
    LocalExecutor,
    KUBE_JOB_ENV_AVAILABLE,
    RUN_CONFIG_AVAILABLE,
    Webhook,
)
from .settings import Settings
from .messages import Errors

if RUN_CONFIG_AVAILABLE:
    from prefect.run_configs import KubernetesRun  # pylint: disable=ungrouped-imports

if KUBE_JOB_ENV_AVAILABLE:
    from prefect.environments import KubernetesJobEnvironment  # pylint: disable=ungrouped-imports


def _session(token: str) -> Session:
    retry_logic = HTTPAdapter(max_retries=3)
    session = Session()
    session.mount("http://", retry_logic)
    session.mount("https://", retry_logic)
    session.headers.update({"Authorization": f"token {token}"})
    return session


def describe_sizes() -> Dict[str, Any]:
    """Returns available instance sizes for flows and dask clusters"""
    settings = Settings()
    res = _session(settings.SATURN_TOKEN).get(
        url=f"{settings.BASE_URL}/api/info/servers",
        headers={"Content-Type": "application/json"},
    )
    res.raise_for_status()
    response_json = res.json()
    return response_json["sizes"]


class PrefectCloudIntegration:
    """
    :Example:

    .. code-block:: python

        import prefect
        from prefect import Flow, task
        from prefect_saturn import PrefectCloudIntegration


        @task
        def hello_task():
            logger = prefect.context.get("logger")
            logger.info("hello prefect-saturn")


        flow = Flow("sample-flow", tasks=[hello_task])

        project_name = "sample-project"
        integration = PrefectCloudIntegration(
            prefect_cloud_project_name=project_name
        )
        flow = integration.register_flow_with_saturn(flow)

        flow.register(project_name=project_name)
    """

    def __init__(self, prefect_cloud_project_name: str):
        self.prefect_cloud_project_name: str = prefect_cloud_project_name
        self._saturn_flow_id: Optional[str] = None
        self._saturn_flow_version_id: Optional[str] = None
        self._saturn_image: Optional[str] = None
        self._saturn_flow_labels: Optional[List[str]] = None

        # set up logic for authenticating with Saturn back-end service
        self._settings = Settings()
        self._session = _session(self._settings.SATURN_TOKEN)

    def _hash_flow(self, flow: Flow) -> str:
        """
        In Prefect Cloud, all versions of a flow in a project are tied together
        by a `flow_group_id`. This is the unique identifier used to store
        flows in Saturn.

        Since this library registers a flow with Saturn Cloud before registering
        it with Prefect Cloud, it can't rely on the `flow_group_id` generated
        by Prefect Cloud. Instead, this function hashes these pieces of
        information that uniquely identify a flow group:

        * project name
        * flow name
        * tenant id

        The identifier produced here should uniquely identify all versions of a
        flow with a given name, in a given Prefect Cloud project, for a given
        Prefect Cloud tenant.
        """
        prefect_version = Version(prefect.__version__)

        if prefect_version < parse("0.15.0"):
            tenant_id = Client()._active_tenant_id  # type: ignore # pylint: disable=no-member
        else:
            tenant_id = Client().tenant_id  # type: ignore

        identifying_content = [
            self.prefect_cloud_project_name,
            flow.name,
            tenant_id,
        ]
        hasher = hashlib.sha256()
        hasher.update(cloudpickle.dumps(identifying_content, protocol=4))
        return hasher.hexdigest()

    def _set_flow_metadata(self, flow: Flow, instance_size: Union[str, None]) -> None:
        """
        Given a Flow, register it with Saturn. This method has
        the following side effects.

        The first time you run it for a flow:

            * creates a Deployment in Saturn's database for this flow
            * creates a record of the flow in Saturn's database
            * stores a hash of the flow in the database
            * stores a commit hash for the current state of the git repo in
                /home/jovyan/project
            * sets ``self._saturn_flow_id`` to an identifier for the flow in Saturn
            * sets ``self._saturn_flow_version_id`` to an identifier for this flow
                version
            * sets ``self._saturn_image`` to the name of a Docker image the flow
                will run in

        If you run this method for a flow that already exists in Saturn:

            * updates the /home/jovyan/project commit hash stored in the DB
            * updates the deployment's image to the current image for the project
                it belongs to
            * updates ``self._saturn_flow_version_id`` to a new value. Just like
                ``flow.register()`` from ``prefect``, each call of this method
                generates a new flow version.
            * updates ``self._saturn_image`` in case the image for the flow
                has changed
        """
        data = {
            "name": flow.name,
            "prefect_cloud_project_name": self.prefect_cloud_project_name,
            "flow_hash": self._hash_flow(flow),
        }
        if instance_size:
            data["instance_size"] = instance_size
        res = self._session.put(
            url=f"{self._settings.BASE_URL}/api/prefect_cloud/flows",
            headers={"Content-Type": "application/json"},
            json=data,
        )
        res.raise_for_status()
        response_json = res.json()
        self._saturn_flow_id = str(response_json["id"])
        self._saturn_flow_version_id = response_json["flow_version_id"]
        self._saturn_image = response_json["image"]
        self._saturn_flow_labels = response_json.get("labels", ["saturn-cloud"])

    @property
    def flow_id(self) -> str:
        """
        Saturn identifier for a flow. This uniquely identifies a flow
        within a Saturn instance.
        """
        if self._saturn_flow_id is None:
            raise RuntimeError(Errors.NOT_REGISTERED)
        return self._saturn_flow_id

    @property
    def flow_version_id(self) -> str:
        """
        Unique identifier for one version of a flow. This is randomly
        generated by Saturn and is not an identifier known to
        Prefect Cloud. This is used to ensure that each flow run
        pulls the correct flow version from storage.
        """
        if self._saturn_flow_version_id is None:
            raise RuntimeError(Errors.NOT_REGISTERED)
        return self._saturn_flow_version_id

    @property
    def image(self) -> str:
        """
        Docker image that flows will run in. This is the same as
        the image used in the Saturn project where the flow was
        created.
        """
        if self._saturn_image is None:
            raise RuntimeError(Errors.NOT_REGISTERED)
        return self._saturn_image

    def register_flow_with_saturn(
        self,
        flow: Flow,
        dask_cluster_kwargs: Optional[Dict[str, Any]] = None,
        dask_adapt_kwargs: Optional[Dict[str, Any]] = None,
        instance_size: Optional[str] = None,
    ) -> Flow:
        """
        Given a flow, set up all the details needed to run it on
        a Saturn Dask cluster.

        :param flow: A Prefect ``Flow`` object
        :param dask_cluster_kwargs: Dictionary of keyword arguments
            to the ``dask_saturn.SaturnCluster`` constructor. If ``None``
            (the default), no cluster will be created.
        :param dask_adapt_kwargs: Dictionary of keyword arguments
            to pass to ``dask_saturn.SaturnCluster.adapt()``. If
            ``None`` (the default), adaptive scaling will not be used.
        :param instance_size: Instance size for the flow run. Does not affect
            the size of dask workers. If ``None``, the smallest available size
            will be used.

        Prefect components
        ------------------

        This method modifies the following components of ``Flow`` objects
        passed to it.

        * ``.storage``: a ``Webhook`` storage instance is added

        If using ``prefect<0.14.0``

        * ``.environment``: a ``KubernetesJobEnvironment`` with a ``DaskExecutor``
            is added. This environment will use the same image as the notebook
            from which this code is run.

        If using ``prefect>=0.14.0``

        * ``run_config``: a ``KubernetesRun`` is added, which by default will use
            the same image, start script, and environment variables as the notebook
            from which this code is run.
        * ``executor``: a ``DaskExecutor``, which uses the same image as the notebook
            from which this code is run.

        Adaptive scaling is off by default
        --------------------------------------

        Dasks's `adaptive scaling <https://docs.dask.org/en/latest/setup/adaptive.html>`_
        can improve resource utilization by allowing Dask to spin things up
        and down based on your workload.

        This is off by default in the ``DaskExecutor`` created by ``prefect-saturn``
        because in some cases, the interaction between Dask and Prefect can lead
        adaptive scaling to make choices that interfere with the way Prefect executes
        flows.

        Dask cluster is not closed at the end of each flow run
        ------------------------------------------------------

        The first time a flow runs in Saturn, it will look for a specific Dask cluster. If
        that cluster isn't found, it will start one. By default, the Dask cluster will not
        be shut down when the flow is done running. All runs of one flow are executed on the
        same Saturn Dask cluster. Autoclosing is off by default to avoid the situation
        where you have two runs of the same flow happening at the same time, and one flow
        kills the Dask cluster the other flow is still running on.

        If you are not worried about concurrent flow runs and want to know that the Dask
        cluster will be shut down at the end of each flow run, you can override this default
        behavior with the parameter ``autoclose``. Setting this to ``True`` will tell Saturn
        to close down the Dask cluster at the end of a flow run.

        .. code-block:: python

            flow = integration.register_flow_with_saturn(
                flow=flow,
                dask_cluster_kwargs={
                    "n_workers": 4,
                    "autoclose": True
                }
            )

        Instance size
        -------------

        Use ``prefect_saturn.describe_sizes()`` to get the available instance_size options.
        The returned dict maps instance_size to a short description of the resources available on
        that size (e.g. {"medium": "Medium - 2 cores - 4 GB RAM", ...})
        """
        self._set_flow_metadata(flow, instance_size=instance_size)

        storage = self._get_storage()
        flow.storage = storage

        executor: Union[LocalExecutor, DaskExecutor] = LocalExecutor()

        if dask_cluster_kwargs is not None:
            if dask_adapt_kwargs is None:
                dask_adapt_kwargs = {}

            executor = DaskExecutor(
                cluster_class="dask_saturn.SaturnCluster",
                cluster_kwargs=dask_cluster_kwargs,
                adapt_kwargs=dask_adapt_kwargs,
            )

        if RUN_CONFIG_AVAILABLE:
            flow.executor = executor

            flow.run_config = KubernetesRun(
                job_template=self._flow_run_job_spec,
                labels=self._saturn_flow_labels,
                image=self._saturn_image,
            )
        else:
            flow.environment = self._get_environment(executor)

        return flow

    def _get_storage(self) -> Webhook:
        """
        Create a `Webhook` storage object with Saturn-y details.
        """
        url = (
            "${BASE_URL}/api/prefect_cloud/flows/"
            + self.flow_id
            + "/"
            + self.flow_version_id
            + "/content"
        )
        storage = Webhook(
            build_request_kwargs={
                "url": url,
                "headers": {
                    "Content-Type": "application/octet-stream",
                    "Authorization": "token ${SATURN_TOKEN}",
                },
            },
            build_request_http_method="POST",
            get_flow_request_kwargs={
                "url": url,
                "headers": {
                    "Accept": "application/octet-stream",
                    "Authorization": "token ${SATURN_TOKEN}",
                },
            },
            get_flow_request_http_method="GET",
        )

        return storage

    @property
    def _flow_run_job_spec(self) -> Dict[str, Any]:
        """k8s job spec with Saturn details"""
        url = f"{self._settings.BASE_URL}/api/prefect_cloud/flows/{self.flow_id}/run_job_spec"
        response = self._session.get(url=url)
        response.raise_for_status()
        job_dict = response.json()
        return job_dict

    def _get_environment(self, executor: Union[LocalExecutor, DaskExecutor]):
        """
        Get an environment that customizes the execution of a Prefect flow run.
        """

        local_tmp_file = "/tmp/prefect-flow-run.yaml"
        with open(local_tmp_file, "w", encoding="utf8") as f:
            YAML().dump(self._flow_run_job_spec, stream=f)

        # saturn_flow_id is used by Saturn's custom Prefect agent
        k8s_environment = KubernetesJobEnvironment(
            metadata={"saturn_flow_id": self.flow_id, "image": self.image},
            executor=executor,
            job_spec_file=local_tmp_file,
            labels=self._saturn_flow_labels,
            unique_job_name=True,
        )

        # patch command and args to run the user's start script
        new_command = ["/bin/bash", "-ec"]
        k8s_environment._job_spec["spec"]["template"]["spec"]["containers"][0][
            "command"
        ] = new_command

        args_from_prefect = k8s_environment._job_spec["spec"]["template"]["spec"]["containers"][
            0
        ].get("args", [])
        args_from_prefect = " ".join(args_from_prefect)
        new_args = f"source /home/jovyan/.saturn/start_wrapper.sh; {args_from_prefect}"
        k8s_environment._job_spec["spec"]["template"]["spec"]["containers"][0]["args"] = [new_args]

        return k8s_environment
