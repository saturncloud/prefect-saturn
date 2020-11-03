"""
This module contains the user-facing API for ``prefect-saturn``.
"""

import hashlib
import os

from typing import Any, Dict, List, Optional
from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import cloudpickle
from prefect import Flow
from prefect.client import Client
from prefect.engine.executors import DaskExecutor
from prefect.environments.storage import Webhook
from prefect.environments import KubernetesJobEnvironment
import yaml

from .messages import Errors


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
        try:
            SATURN_TOKEN = os.environ["SATURN_TOKEN"]
        except KeyError as err:
            raise RuntimeError(Errors.missing_env_var("SATURN_TOKEN")) from err

        try:
            base_url = os.environ["BASE_URL"]
            if base_url.endswith("/"):
                raise RuntimeError(Errors.BASE_URL_NO_SLASH)
            self._base_url: str = base_url
        except KeyError as err:
            raise RuntimeError(Errors.missing_env_var("BASE_URL")) from err

        self.prefect_cloud_project_name: str = prefect_cloud_project_name
        self._saturn_flow_id: Optional[str] = None
        self._saturn_flow_version_id: Optional[str] = None
        self._saturn_image: Optional[str] = None
        self._saturn_flow_labels: Optional[List[str]] = None

        # set up logic for authenticating with Saturn back-end service
        retry_logic = HTTPAdapter(max_retries=Retry(total=3))
        self._session = Session()
        self._session.mount("http://", retry_logic)
        self._session.mount("https://", retry_logic)
        self._session.headers.update({"Authorization": f"token {SATURN_TOKEN}"})

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
        identifying_content = [
            self.prefect_cloud_project_name,
            flow.name,
            Client()._active_tenant_id,  # pylint: disable=protected-access
        ]
        hasher = hashlib.sha256()
        hasher.update(cloudpickle.dumps(identifying_content))
        return hasher.hexdigest()

    def _set_flow_metadata(self, flow: Flow) -> None:
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
        res = self._session.put(
            url=f"{self._base_url}/api/prefect_cloud/flows",
            headers={"Content-Type": "application/json"},
            json={
                "name": flow.name,
                "prefect_cloud_project_name": self.prefect_cloud_project_name,
                "flow_hash": self._hash_flow(flow),
            },
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
    ) -> Flow:
        """
        Given a flow, set up all the details needed to run it on
        a Saturn Dask cluster.

        :param flow: A Prefect ``Flow`` object
        :param dask_cluster_kwargs: Dictionary of keyword arguments
            to the ``prefect_saturn.SaturnCluster`` constructor. If ``None``
            (the default), the cluster will be created with
            one worker (``{"n_workers": 1}``).
        :param dask_adapt_kwargs: Dictionary of keyword arguments
            to pass to ``prefect_saturn.SaturnCluster.adapt()``. If
            ``None`` (the default), adaptive scaling will not be used.

        Why adaptive scaling is off by default
        --------------------------------------

        Dasks's `adaptive scaling <https://docs.dask.org/en/latest/setup/adaptive.html>`_
        can improve resource utilization by allowing Dask to spin things up
        and down based on your workload.

        This is off by default in the ``DaskExecutor`` created by ``prefect-saturn``
        because in some cases, the interaction between Dask and Prefect can lead
        adaptive scaling to make choices that interfere with the way Prefect executes
        flows.

        Prefect components
        ------------------

        This method modifies the following components of ``Flow`` objects
        passed to it.

        * ``.storage``: a ``Webhook`` storage instance is added
        * ``.environment``: a ``KubernetesJobEnvironment`` with a ``DaskExecutor``
            is added. This environment will use the same image as the notebook
            from which this code is run.
        """
        if dask_cluster_kwargs is None:
            dask_cluster_kwargs = {"n_workers": 1}

        if dask_adapt_kwargs is None:
            dask_adapt_kwargs = {}

        self._set_flow_metadata(flow)

        storage = self._get_storage()
        flow.storage = storage

        environment = self._get_environment(
            cluster_kwargs=dask_cluster_kwargs, adapt_kwargs=dask_adapt_kwargs  # type: ignore
        )
        flow.environment = environment

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

    def _get_environment(
        self,
        cluster_kwargs: Dict[str, Any],
        adapt_kwargs: Dict[str, Any],
    ) -> KubernetesJobEnvironment:
        """
        Get an environment that customizes the execution of a Prefect flow run.
        """

        # get job spec with Saturn details from Atlas
        url = f"{self._base_url}/api/prefect_cloud/flows/{self.flow_id}/run_job_spec"
        response = self._session.get(url=url)
        response.raise_for_status()
        job_dict = response.json()

        local_tmp_file = "/tmp/prefect-flow-run.yaml"
        with open(local_tmp_file, "w") as f:
            f.write(yaml.dump(job_dict))

        # saturn_flow_id is used by Saturn's custom Prefect agent
        k8s_environment = KubernetesJobEnvironment(
            metadata={"saturn_flow_id": self.flow_id, "image": self.image},
            executor=DaskExecutor(
                cluster_class="dask_saturn.SaturnCluster",
                cluster_kwargs=cluster_kwargs,
                adapt_kwargs=adapt_kwargs,
            ),
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
