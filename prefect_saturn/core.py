"""
This module contains the user-facing API for ``prefect-saturn``.
"""

import hashlib
import os
import uuid

from typing import Any, Dict, Optional
from requests import Session
from requests.adapters import HTTPAdapter
from requests.models import Response
from requests.packages.urllib3.util.retry import Retry

import cloudpickle
from prefect import Flow
from prefect.client import Client
from prefect.engine.executors import DaskExecutor
from prefect.environments.storage import Docker
from prefect.environments import KubernetesJobEnvironment
import yaml

from .messages import Errors


class PrefectCloudIntegration:
    """
    :Example:

    .. code-block:: python

        integration = PrefectCloudIntegration(PROJECT_NAME)
        integration.register_flow_with_saturn(flow)
        flow = integration.add_environment(flow)
        flow = integration.add_storage(flow)
        flow.storage.add_flow(flow)
        integration.build_storage(flow)
        flow.register(
            project_name=PROJECT_NAME,
            build=False,
            labels=[
                "saturn-cloud"
            ]
        )
    """

    def __init__(self, prefect_cloud_project_name: str):
        try:
            SATURN_TOKEN = os.environ["SATURN_TOKEN"]
        except KeyError:
            raise RuntimeError(Errors.missing_env_var("SATURN_TOKEN"))

        try:
            base_url = os.environ["BASE_URL"]
            if not base_url.endswith("/"):
                base_url += "/"
            self._base_url: str = base_url
        except KeyError:
            raise RuntimeError(Errors.missing_env_var("BASE_URL"))

        self.prefect_cloud_project_name: str = prefect_cloud_project_name
        self._saturn_flow_id: Optional[int] = None

        # set up logic for authenticating with Saturn back-end service
        retry_logic = HTTPAdapter(max_retries=Retry(total=3))
        self._session = Session()
        self._session.mount("http://", retry_logic)
        self._session.mount("https://", retry_logic)
        self._session.headers.update({"Authorization": f"token {SATURN_TOKEN}"})

        # figure out the image this notebook is running in
        res = self._session.get(
            url=f"{self._base_url}api/current_image", headers={"Content-Type": "application/json"}
        )
        res.raise_for_status()
        self._saturn_base_image = res.json()["image"]

        self._storage_details: Optional[Dict[str, Any]] = None

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

    def register_flow_with_saturn(self, flow: Flow) -> bool:
        """
        Given a Flow, register it with Saturn. This method has
        the following side effects:

        The first time you run it:

            * create a Deployment in Saturn's database for this flow
            * create a record of the flow in Saturn's database
            * store a hash of the flow in the database
            * store a commit hash for the current state of /home/jovyan/project

        Any time you run it

            * update the flow's hash in the database
            * update the /home/jovyan/project commit hash stored in the DB
            * update the deployment's image to the current image for the userproject
                it belongs to
            * sets ``self._saturn_flow_id`` on this instance
        """
        res = self._session.put(
            url=f"{self._base_url}api/prefect_cloud/flows",
            headers={"Content-Type": "application/json"},
            json={
                "name": flow.name,
                "prefect_cloud_project_name": self.prefect_cloud_project_name,
                "flow_hash": self._hash_flow(flow),
            },
        )
        res.raise_for_status()
        response_json = res.json()
        self._saturn_flow_id = response_json["id"]
        return True

    @property
    def storage_details(self) -> Dict[str, Any]:
        """
        Information from Atlas used to build flow storage.
        This method can only be run for flows which have already been registered
        with ``register_flow_with_saturn()``.

        updates ``self._storage_details`` with the following Saturn-specific details for this flow:
            - registry_url: the container registry to push flow storage too, since
                Saturn currently uses ``Docker`` storage from Prefect
            - image_name: name for the Docker image that will store the flow
        """
        if self._saturn_flow_id is None:
            raise RuntimeError(Errors.NOT_REGISTERED)

        res = self._session.get(
            url=f"{self._base_url}api/prefect_cloud/flows/{self._saturn_flow_id}/storage_details",
            headers={"Content-Type": "application/json"},
        )
        res.raise_for_status()
        response_json = res.json()
        self._storage_details = response_json
        return response_json

    def add_storage(self, flow: Flow) -> Flow:
        """
        Create a Docker Storage object with Saturn-y details and set
        it on `flow.storage`.

        This method sets the `image_tag` to a random string to avoid conflicts.
        The image name is generated in Saturn.
        """
        storage_details = self.storage_details
        image_tag = str(uuid.uuid4())
        # NOTE: SATURN_TOKEN and BASE_URL have to be set to be able
        #       to load the flow. Those variables will be overridden by
        #       Kubernetes in all the places where it matters, and values
        #       set in the image are overridden by those from kubernetes
        #       https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container
        flow.storage = Docker(
            base_image=self._saturn_base_image,
            image_name=storage_details["image_name"],
            image_tag=image_tag,
            registry_url=storage_details["registry_url"],
            prefect_directory="/tmp",
            env_vars={"SATURN_TOKEN": "placeholder-token", "BASE_URL": "placeholder-url"},
            python_dependencies=["kubernetes"],
            ignore_healthchecks=True,
        )
        return flow

    def build_storage(self, flow: Flow) -> Response:
        """
        Actually build and push the storage
        """
        if self._saturn_flow_id is None:
            raise RuntimeError(Errors.NOT_REGISTERED)
        res = self._session.post(
            url=f"{self._base_url}api/prefect_cloud/flows/{self._saturn_flow_id}/store",
            headers={"Content-Type": "application/octet-stream"},
            data=cloudpickle.dumps(flow),
        )
        res.raise_for_status()
        return res

    def add_environment(
        self,
        flow: Flow,
        cluster_kwargs: Optional[Dict[str, Any]] = None,
        adapt_kwargs: Optional[Dict[str, Any]] = None,
    ) -> Flow:
        """
        Get an environment that customizes the execution of a Prefect flow run.
        """
        cluster_kwargs = cluster_kwargs or {"n_workers": 1}
        adapt_kwargs = adapt_kwargs or {"minimum": 1, "maximum": 2}

        if self._saturn_flow_id is None:
            raise RuntimeError(Errors.NOT_REGISTERED)

        # get job spec with Saturn details from Atlas
        url = f"{self._base_url}api/prefect_cloud/flows/{self._saturn_flow_id}/run_job_spec"
        response = self._session.get(url=url)
        response.raise_for_status()
        job_dict = response.json()

        local_tmp_file = "/tmp/prefect-flow-run.yaml"
        with open(local_tmp_file, "w") as f:
            f.write(yaml.dump(job_dict))

        k8s_environment = KubernetesJobEnvironment(
            metadata={"saturn_flow_id": self._saturn_flow_id},
            executor=DaskExecutor(
                cluster_class="dask_saturn.SaturnCluster",
                cluster_kwargs=cluster_kwargs,
                adapt_kwargs=adapt_kwargs,
            ),
            job_spec_file=local_tmp_file,
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
        new_args = f"source /home/jovyan/.saturn/start.sh; {args_from_prefect}"
        k8s_environment._job_spec["spec"]["template"]["spec"]["containers"][0]["args"] = [new_args]

        flow.environment = k8s_environment
        return flow
