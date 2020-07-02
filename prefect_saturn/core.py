"""
This module contains the user-facing API for ``prefect-saturn``.
"""

import hashlib
import os

from typing import Any, Dict, Optional
from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


import cloudpickle
import prefect
import yaml

from prefect import Flow
from prefect.engine.executors import DaskExecutor
from prefect.environments.storage import Docker
from prefect.environments import KubernetesJobEnvironment

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
                "s3-flow-storage"
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

        self._saturn_details: Optional[Dict[str, Any]] = None

    def _hash_flow(self, flow: Flow) -> str:
        """
        Given a prefect flow object, return a hash for its contents.
        Currently uses sha256
        """
        # flow's hash shouldn't include storage or environment, since those
        # have to be created after the flow was created
        #     - tasks,
        #     - flow name,
        #     - Prefect Cloud project name
        #     - prefect version
        #     - saturn image
        identifying_content = [
            flow.name,
            self.prefect_cloud_project_name,
            flow.tasks,
            prefect.__version__,
            self._saturn_base_image,
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
    def saturn_details(self) -> Dict[str, Any]:
        """
        Information from Atlas used to build the storage and environment objects.
        This method can only be run for flows which have already been registered
        with ``register_flow_with_saturn()``.

        updates ``self._saturn_details`` with the following Saturn-specific details for this flow:
            - registry_url: the container registry to push flow storage too, since
                Saturn currently uses ``Docker`` storage from Prefect
            - host_aliases: a bit of kubernetes networking stuff you can ignore
            - deployment_token: a long-lived token that uniquely identifies this flow
            - image_name: name for the Docker image that will store the flow
        """
        if self._saturn_flow_id is None:
            raise RuntimeError(Errors.NOT_REGISTERED)

        res = self._session.get(
            url=f"{self._base_url}api/prefect_cloud/flows/{self._saturn_flow_id}/saturn_details",
            headers={"Content-Type": "application/json"},
        )
        res.raise_for_status()
        response_json = res.json()
        self._saturn_details = response_json
        return response_json

    def add_storage(self, flow: Flow) -> Flow:
        """
        Get a Docker Storage object with Saturn-y
        details.
        """
        saturn_details = self.saturn_details
        image_tag = self._hash_flow(flow)[0:12]
        # NOTE: SATURN_TOKEN and BASE_URL have to be set to be able
        #       to load the flow. Those variables will be overridden by
        #       Kubernetes in all the places where it matters, and values
        #       set in the image are overridden by those from kubernetes
        #       https://kubernetes.io/docs/tasks/inject-data-application/define-environment-variable-container
        flow.storage = Docker(
            base_image=self._saturn_base_image,
            image_name=saturn_details["image_name"],
            image_tag=image_tag,
            registry_url=saturn_details["registry_url"],
            python_dependencies=["git+https://github.com/saturncloud/dask-saturn@main"],
            prefect_directory="/tmp",
            env_vars={"SATURN_TOKEN": "placeholder-token", "BASE_URL": "placeholder-url"},
        )
        return flow

    def build_storage(self, flow: Flow):
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
    ) -> KubernetesJobEnvironment:
        """
        Get an environment that customizes the execution of a Prefect flow run.
        """
        cluster_kwargs = cluster_kwargs or {"n_workers": 1}
        adapt_kwargs = adapt_kwargs or {"minimum": 1, "maximum": 2}
        saturn_details = self.saturn_details
        job_name = f"pfct-{flow.name}"
        job_suffix = self._hash_flow(flow)[0:12]
        job_name = f"{job_name}-{job_suffix}"
        host_aliases = saturn_details["host_aliases"]
        job_env = {
            "BASE_URL": self._base_url,
            "SATURN_TOKEN": saturn_details["deployment_token"],
        }

        # fill out template for the jobs that handle flow runs
        template_content = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": job_name, "labels": {"identifier": "", "flow_run_id": ""}},
            "spec": {
                "template": {
                    "metadata": {"labels": {"identifier": ""}},
                    "spec": {
                        "restartPolicy": "Never",
                        "hostAliases": host_aliases,
                        "containers": [
                            {
                                "name": "flow-container",
                                "image": "",
                                "command": [],
                                "args": [],
                                "env": [{"name": k, "value": v} for k, v in job_env.items()],
                            }
                        ],
                    },
                }
            },
        }

        local_tmp_file = "/tmp/prefect-flow-run.yaml"
        with open(local_tmp_file, "w") as f:
            f.write(yaml.dump(template_content))

        flow.environment = KubernetesJobEnvironment(
            executor=DaskExecutor(
                cluster_class="dask_saturn.SaturnCluster",
                cluster_kwargs=cluster_kwargs,
                adapt_kwargs=adapt_kwargs,
            ),
            job_spec_file=local_tmp_file,
        )
        return flow
