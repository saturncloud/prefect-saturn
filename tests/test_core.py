import os
import prefect
import prefect_saturn
import random
import responses
import uuid
import yaml

from copy import deepcopy
from typing import Any, Dict, Optional

from prefect import task, Flow
from prefect.engine.executors import DaskExecutor
from prefect.environments import KubernetesJobEnvironment
from prefect.environments.storage import Docker
from pytest import raises
from requests.exceptions import HTTPError
from unittest.mock import patch

os.environ["SATURN_TOKEN"] = "placeholder-token"
os.environ["BASE_URL"] = "http://placeholder-url"

TEST_FLOW_ID = random.randint(1, 500)
TEST_FLOW_NAME = "plz-w0rk"
TEST_DEPLOYMENT_TOKEN = str(uuid.uuid4())
TEST_IMAGE = "test-image:0.1"
TEST_PREFECT_PROJECT_NAME = "i-luv-pr3f3ct"
TEST_REGISTRY_URL = "12345.ecr.aws"


@task
def hello_task():
    logger = prefect.context.get("logger")
    logger.info("hola")


TEST_FLOW = Flow(TEST_FLOW_NAME, tasks=[hello_task])

# ------------------ #
# /api/current_image #
# ------------------ #
CURRENT_IMAGE_RESPONSE = {
    "method": responses.GET,
    "url": f"{os.environ['BASE_URL']}/api/current_image",
    "json": {"image": TEST_IMAGE},
    "status": 200,
}


def CURRENT_IMAGE_FAILURE_RESPONSE(status: int):
    return {
        "method": responses.GET,
        "url": f"{os.environ['BASE_URL']}/api/current_image",
        "json": {"image": TEST_IMAGE},
        "status": status,
    }


# ------------------------ #
# /api/prefect_cloud/flows #
# ------------------------ #
def REGISTER_FLOW_RESPONSE(
    flow_id: Optional[int] = None,
    name: Optional[str] = None,
    prefect_cloud_project_name: Optional[str] = None,
    flow_hash: Optional[str] = None,
    deployment_id: Optional[int] = None,
    status: Optional[int] = None,
) -> Dict[str, Any]:
    return {
        "method": responses.PUT,
        "url": f"{os.environ['BASE_URL']}/api/prefect_cloud/flows",
        "json": {
            "id": flow_id or TEST_FLOW_ID,
            "name": name or "c00l-flow",
            "prefect_cloud_project_name": prefect_cloud_project_name or TEST_PREFECT_PROJECT_NAME,
            "flow_hash": flow_hash or "chicago",
            "deployment": {"id": deployment_id or random.randint(1, 500)},
        },
        "status": status or 201,
    }


def REGISTER_FLOW_FAILURE_RESPONSE(status: int):
    return {
        "method": responses.PUT,
        "url": f"{os.environ['BASE_URL']}/api/prefect_cloud/flows",
        "status": status,
    }


# -------------------------------------------- #
# /api/prefect_cloud/flows/{id}/storage_details #
# -------------------------------------------- #
TEST_NODE_ROLE_KEY = "some-stuff/role"
TEST_NODE_ROLE = "medium"
TEST_ENV_SECRET_NAME = "prefect-some-n0ns3nse"
STORAGE_DETAILS_RESPONSE = {
    "method": responses.GET,
    "url": f"{os.environ['BASE_URL']}/api/prefect_cloud/flows/{TEST_FLOW_ID}/storage_details",
    "json": {"registry_url": TEST_REGISTRY_URL, "image_name": TEST_IMAGE},
    "status": 200,
}

STORAGE_DETAILS_FAILURE_RESPONSE = {
    "method": responses.GET,
    "url": f"{os.environ['BASE_URL']}/api/prefect_cloud/flows/{TEST_FLOW_ID}/storage_details",
    "status": 404,
}

# ----------------------------------- #
# /api/prefect_cloud/flows/{id}/store #
# ----------------------------------- #
BUILD_STORAGE_RESPONSE = {
    "method": responses.POST,
    "url": f"{os.environ['BASE_URL']}/api/prefect_cloud/flows/{TEST_FLOW_ID}/store",
    "status": 201,
}


# ------------------------------------------ #
# /api/prefect_cloud/flows/{id}/run_job_spec #
# ------------------------------------------ #
def REGISTER_RUN_JOB_SPEC_RESPONSE(status: int, flow_id: int = TEST_FLOW_ID) -> Dict[str, Any]:
    run_job_spec_file = os.path.join(os.path.dirname(__file__), "run-job-spec.yaml")
    with open(run_job_spec_file, "r") as file:
        run_job_spec = yaml.load(file, Loader=yaml.FullLoader)

    base_url = os.environ["BASE_URL"]
    return {
        "method": responses.GET,
        "url": f"{base_url}/api/prefect_cloud/flows/{flow_id}/run_job_spec",
        "json": run_job_spec,
        "status": status,
    }


class MockClient:
    def __init__(self):
        self._active_tenant_id = "543c5453-0a47-496a-9c61-a6765acef352"
        pass


@responses.activate
def test_initialize():
    responses.add(**CURRENT_IMAGE_RESPONSE)

    project_name = "some-pr0ject"
    integration = prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=project_name)
    assert getattr(integration, "SATURN_TOKEN", None) is None
    assert integration._session.headers["Authorization"] == f"token {os.environ['SATURN_TOKEN']}"
    assert integration.prefect_cloud_project_name == project_name
    assert integration._saturn_base_image == TEST_IMAGE
    assert integration._saturn_flow_id is None

    # __init__() should add this trailing slash if it's necessary
    assert integration._base_url == os.environ["BASE_URL"] + "/"


@responses.activate
def test_initialize_raises_error_on_failure():
    responses.add(**CURRENT_IMAGE_FAILURE_RESPONSE(403))

    with raises(HTTPError, match="403 Client Error"):
        prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME)

    failure_response = CURRENT_IMAGE_FAILURE_RESPONSE(504)
    failure_response["method_or_response"] = failure_response.pop("method")
    responses.replace(**failure_response)
    with raises(HTTPError, match="504 Server Error"):
        prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME)


def test_initialize_raises_error_on_missing_saturn_token(monkeypatch):
    monkeypatch.delenv("SATURN_TOKEN")
    with raises(RuntimeError, match=prefect_saturn.Errors.missing_env_var("SATURN_TOKEN")):
        prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME)


def test_initialize_raises_error_on_missing_base_url(monkeypatch):
    monkeypatch.delenv("BASE_URL")
    with raises(RuntimeError, match=prefect_saturn.Errors.missing_env_var("BASE_URL")):
        prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME)


@responses.activate
def test_hash_flow():
    responses.add(**CURRENT_IMAGE_RESPONSE)

    flow = TEST_FLOW.copy()

    integration = prefect_saturn.PrefectCloudIntegration(
        prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
    )

    with patch("prefect_saturn.core.Client", new=MockClient):
        flow_hash = integration._hash_flow(flow)
        assert isinstance(flow_hash, str) and len(flow_hash) > 0

        # should be deterministic
        flow_hash_again = integration._hash_flow(flow)
        assert flow_hash == flow_hash_again

        # should not be impacted by storage
        flow.storage = Docker()
        assert flow_hash == integration._hash_flow(flow)

        # should not be impacted by environment
        flow.environment = KubernetesJobEnvironment()
        assert flow_hash == integration._hash_flow(flow)

        # should not change if you add a new task
        @task
        def goodbye_task():
            logger = prefect.context.get("logger")
            logger.info("adios")

        flow.tasks = [hello_task, goodbye_task]
        new_flow_hash = integration._hash_flow(flow)

        assert isinstance(new_flow_hash, str) and len(new_flow_hash) > 0
        assert new_flow_hash == flow_hash

        # should change if flow name changes
        flow.name = str(uuid.uuid4())
        new_flow_hash = integration._hash_flow(flow)
        assert new_flow_hash != flow_hash

        # should change if project name changes
        previous_flow_hash = new_flow_hash
        integration.prefect_cloud_project_name = str(uuid.uuid4())
        new_flow_hash = integration._hash_flow(flow)
        assert isinstance(new_flow_hash, str) and len(new_flow_hash) > 0
        assert new_flow_hash != previous_flow_hash


@responses.activate
def test_hash_flow_hash_changes_if_tenant_id_changes():
    responses.add(**CURRENT_IMAGE_RESPONSE)

    flow = TEST_FLOW.copy()

    integration = prefect_saturn.PrefectCloudIntegration(
        prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
    )

    with patch("prefect_saturn.core.Client", new=MockClient):
        flow_hash = integration._hash_flow(flow)
        assert isinstance(flow_hash, str) and len(flow_hash) > 0

    class OtherMockClient:
        def __init__(self):
            self._active_tenant_id = "some-other-garbage"

    with patch("prefect_saturn.core.Client", new=OtherMockClient):
        new_flow_hash = integration._hash_flow(flow)
        assert isinstance(new_flow_hash, str) and len(new_flow_hash) > 0
        assert new_flow_hash != flow_hash


@responses.activate
def test_register_flow_with_saturn():
    with patch("prefect_saturn.core.Client", new=MockClient):
        test_flow_id = random.randint(1, 500)
        responses.add(**CURRENT_IMAGE_RESPONSE)
        responses.add(**REGISTER_FLOW_RESPONSE(flow_id=test_flow_id))

        # Set up integration
        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )

        assert integration._saturn_flow_id is None
        integration.register_flow_with_saturn(flow=TEST_FLOW.copy())
        assert integration._saturn_flow_id == test_flow_id


@responses.activate
def test_register_flow_with_saturn_raises_error_on_failure():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )

        responses.add(**REGISTER_FLOW_FAILURE_RESPONSE(500))
        with raises(HTTPError, match="500 Server Error"):
            integration.register_flow_with_saturn(flow=TEST_FLOW.copy())

        failure_response = REGISTER_FLOW_FAILURE_RESPONSE(401)
        failure_response["method_or_response"] = failure_response.pop("method")
        responses.replace(**failure_response)
        with raises(HTTPError, match="401 Client Error"):
            integration.register_flow_with_saturn(flow=TEST_FLOW.copy())


@responses.activate
def test_get_storage_details():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)
        responses.add(**REGISTER_FLOW_RESPONSE())

        test_token = str(uuid.uuid4())
        test_registry = "8987.ecr.aws"
        details = deepcopy(STORAGE_DETAILS_RESPONSE)
        details["json"].update({"registry_url": test_registry, "deployment_token": test_token})
        responses.add(**details)

        # Set up integration
        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        integration.register_flow_with_saturn(flow=TEST_FLOW.copy())

        # response with details for building storage
        storage_details = integration.storage_details
        assert isinstance(storage_details, dict)
        assert integration._storage_details["image_name"] == TEST_IMAGE
        assert integration._storage_details["registry_url"] == test_registry


@responses.activate
def test_get_storage_details_raises_error_on_failure():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**STORAGE_DETAILS_FAILURE_RESPONSE)

        # Set up integration
        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        integration.register_flow_with_saturn(flow=TEST_FLOW.copy())
        with raises(HTTPError, match="404 Client Error"):
            integration.storage_details


@responses.activate
def test_build_environment():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**STORAGE_DETAILS_RESPONSE)
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        integration.register_flow_with_saturn(flow=flow)

        flow = integration.add_environment(flow=flow)
        assert isinstance(flow.environment, KubernetesJobEnvironment)
        assert isinstance(flow.environment.executor, DaskExecutor)
        assert flow.environment.unique_job_name is True
        env_args = flow.environment._job_spec["spec"]["template"]["spec"]["containers"][0]["args"]
        assert len(env_args) == 1
        assert env_args[0].startswith("source /home/jovyan/.saturn/start.sh;")
        env_cmd = flow.environment._job_spec["spec"]["template"]["spec"]["containers"][0]["command"]
        assert env_cmd == ["/bin/bash", "-ec"]


@responses.activate
def test_add_storage():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**STORAGE_DETAILS_RESPONSE)

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        integration.register_flow_with_saturn(flow=flow)

        assert flow.storage is None
        flow = integration.add_storage(flow=flow)
        assert isinstance(flow.storage, Docker)
        assert flow.storage.base_image == TEST_IMAGE
        assert flow.storage.image_name == integration.storage_details["image_name"]
        assert flow.storage.registry_url == TEST_REGISTRY_URL
        assert flow.storage.prefect_directory == "/tmp"
        assert "kubernetes" in flow.storage.python_dependencies
        assert "BASE_URL" in flow.storage.env_vars.keys()
        assert "SATURN_TOKEN" in flow.storage.env_vars.keys()

        # healthchecks have to be ignored because otherwise we'd have
        # to figure out how to run the Saturn start script inside
        # the build process
        assert flow.storage.ignore_healthchecks is True


@responses.activate
def test_add_storage_fails_if_flow_not_registerd():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)
        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        with raises(RuntimeError, match=prefect_saturn.Errors.NOT_REGISTERED):
            integration.add_storage(flow=flow)


@responses.activate
def test_build_storage():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**STORAGE_DETAILS_RESPONSE)
        responses.add(**BUILD_STORAGE_RESPONSE)

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        integration.register_flow_with_saturn(flow=flow)
        flow = integration.add_storage(flow=flow)
        res = integration.build_storage(flow)
        assert res.status_code == 201


@responses.activate
def test_build_storage_fails_if_flow_not_registered():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**STORAGE_DETAILS_RESPONSE)
        responses.add(**BUILD_STORAGE_RESPONSE)

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        integration.register_flow_with_saturn(flow=flow)
        flow = integration.add_storage(flow=flow)

        integration._saturn_flow_id = None
        with raises(RuntimeError, match=prefect_saturn.Errors.NOT_REGISTERED):
            integration.build_storage(flow)


@responses.activate
def test_add_environment_fails_if_flow_not_registered():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()

        with raises(RuntimeError, match=prefect_saturn.Errors.NOT_REGISTERED):
            integration.add_environment(flow=flow)


@responses.activate
def test_add_environment_fails_if_id_not_recognized():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**CURRENT_IMAGE_RESPONSE)
        responses.add(**REGISTER_FLOW_RESPONSE(45))
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(404, flow_id=45))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        integration.register_flow_with_saturn(flow=flow)

        with raises(HTTPError, match="404 Client Error"):
            integration.add_environment(flow=flow)
