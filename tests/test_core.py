import os
import prefect
import prefect_saturn
import random
import responses
import uuid

from typing import Any, Dict, Optional

from prefect import task, Flow
from prefect.environments import KubernetesJobEnvironment
from pytest import raises
from requests.exceptions import HTTPError
from unittest.mock import patch
from urllib.parse import urlparse
from ruamel import yaml

from prefect_saturn._compat import Webhook, DaskExecutor, RUN_CONFIG_AVAILABLE
if RUN_CONFIG_AVAILABLE:
    from prefect.run_configs import KubernetesRun

FLOW_LABELS = [urlparse(os.environ["BASE_URL"]).hostname, "saturn-cloud", "webhook-flow-storage"]

TEST_FLOW_ID = str(random.randint(1, 500))
TEST_FLOW_VERSION_ID = str(uuid.uuid4())
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


# ------------------------ #
# /api/prefect_cloud/flows #
# ------------------------ #
def REGISTER_FLOW_RESPONSE(
    flow_id: Optional[str] = None,
    status: Optional[int] = None,
) -> Dict[str, Any]:
    return {
        "method": responses.PUT,
        "url": f"{os.environ['BASE_URL']}/api/prefect_cloud/flows",
        "json": {
            "id": flow_id or TEST_FLOW_ID,
            "flow_version_id": TEST_FLOW_VERSION_ID,
            "image": TEST_IMAGE,
            "labels": FLOW_LABELS,
        },
        "status": status or 201,
    }


def REGISTER_FLOW_FAILURE_RESPONSE(status: int) -> Dict[str, Any]:
    return {
        "method": responses.PUT,
        "url": f"{os.environ['BASE_URL']}/api/prefect_cloud/flows",
        "status": status,
    }


# ----------------------------------- #
# /api/prefect_cloud/flows/{id}/store #
# ----------------------------------- #
def BUILD_STORAGE_RESPONSE(
    flow_id: str = TEST_FLOW_ID, flow_version_id: str = TEST_FLOW_VERSION_ID
) -> Dict[str, Any]:
    return {
        "method": responses.POST,
        "url": (
            f"{os.environ['BASE_URL']}/api/prefect_cloud/flows"
            f"/{flow_id}/{flow_version_id}/content"
        ),
        "status": 201,
    }


def BUILD_STORAGE_FAILURE_RESPONSE(
    status: int, flow_id: str = TEST_FLOW_ID, flow_version_id: str = TEST_FLOW_VERSION_ID
) -> Dict[str, Any]:
    return {
        "method": responses.POST,
        "url": (
            f"{os.environ['BASE_URL']}/api/prefect_cloud/flows"
            f"/{flow_id}/{flow_version_id}/content"
        ),
        "status": status,
    }


# ------------------------------------------ #
# /api/prefect_cloud/flows/{id}/run_job_spec #
# ------------------------------------------ #
def REGISTER_RUN_JOB_SPEC_RESPONSE(status: int, flow_id: str = TEST_FLOW_ID) -> Dict[str, Any]:
    run_job_spec_file = os.path.join(os.path.dirname(__file__), "run-job-spec.yaml")
    with open(run_job_spec_file, "r") as file:
        run_job_spec = yaml.load(file, Loader=yaml.RoundTripLoader)

    base_url = os.environ["BASE_URL"]
    return {
        "method": responses.GET,
        "url": f"{base_url}/api/prefect_cloud/flows/{flow_id}/run_job_spec",
        "json": run_job_spec,
        "status": status,
    }


# ----------------- #
# /api/info/servers #
# ------------------#
def SERVER_SIZES_RESPONSE(status: int) -> Dict[str, Any]:
    return {
        "method": responses.GET,
        "url": f"{os.environ['BASE_URL']}/api/info/servers",
        "status": status,
        "json": {
            "sizes": {
                "medium": "medium - 2 cores - 4 GB RAM",
                "8xlarge": "8XLarge - 32 cores - 256 GB RAM",
            }
        },
    }


class MockClient:
    def __init__(self):
        self._active_tenant_id = "543c5453-0a47-496a-9c61-a6765acef352"
        pass


def test_initialize():

    project_name = "some-pr0ject"
    integration = prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=project_name)
    assert getattr(integration, "SATURN_TOKEN", None) is None
    assert integration._session.headers["Authorization"] == f"token {os.environ['SATURN_TOKEN']}"
    assert integration.prefect_cloud_project_name == project_name
    assert integration._saturn_image is None
    assert integration._saturn_flow_id is None
    assert integration._saturn_flow_version_id is None


def test_initialize_fails_on_extra_trailing_slash_in_base_url(monkeypatch):
    monkeypatch.setenv("BASE_URL", "http://abc/")
    with raises(RuntimeError, match=prefect_saturn.Errors.BASE_URL_NO_SLASH):
        prefect_saturn.PrefectCloudIntegration("x")


def test_initialize_raises_error_on_missing_saturn_token(monkeypatch):
    monkeypatch.delenv("SATURN_TOKEN")
    with raises(RuntimeError, match=prefect_saturn.Errors.missing_env_var("SATURN_TOKEN")):
        prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME)


def test_initialize_raises_error_on_missing_base_url(monkeypatch):
    monkeypatch.delenv("BASE_URL")
    with raises(RuntimeError, match=prefect_saturn.Errors.missing_env_var("BASE_URL")):
        prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME)


def test_initialize_raises_error_on_accessing_properties_if_not_registered():
    integration = prefect_saturn.PrefectCloudIntegration(
        prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
    )
    with raises(RuntimeError, match=prefect_saturn.Errors.NOT_REGISTERED):
        integration.flow_id
    with raises(RuntimeError, match=prefect_saturn.Errors.NOT_REGISTERED):
        integration.flow_version_id
    with raises(RuntimeError, match=prefect_saturn.Errors.NOT_REGISTERED):
        integration.image


def test_hash_flow():

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
        flow.storage = Webhook(
            build_request_kwargs={},
            build_request_http_method="POST",
            get_flow_request_kwargs={},
            get_flow_request_http_method="GET",
        )
        assert flow_hash == integration._hash_flow(flow)

        # should not be impacted by environment of run_config
        if RUN_CONFIG_AVAILABLE:
            flow.run_config = KubernetesRun()
        else:
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


def test_hash_flow_hash_changes_if_tenant_id_changes():

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
def test_get_storage():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**BUILD_STORAGE_RESPONSE())
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        integration.register_flow_with_saturn(flow=flow)

        storage = integration._get_storage()
        assert isinstance(storage, Webhook)
        assert storage.stored_as_script is False
        assert storage.build_request_http_method == "POST"
        assert storage.get_flow_request_http_method == "GET"
        assert storage.build_request_kwargs["headers"]["Content-Type"] == "application/octet-stream"
        assert storage.get_flow_request_kwargs["headers"]["Accept"] == "application/octet-stream"


def test_get_storage_fails_if_flow_not_registerd():
    integration = prefect_saturn.PrefectCloudIntegration(
        prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
    )
    with raises(RuntimeError, match=prefect_saturn.Errors.NOT_REGISTERED):
        integration._get_storage()


@responses.activate
def test_store_flow_fails_if_flow_not_registered():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**BUILD_STORAGE_FAILURE_RESPONSE(404))
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        flow = integration.register_flow_with_saturn(flow=flow)
        with raises(HTTPError, match="Not Found for url"):
            flow.storage.add_flow(flow)
            flow.storage.build()


@responses.activate
def test_store_flow_fails_if_validation_fails():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**BUILD_STORAGE_FAILURE_RESPONSE(400))
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        flow = integration.register_flow_with_saturn(flow=flow)
        with raises(HTTPError, match="Client Error"):
            flow.storage.add_flow(flow)
            flow.storage.build()


@responses.activate
def test_get_environment():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**BUILD_STORAGE_RESPONSE())
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        integration.register_flow_with_saturn(flow=flow)

        environment = integration._get_environment(cluster_kwargs={"n_workers": 3}, adapt_kwargs={})
        assert isinstance(environment, KubernetesJobEnvironment)
        assert environment.unique_job_name is True
        env_args = environment._job_spec["spec"]["template"]["spec"]["containers"][0]["args"]
        assert len(env_args) == 1
        assert env_args[0].startswith("source /home/jovyan/.saturn/start_wrapper.sh;")
        env_cmd = environment._job_spec["spec"]["template"]["spec"]["containers"][0]["command"]
        assert env_cmd == ["/bin/bash", "-ec"]
        assert environment.metadata["image"] == integration._saturn_image
        assert len(environment.labels) == len(FLOW_LABELS)
        for label in FLOW_LABELS:
            assert label in environment.labels


@responses.activate
def test_get_environment_dask_kwargs():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**BUILD_STORAGE_RESPONSE())
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        flow = integration.register_flow_with_saturn(
            flow=flow,
            dask_cluster_kwargs={"n_workers": 8, "autoclose": True},
            dask_adapt_kwargs={"minimum": 3, "maximum": 3},
        )

        assert isinstance(flow.environment, KubernetesJobEnvironment)
        assert isinstance(flow.environment.executor, DaskExecutor)
        assert flow.environment.executor.cluster_kwargs == {"n_workers": 8, "autoclose": True}
        assert flow.environment.executor.adapt_kwargs == {"minimum": 3, "maximum": 3}


@responses.activate
def test_get_environment_dask_adaptive_scaling_and_autoclosing_off_by_default():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**BUILD_STORAGE_RESPONSE())
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        flow = integration.register_flow_with_saturn(flow=flow)

        assert isinstance(flow.environment, KubernetesJobEnvironment)
        assert isinstance(flow.environment.executor, DaskExecutor)
        assert flow.environment.executor.cluster_kwargs == {"n_workers": 1, "autoclose": False}
        assert flow.environment.executor.adapt_kwargs == {}


@responses.activate
def test_get_environment_dask_kwargs_respects_empty_dict():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**REGISTER_FLOW_RESPONSE())
        responses.add(**BUILD_STORAGE_RESPONSE())
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME,
        )
        flow = TEST_FLOW.copy()
        flow = integration.register_flow_with_saturn(
            flow=flow, dask_cluster_kwargs={}, dask_adapt_kwargs={}
        )

        assert isinstance(flow.environment, KubernetesJobEnvironment)
        assert isinstance(flow.environment.executor, DaskExecutor)
        assert flow.environment.executor.cluster_kwargs == {}
        assert flow.environment.executor.adapt_kwargs == {}


def test_get_environment_fails_if_flow_not_registered():
    integration = prefect_saturn.PrefectCloudIntegration(
        prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
    )
    with raises(RuntimeError, match=prefect_saturn.Errors.NOT_REGISTERED):
        integration._get_environment(cluster_kwargs={}, adapt_kwargs={})


@responses.activate
def test_add_environment_fails_if_id_not_recognized():
    with patch("prefect_saturn.core.Client", new=MockClient):
        responses.add(**REGISTER_FLOW_RESPONSE(flow_id=45))
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(404, flow_id=45))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )
        flow = TEST_FLOW.copy()
        integration._set_flow_metadata(flow=flow, instance_size=None)

        with raises(HTTPError, match="404 Client Error"):
            integration._get_environment(cluster_kwargs={}, adapt_kwargs={})


@responses.activate
def test_register_flow_with_saturn_raises_error_on_failure():
    with patch("prefect_saturn.core.Client", new=MockClient):

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
def test_register_flow_with_saturn_does_everything():
    with patch("prefect_saturn.core.Client", new=MockClient):
        test_flow_id = str(random.randint(1, 500))
        responses.add(**REGISTER_FLOW_RESPONSE(flow_id=test_flow_id))
        responses.add(**BUILD_STORAGE_RESPONSE(flow_id=test_flow_id))
        responses.add(**REGISTER_RUN_JOB_SPEC_RESPONSE(200, flow_id=test_flow_id))

        integration = prefect_saturn.PrefectCloudIntegration(
            prefect_cloud_project_name=TEST_PREFECT_PROJECT_NAME
        )

        # no details are stored just on initialization
        assert integration._saturn_flow_id is None
        assert integration._saturn_flow_version_id is None
        assert integration._saturn_image is None

        flow = TEST_FLOW.copy()
        assert flow.storage is None

        flow = integration.register_flow_with_saturn(flow=flow, instance_size="large")
        assert integration._saturn_flow_id == test_flow_id
        assert integration._saturn_flow_version_id == TEST_FLOW_VERSION_ID
        assert integration._saturn_image == TEST_IMAGE
        assert isinstance(flow.storage, Webhook)
        assert isinstance(flow.environment, KubernetesJobEnvironment)
        assert isinstance(flow.environment.executor, DaskExecutor)


@responses.activate
def test_describe_sizes_successful():
    responses.add(**SERVER_SIZES_RESPONSE(status=200))
    result = prefect_saturn.describe_sizes()
    assert isinstance(result, dict)
    assert result["medium"] == "medium - 2 cores - 4 GB RAM"


@responses.activate
def test_describe_sizes_raises_informative_error_on_failure():
    with raises(HTTPError, match="Unauthorized"):
        responses.add(**SERVER_SIZES_RESPONSE(status=401))
        prefect_saturn.describe_sizes()

    failure_response = SERVER_SIZES_RESPONSE(500)
    failure_response["method_or_response"] = failure_response.pop("method")
    responses.replace(**failure_response)
    with raises(HTTPError, match="Server Error"):
        prefect_saturn.describe_sizes()
