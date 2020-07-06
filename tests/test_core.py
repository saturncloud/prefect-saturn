import os
import prefect
import prefect_saturn
import random
import responses
import uuid

from prefect import task, Flow
from prefect.engine.executors import DaskExecutor
from prefect.environments import KubernetesJobEnvironment
from prefect.environments.storage import Docker

os.environ["SATURN_TOKEN"] = "placeholder-token"
os.environ["BASE_URL"] = "http://placeholder-url"


@responses.activate
def test_initialize():
    test_image = "test-image:0.1"
    responses.add(
        responses.GET,
        f"{os.environ['BASE_URL']}/api/current_image",
        json={"image": test_image},
        status=200,
    )
    project_name = "some-pr0ject"
    integration = prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=project_name)
    assert getattr(integration, "SATURN_TOKEN", None) is None
    assert integration._session.headers["Authorization"] == f"token {os.environ['SATURN_TOKEN']}"
    assert integration.prefect_cloud_project_name == project_name
    assert integration._saturn_base_image == test_image
    assert integration._saturn_flow_id is None

    # __init__() should add this trailing slash if it's necessary
    assert integration._base_url == os.environ["BASE_URL"] + "/"


@responses.activate
def test_hash_flow():
    @task
    def hello_task():
        logger = prefect.context.get("logger")
        logger.info("hola")

    flow = Flow("plase-work", tasks=[hello_task])

    test_image = "test-image:0.1"
    responses.add(
        responses.GET,
        f"{os.environ['BASE_URL']}/api/current_image",
        json={"image": test_image},
        status=200,
    )
    project_name = "some-pr0ject"
    integration = prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=project_name)

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

    # should change if you add a new task
    @task
    def goodbye_task():
        logger = prefect.context.get("logger")
        logger.info("adios")

    flow.tasks = [hello_task, goodbye_task]
    new_flow_hash = integration._hash_flow(flow)

    assert isinstance(new_flow_hash, str) and len(new_flow_hash) > 0
    assert new_flow_hash != flow_hash


@responses.activate
def test_register_flow_with_saturn():
    @task
    def hello_task():
        logger = prefect.context.get("logger")
        logger.info("hola")

    project_name = "some-pr0ject"
    flow_name = "plz-w0rk"
    flow = Flow(flow_name, tasks=[hello_task])

    # Set up integration
    test_image = "test-image:0.1"
    responses.add(
        responses.GET,
        f"{os.environ['BASE_URL']}/api/current_image",
        json={"image": test_image},
        status=200,
    )
    integration = prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=project_name)

    # register flow
    test_id = random.randint(1, 500)
    responses.add(
        responses.PUT,
        f"{os.environ['BASE_URL']}/api/prefect_cloud/flows",
        json={
            "id": test_id,
            "name": flow_name,
            "prefect_cloud_project_name": project_name,
            "flow_hash": integration._hash_flow(flow),
            "deployment": {"id": random.randint(1, 500)},
        },
    )

    assert integration._saturn_flow_id is None
    integration.register_flow_with_saturn(flow=flow)
    assert integration._saturn_flow_id == test_id


@responses.activate
def test_get_saturn_details():
    # Set up integration
    project_name = "some-pr0ject"
    test_image = "test-image:0.1"
    responses.add(
        responses.GET,
        f"{os.environ['BASE_URL']}/api/current_image",
        json={"image": test_image},
        status=200,
    )
    integration = prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=project_name)

    # mock "registering" the flow
    test_id = random.randint(1, 500)
    integration._saturn_flow_id = test_id

    # response with details for building storage
    test_token = str(uuid.uuid4())
    test_registry = "12345.ecr.aws"
    responses.add(
        responses.GET,
        f"{os.environ['BASE_URL']}/api/prefect_cloud/flows/{test_id}/saturn_details",
        json={
            "registry_url": test_registry,
            "host_aliases": [],
            "deployment_token": test_token,
            "image_name": test_image,
            "environment_variables": {},
        },
    )
    saturn_details = integration.saturn_details
    assert isinstance(saturn_details, dict)
    assert integration._saturn_details["host_aliases"] == []
    assert integration._saturn_details["deployment_token"] == test_token
    assert integration._saturn_details["image_name"] == test_image
    assert integration._saturn_details["registry_url"] == test_registry
    assert integration._saturn_details["environment_variables"] == {}


@responses.activate
def test_build_environment():
    # Set up integration
    project_name = "some-pr0ject"
    test_image = "test-image:0.1"
    responses.add(
        responses.GET,
        f"{os.environ['BASE_URL']}/api/current_image",
        json={"image": test_image},
        status=200,
    )
    integration = prefect_saturn.PrefectCloudIntegration(prefect_cloud_project_name=project_name)

    # mock "registering" the flow
    test_id = random.randint(1, 500)
    integration._saturn_flow_id = test_id

    # mock saturn_details, since they're used by the job environment
    test_token = str(uuid.uuid4())
    test_registry = "12345.ecr.aws"
    responses.add(
        responses.GET,
        f"{os.environ['BASE_URL']}/api/prefect_cloud/flows/{test_id}/saturn_details",
        json={
            "registry_url": test_registry,
            "host_aliases": [],
            "deployment_token": test_token,
            "image_name": test_image,
        },
    )

    @task
    def hello_task():
        logger = prefect.context.get("logger")
        logger.info("hola")

    project_name = "some-pr0ject"
    flow_name = "plz-w0rk"
    flow = Flow(flow_name, tasks=[hello_task])

    flow = integration.add_environment(flow=flow)
    assert isinstance(flow.environment, KubernetesJobEnvironment)
    assert isinstance(flow.environment.executor, DaskExecutor)
