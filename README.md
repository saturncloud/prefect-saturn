# prefect-saturn

![GitHub Actions](https://github.com/saturncloud/prefect-saturn/workflows/GitHub%20Actions/badge.svg) [![PyPI Version](https://img.shields.io/pypi/v/prefect-saturn.svg)](https://pypi.org/project/prefect-saturn)

`prefect-saturn` is a Python package that makes it easy to run [Prefect Cloud](https://www.prefect.io/cloud/) flows on a Dask cluster with [Saturn Cloud](https://www.saturncloud.io/).

## Getting Started

```python
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

flow.register(
    project_name=project_name,
    labels=["saturn-cloud"]
)
```

## Installation

`prefect-saturn` is available on PyPi.

```shell
pip install prefect-saturn
```

`prefect-saturn` can be installed directly from GitHub

```shell
pip install git+https://github.com/saturncloud/prefect-saturn.git@main
```

## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for documentation on how to test and contribute to `prefect-saturn`.
