# prefect-saturn

![GitHub Actions](https://github.com/saturncloud/prefect-saturn/workflows/GitHub%20Actions/badge.svg) [![PyPI Version](https://img.shields.io/pypi/v/prefect-saturn.svg)](https://pypi.org/project/prefect-saturn)

`prefect-saturn` is a Python package that makes it easy to run [Prefect Cloud](https://www.prefect.io/cloud/) flows on a Dask cluster with [Saturn Cloud](https://www.saturncloud.io/). For a detailed tutorial, see ["Fault-Tolerant Data Pipelines with Prefect Cloud
"](https://www.saturncloud.io/docs/tutorials/prefect-cloud/).

## Installation

`prefect-saturn` is available on PyPi.

```shell
pip install prefect-saturn
```

`prefect-saturn` can be installed directly from GitHub

```shell
pip install git+https://github.com/saturncloud/prefect-saturn.git@main
```

## Getting Started

`prefect-saturn` is intended for use inside a [Saturn Cloud](https://www.saturncloud.io/) environment, such as a Jupyter notebook.

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

### Customize Dask

You can customize the size and behavior of the Dask cluster used to run prefect flows. `prefect_saturn.PrefectCloudIntegration.register_flow_with_saturn()` accepts to arguments to accomplish this:

* `dask_cluster_kwargs`: keyword arguments to pass to the constructor [`dask_saturn.SaturnCluster`](https://github.com/saturncloud/dask-saturn/blob/936c91d54964f578b7224fa9c6fea7ea812e47d7/dask_saturn/core.py#L68-L94).
* `dask_adapt_kwargs`: keyword arguments used to configure ["Adaptive Scaling"](https://docs.dask.org/en/latest/setup/adaptive.html)

For example, the code below tells Saturn that this flow should run on a Dask cluster with 3 xlarge workers, and that prefect should shut down the cluster once the flow run has finished.

```python
flow = integration.register_flow_with_saturn(
    flow=flow,
    dask_cluster_kwargs={
        "n_workers": 3,
        "worker_size": "xlarge",
        "autoclose": True
    }
)

flow.register(
    project_name=project_name,
    labels=["saturn-cloud"]
)
```


## Contributing

See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for documentation on how to test and contribute to `prefect-saturn`.
