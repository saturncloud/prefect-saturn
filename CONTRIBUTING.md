# contributing to prefect-saturn

This document contains details on contributing to `prefect-saturn`.

* [Documentation](#documentation)
* [Installation](#installation)
* [Testing](#testing)
* [Releasing](#releasing)

## Documentation

This repository is the main source of documentation for `prefect-saturn`. If you would like to request a feature, report a bug, or ask a question of the maintainers, please [create an issue](https://github.com/saturncloud/prefect-saturn/issues).

For general documentation on Saturn Cloud and its components, please visit https://docs.saturncloud.io/en/.

## Installation

To develop `prefect-saturn`, install it locally with the following command

```shell
python setup.py develop
```

NOTE: If you have previously `pip install`'d `prefect-saturn`, some steps in this project might not work for you. Run `pip uninstall -y prefect-saturn` to remove any `pip install`'d versions.

## Testing

Every commit to this repository is tested automatically using continuous integration (CI). All CI checks must pass to for a pull request to be accepted.

To try running the tests locally, run the following:

```shell
make test
```

### Linting

`prefect-saturn` uses the following static analysis tools:

* `black`
* `flake8`
* `mypy`

```shell
make format
make lint
```

### Unit tests

Unit tests for the project use `pytest`, `pytest-cov`, and `responses`. All tests are stored in the `tests/` directory.

```shell
make unit-tests
```

The `unit-tests` recipe in `Makefile` includes a minimum code coverage threshold. All pull requests must pass all tests with more than this level of code coverage. The current coverage is reported in the results of `make unit-tests`.

To get a clickable report that shows which lines are not covered by tests, use the following:

```shell
make unit-tests
coverage html
```

Open the file `htmlcov/index.html` to see a coverage report.

### Integration tests

`prefect-saturn`'s unit tests mock out its interactions with the rest of Saturn Cloud. Integration tests that test those interactions contain some sensitive information, and are stored in a private repository.

If you experience issues using `prefect-saturn` and Saturn Cloud, please see [the Saturn documentation](#documentation) or contact us at by following the `Contact Us` navigation at https://www.saturncloud.io/s.

## Releasing

This section describes how to release a new version of `prefect-saturn` to PyPi. It is intended only for maintainers.

1. Open a new pull request which bumps the version in `VERSION`. Merge that PR.
2. [Create a new release](https://github.com/saturncloud/prefect-saturn/releases/new)
    - the tag should be a version number, like `v0.0.1`
    - choose the target from "recent commits", and select the most recent commit on `main`
3. Once this release is created, a GitHub Actions build will automatically start. That build publishes a release to PyPi.
4. When a release has been created, open a new pull request that appens a `.9000` to the end of the version number in `VERSION`. This guarantees that installations from version control will always know that they have a newer version than any available on package managers.
