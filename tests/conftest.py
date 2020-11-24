import os

from pytest import Session


def pytest_sessionstart(session: Session):
    # Set required environment variables
    os.environ["SATURN_TOKEN"] = "placeholder-token"
    os.environ["BASE_URL"] = "http://placeholder-url"
