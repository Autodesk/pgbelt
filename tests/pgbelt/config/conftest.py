from os import environ


def pytest_collectstart():
    environ["CI"] = "true"
