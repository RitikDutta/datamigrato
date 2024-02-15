def pytest_addoption(parser):
    parser.addoption("--client_url", action="store", default="default name")
