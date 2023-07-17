# CBT Python Test Proxy

The CBT test proxy is intended for running confromance tests for Cloug Bigtable Python Client.

## Start test proxy

#### running the proxy with nox

You can launch the test proxy directly using `nox`, which will handle dependency management

```
cd python-bigtable/test_proxy
nox -s run_proxy
```

The port can be configured using the `PROXY_SERVER_PORT` environment variable

```
cd python-bigtable/test_proxy
PROXY_SERVER_PORT=8080
nox -s run_proxy
```

#### running the proxy script manually

You can also run the `test_proxy.py` file directly

```
cd python-bigtable/test_proxy
python test_proxy.py
```

The port can be set by passing in an extra positional argument

```
cd python-bigtable/test_proxy
python test_proxy.py --port 8080
```

## Run the test cases

Prerequisites:
- If you have not already done so, [install golang](https://go.dev/doc/install).
- Before running tests, [launch an instance of the test proxy](#start-test-proxy) 
in a separate shell session, and make note of the port

#### running the test cases with nox

You can trigger the tests directly using `nox`, which will clone the test repo locally if it doesn't exist

```
cd python-bigtable/test_proxy
nox -s conformance_tests
```

The port can be configured using the `PROXY_SERVER_PORT` environment variable

```
cd python-bigtable/test_proxy
PROXY_SERVER_PORT=8080
nox -s conformance_tests
```

#### running the test cases manually

Clone and navigate to the go test library:

```
git clone https://github.com/googleapis/cloud-bigtable-clients-test.git
cd cloud-bigtable-clients-test/tests
```


Launch the tests

```
go test -v -proxy_addr=:50055
```

## Test a released client

You can run the test proxy against a released version of the library with `nox`
by setting the `PROXY_CLIENT_VERSION` environment variable:

```
PROXY_CLIENT_VERSION=3.0.0
nox -s run_proxy
```

if unset, it will default to installing the library from source

## Test the legacy client

You can run the test proxy against the previous `v2` client by running it with the `--legacy-client` flag:

```
python test_proxy.py --legacy-client
```
