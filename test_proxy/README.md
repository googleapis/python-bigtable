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

You can also run the `proxy_server.py` file directly

```
cd python-bigtable/test_proxy
python proxy_server.py
```

The port can be set by passing in an extra positional argument

```
cd python-bigtable/test_proxy
python proxy_server.py 8080
```

## Run the test cases

If you have not already done so, [install golang](https://go.dev/doc/install), then clone the go test library:

```
git clone https://github.com/googleapis/cloud-bigtable-clients-test.git
```

Navigate to the test directory in `cloud-bigtable-clients-test`, and launch the tests

```
cd cloud-bigtable-clients-test/tests
go test -v -proxy_addr=:9999
```

## Test a released client

You can run the test proxy against a released version of the library with `nox`
by setting the `PROXY_CLIENT_VERSION` environment variable:

```
PROXY_CLIENT_VERSION=3.0.0
nox -s run_proxy
```

if unset, it will default to installing the library from source
