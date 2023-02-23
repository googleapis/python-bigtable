# Test Proxy Readme


## Installation instructions

From within the ```test_proxy``` directory, create and activate a virtual environment.

```
python3 -m venv venv
source venv/bin/activate
```

Install dependencies

```
python3 -m pip install -U pip googleapis-common-protos google-cloud-bigtable
```

Start the server

From within the ```proto_output``` directory:

```
python proxy_server.py
```

It will run the server in port 50055

## Run the proxy test

Run one test, for example:
```
go test -v -run TestReadRows_NoRetry_ClosedStartUnspecifiedEnd -proxy_addr=:50055
```