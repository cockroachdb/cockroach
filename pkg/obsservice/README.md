# CockroachDB Observability Service

This directory contains the source code of the CRDB Observability Service - a
service that collects monitoring and observability functionality from CRDB and
serves a web console that exposes the data.

The Obs Service is developed as a library (in the `obslib` package) and a binary
(the `cmd\obsservice` package). The idea is for other binaries to be able to
embed and extend the library (for example we imagine CockroachCloud doing so in
the future).

## Building the Obs Service

Build with

```shell
./dev build obsservice
```

which will produce a binary in `./bin/obsservice`.

## Running

Assuming you're already running a local CRDB instance:

```shell
obsservice --otlp-addr=localhost:4317 --http-addr=localhost:8081 --sink-pgurl=postgresql://root@localhost:26257?sslmode=disable
```

- `--otlp-addr` is the address on which the OTLP Logs gRPC service is exposed.
  This address can be passed to CRDB nodes as `--obsservice-addr`. CRDB can also
  be configured to export to the OpenTelemetry Collector, and the collector can
  be configured to route events to the Obs Service with configuration like:
```yaml
exporters:
  otlp:
    endpoint: localhost:4317
    tls:
      insecure: true
```
- `--http-addr` is the address on which any HTTP-related endpoints, such as healthchecks or
  metrics, will be served on.
- `--sink-pgurl` is the address which the obsservice will use to write data to after processing. 
  It will also be used to run migrations found in `pkg/obsservice/obslib/migrations/sqlmigrations`
  on startup. TIP: Use the `sslrootcert` query parameter in the pgurl string to point to a root certificate.
- `--no-db` will prevent the obsservice from attempting to connect to the `--sink-pgurl` at startup.
  This is meant for testing purposes only.

## Building & Pushing a Docker Image

With a local docker instance running, you can build a docker image using the Dockerfile in `pkg/obsservice/cmd/obsservice`,
which can then be pushed & hosted in GCR. Once in GCR, the image is available to be pulled by environments such
as Kubernetes.

To build an `obsservice` docker image locally, make use of the `build-docker.sh` script in
`pkg/obsservice/cmd/obsservice`.

```shell
$ ./pkg/obsservice/cmd/obsservice/build-docker.sh
```

This should create a Docker image locally. You can check this locally.
```shell
 $ docker image ls | grep obsservice
obsservice    latest    13c5d49056cc    12 seconds ago    156MB
```

You can run the image locally via Docker.
```shell
$ docker run --platform=linux/amd64 obsservice:latest
I231113 22:25:02.716505 1 main/main.go:112  [-] 1  Listening for OTLP connections on localhost:4317.
```

You can use environment variables to control things like the OTLP listen address.
```shell
$ docker run -e OTLP_ADDR=0.0.0.0:7171 -e HTTP_ADDR=0.0.0.0:8082 -e SINK_PGURL="postgresql://myuser@myhost:26257?foo=bar" --platform=linux/amd64 obsservice:latest
I231113 22:25:02.716505 1 main/main.go:112  [-] 1  Listening for OTLP connections on 0.0.0.0:7171.
Listening for HTTP requests on http://0.0.0.0:8082.
```

You can find the supported environment variables in `pkg/obsservice/cmd/obsservice/Dockerfile`.

With the image created, you can [push it to GCR](https://cockroachlabs.atlassian.net/wiki/spaces/OI/pages/3249472038/Pushing+an+Antenna+Docker+Image+to+GCR).

NOTE: This script is not meant to last the test of time, but rather get a prototype up and running
quickly. It is not meant for production use. The main problem with it is that it relies on 
`./dev build --cross=linux` for cross compilation of the `obsservice`.
`obsservice` makes use of CGO libraries, meaning it can't be cross-compiled without Docker. Therefore, the
script uses `./dev build --cross=linux` to generate the cross-compiled artifact, copies the artifact into
`pkg/obsservice/cmd/obsservice`, and then the Dockerfile targets that artifact file to generate the Docker
image. It's probably not best practice to use `./dev build --cross` and then copy the artifact elsewhere.

## Functionality

1. The Obs Service exposes the OTLP Logs gRPC service and is able to ingest
   events received through calls to this RPC service. Only insecure gRPC
   connections are supported at the moment. Events are ingested into the
   Obs Service for aggregation and eventual storage. 
2. The Obs Service provides a pluggable framework to define asynchronous event processing
   pipelines. Events can be routed, transformed, validated, enqueued, consumed,
   processed, and stored.

## Event ingestion

The Obs Service ingests events using the
[OTLP](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/otlp.md)
Logs [gRPC
service](https://github.com/open-telemetry/opentelemetry-proto/blob/2119dc9affc4c246f9227fa5411765b81bc91f87/opentelemetry/proto/collector/logs/v1/logs_service.proto).
CRDB exports events using a gRPC client. The events are records are grouped into
[`ResourceLogs`](https://github.com/open-telemetry/opentelemetry-proto/blob/200ccff768a29f8bd431e0a4a463da7ed58be557/opentelemetry/proto/logs/v1/logs.proto)
and,within that, into
[`ScopeLogs`](https://github.com/open-telemetry/opentelemetry-proto/blob/200ccff768a29f8bd431e0a4a463da7ed58be557/opentelemetry/proto/logs/v1/logs.proto#L64).
A resource identifies the cluster/node/tenant that is emitting the respective
events. A scope identifies the type of event; events of different types get
routed to different processing pipelines, based on this event type. Events of 
unrecognized types are dropped. Currently, a single event type is supported: `"eventlog"`.
The log records carry attributes and a JSON payload representing the event.

## Licensing

The Observability Service is licensed as Apache 2.0.
