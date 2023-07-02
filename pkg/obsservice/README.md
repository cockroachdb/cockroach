# CockroachDB Observability Service

This directory contains the source code of the CRDB Observability Service - a
service that collects monitoring and observability functionality from CRDB and
serves a web console that exposes the data.

The Obs Service is developed as a library (in the `obslib` package) and a binary
(the `cmd\obsservice` package). The idea is for other binaries to be able to
embed and extend the library (for example we imagine CockroachCloud doing so in
the future).

**Note**: Serving DB Console is no longer a core part of the planned utility of the
Obs Service. However, as the functionality to serve the DB Console is maintained for
now, in case it proves useful down the line.

## Building the Obs Service

Build with

```shell
./dev build obsservice
```

which will include the DB Console UI served on the HTTP port.

You can also build without the UI using:

```shell
./dev build pkg/obsservice/cmd/obsservice
```

which will produce a binary in `./bin/obsservice`.

## Running

Assuming you're already running a local CRDB instance:

```shell
obsservice --otlp-addr=localhost:4317 --http-addr=localhost:8081 --crdb-http-url=http://localhost:8080 --ui-cert=certs/cert.pem --ui-cert-key=certs/key.pem --ca-cert=certs/ca.crt
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
- `--http-addr` is the address on which the DB Console is served. NB: This feature may
  be removed in the future. See note above in [header section](#CockroachDB-Observability-Service)
- `--crdb-http-url` is CRDB's HTTP address. For a multi-node CRDB cluster, this
  can point to a load-balancer. It can be either a HTTP or an HTTPS address,
  depending on whether the CRDB cluster is running as `--insecure`.
- `--ui-cert` and `--ui-cert-key` are the paths to the certificate
  presented by the Obs Service to its HTTPS clients, and the private key
  corresponding to the certificate. If no certificates are configured, the Obs
  Service will not use TLS. Certificates need to be specified if the CRDB
  cluster is not running in `--insecure` mode: i.e. the Obs Service will refuse
  to forward HTTP requests over HTTPS. The reverse is allowed, though: the Obs
  Service can be configured with certificates even if CRDB is running in
  `--insecure`. In this case, the Obs Service will terminate TLS connections and
  forward HTTPS requests over HTTP.

  If configured with certificates, HTTP requests will be redirected to HTTPS.  

  For testing, self-signed certificates can be generated, for example, with the
  [`generate_cert.go`](https://go.dev/src/crypto/tls/generate_cert.go) utility in
  the Golang standard library: `go run ./crypto/tls/generate_cert.go
  --host=localhost`.
- `--ca-cert` is the path to a certificate authority certificate file (perhaps
  one created with `cockroach cert create-ca`). If specified, HTTP requests are
  only proxied to CRDB nodes that present certificates signed by this CA. If not
  specified, the system's CA list is used.

## Functionality

In the current fledgling state, the Obs Service does a couple of things:

1. The Obs Service serves the DB Console.

2. The Obs Service reverse-proxies some HTTP routes to
   CRDB (`/_admin/`, `/_status/`, `/ts/`, `/api/v2/`).

3. The Obs Service exposes the OTLP Logs gRPC service and is able to ingest
   events received through calls to this RPC service. Only insecure gRPC
   connections are supported at the moment. Events are ingested into the
   Obs Service for aggregation and eventual storage. 

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
