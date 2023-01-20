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

which will include the DB Console UI served on the HTTP port. This adds the
`"--config=with_ui"` bazel flag that embeds the UI.

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
- `--http-addr` is the address on which the DB Console is served.
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
- `--sink-pgurl` is the connection string for the sink cluster. If the pgurl
  contains a database name, that database will be used; otherwise `obsservice`
  will be used. If not specified, a connection to a local cluster will be
  attempted.

## Functionality

In the current fledgling state, the Obs Service does a couple of things:

1. The Obs Service serves the DB Console, when built with `--config=with_ui`.

2. The Obs Service reverse-proxies some HTTP routes to
   CRDB (`/_admin/`, `/_status/`, `/ts/`, `/api/v2/`).

3. The Obs Service exposes the OTLP Logs gRPC service and is able to ingest
   events received through calls to this RPC service. Only insecure gRPC
   connections are supported at the moment.

4. The Obs Service connects to a sink cluster identified by `--sink-pgurl`. The
   required schema is automatically created using SQL migrations run with
   [goose](https://github.com/pressly/goose). The state of migrations in a sink
   cluster can be inspected through the `observice.obs_admin.migrations` table.
   The ingested events are saved in the sink cluster.

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
persisted in different tables, based on this event type. Events of unrecognized
types are dropped. Currently, a single event type is supported: `"eventlog"`.
The log records carry attributes and a JSON payload representing the event.

The mapping between event types and tables is listed in the table below:

| Event type | Table          | Attributes   |
|------------|----------------|--------------|
| eventlog   | cluster_events | {event_type} |

Each table storing events can have a different schema. It is expected that these
tables store some event fields as columns and otherwise store the raw event in a
JSON column. The values of the different columns can come from the attributes of
the log record (listed in the table above), or from the JSON itself. Virtual or
computed columns can be used to extract data from the JSON directly.

## Licensing

The Observability Service is licensed as Apache 2.0.
