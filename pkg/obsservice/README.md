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
obsservice --http-addr=localhost:8081 --crdb-http-url=http://localhost:8080 --ui-cert=certs/cert.pem --ui-cert-key=certs/key.pem --ca-cert=certs/ca.crt
```

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

_This is the very beginning of the Obs Service; it doesn't have any
functionality yet. Hopefully we'll keep this up to date as it evolves._

The Obs Service will reverse-proxy all HTTP routes it doesn't handle itself to
CRDB. Currently the Obs Service doesn't handle any routes (other than
`/debug/pprof/*`, which exposes the Obs Service's own pprof endpoints), so all
requests are forwarded.

The Obs Service connects to a sink cluster identified by `--sink-pgurl`. The
required schema is automatically created using SQL migrations run with
[goose](https://github.com/pressly/goose). The state of migrations in a sink
cluster can be inspected through the `observice.obs_admin.migrations` table.

## Licensing

The Observability Service is licensed as Apache 2.0.
