This dir contains the gRPC service definition used by CockroachDB to export data
to the Observability Service.


The `opentelemetry-proto` dir contains protos copied from
[opentelemetry-proto](https://github.com/open-telemetry/opentelemetry-proto).
They can be kept up to date with upstream by running
`./update-opentelemetry-proto.sh`.


We copy the protos that we need from `opentelemetry-proto` into our tree because
vendoring the upstream repo proved too difficult (it's not `go get`-able, some
of the protos in it don't build with gogoproto and also we already vendor
[opentelemetry-proto-go](https://github.com/open-telemetry/opentelemetry-proto-go),
which contains the protoc-compiled protos. This other repo clashes with the
import path the opentelemetry-proto wants.

[opentelemetry-collector](https://github.com/open-telemetry/opentelemetry-collector)
also uses gogoproto, and has a complicated build pipeline for the protos. For
example, they transform all "optional" fields into "oneof" [using
sed](https://github.com/open-telemetry/opentelemetry-collector/blob/feab9491538a882737a5bceb8757b4458a86edd3/proto_patch.sed).
