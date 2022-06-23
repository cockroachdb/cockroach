modvendor
=========

Simple tool to copy additional module files into a local ./vendor folder. This
tool should be run after `go mod vendor`.

`go get -u github.com/goware/modvendor`

## Usage

```
$ GO111MODULE=on go mod vendor
$ modvendor -copy="**/*.c **/*.h **/*.proto" -v
```

If you have additional directories that you wish to copy which are not specified
under `./vendor/modules.txt`, use the `-include` flag with multiple values separated
by commas, e.g.:

```
$ GO111MODULE=on go mod vendor
$ modvendor -copy="**/*.c **/*.h **/*.proto" -v -include="github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/api,github.com/grpc-ecosystem/grpc-gateway/third_party/googleapis/google/rpc,github.com/prometheus/client_model"
```

## LICENSE

MIT
