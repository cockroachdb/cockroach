![Build status](https://badge.buildkite.com/c11240e6e9519111f2380dfcf5fcb49e69fd5b2326c11a3059.svg?branch=master)

# A remote build cache for [Bazel](https://bazel.build)

bazel-remote is a HTTP/1.1 and gRPC server that is intended to be used as a remote build cache for
[Bazel](https://bazel.build). The cache contents are stored in a directory on disk. One can specify a maximum cache
size and bazel-remote will automatically enforce this limit and clean the cache by deleting files based on their
last access time. The cache supports HTTP basic authentication with usernames and passwords being specified by a
`.htpasswd` file, and also mutual TLS authentication.

**Project status**: bazel-remote has been serving TBs of cache artifacts per day since April 2018, both on
commodity hardware and AWS servers. Outgoing bandwidth can exceed 15 Gbit/s on the right AWS instance type.

## HTTP/1.1 REST API

Cache entries are set and retrieved by key, and there are two types of keys that can be used:
1. Content addressed storage (CAS), where the key is the lowercase SHA256 hash of the stored value.
   The REST API for these entries is: `/cas/<key>` or with an optional but ignored instance name:
   `/<instance>/cas/<key>`.
2. Action cache, where the key is an arbitrary 64 character lowercase hexadecimal string.
   Bazel uses the SHA256 hash of an action as the key, to store the metadata created by the action.
   The REST API for these entries is: `/ac/<key>` or with an optional instance name: `/<instance>/ac/<key>`.

Values are stored via HTTP PUT requests, and retrieved via GET requests.
HEAD requests can be used to confirm whether a key exists or not.

If the `--enable_ac_key_instance_mangling` flag is specified and the instance
name is not empty, then action cache keys are hashed along with the instance
name to produce the action cache lookup key. Since the URL path is processed
with Go's [path.Clean](https://golang.org/pkg/path/#Clean) function before
extracting the instance name, clients should avoid using repeated slashes,
`./` and `../` in the URL.

Values stored in the action cache are validated as an ActionResult protobuf message as per the
[Bazel Remote Execution API v2](https://github.com/bazelbuild/remote-apis/blob/master/build/bazel/remote/execution/v2/remote_execution.proto)
unless validation is disabled by configuration. The HTTP server also supports reading and writing JSON
encoded protobuf ActionResult messages to the action cache by using HTTP headers `Accept: application/json`
for GET requests and `Content-type: application/json` for PUT requests.

### Useful endpoints

**/status**

Returns the cache status/info.
```
$ curl http://localhost:8080/status
{
 "CurrSize": 414081715503,
 "ReservedSize": 876400,
 "MaxSize": 8589934592000,
 "NumFiles": 621413,
 "ServerTime": 1588329927,
 "GitCommit": "940d540d3a7f17939c3df0038530122eabef2f19",
 "NumGoroutines": 12
}
```

**/cas/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855**

The empty CAS blob is always available, even if the cache is empty. This can be used to test that
a bazel-remote instance is running and accepting requests.
```
$ curl --head --fail http://localhost:8080/cas/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855
HTTP/1.1 200 OK
Content-Length: 0
Date: Fri, 01 May 2020 10:42:06 GMT
```

### Prometheus Metrics

To query endpoint metrics see [github.com/slok/go-http-metrics's query examples](https://github.com/slok/go-http-metrics#prometheus-query-examples).

## gRPC API

bazel-remote also supports the ActionCache, ContentAddressableStorage and Capabilities services in the
[Bazel Remote Execution API v2](https://github.com/bazelbuild/remote-apis/blob/master/build/bazel/remote/execution/v2/remote_execution.proto),
and the corresponding parts of the [Byte Stream API](https://github.com/googleapis/googleapis/blob/master/google/bytestream/bytestream.proto).

When using the `--enable_ac_key_instance_mangling` feature, clients are
advised to avoid repeated slashes, `../` and `./` strings in the instance
name, for consistency with the HTTP interface.

### Prometheus Metrics

To query endpoint metrics see [github.com/grpc-ecosystem/go-grpc-prometheus's metrics documentation](https://github.com/grpc-ecosystem/go-grpc-prometheus#metrics).

### Experimental Remote Asset API Support

There is (very) experimental support for a subset of the Fetch service in the
[Remote Asset API](https://github.com/bazelbuild/remote-apis/blob/master/build/bazel/remote/asset/v1/remote_asset.proto)
which can be enabled with the `--experimental_remote_asset_api` flag.

To use this with Bazel, specify
[--experimental_remote_downloader=grpc://replace-with-your.host:port](https://docs.bazel.build/versions/master/command-line-reference.html#flag--experimental_remote_downloader).

## Usage

If a YAML configuration file is specified by the `--config_file` command line
flag or `BAZEL_REMOTE_CONFIG_FILE` environment variable, then other command
line flags and environment variables are ignored. Otherwise, the flags and
environment variables listed in the help text below can be specified (flags
override the corresponding environment variables).

### Command line flags

```
$ ./bazel-remote --help
NAME:
   bazel-remote - A remote build cache for Bazel

USAGE:
   bazel-remote [global options] [arguments...]

DESCRIPTION:
   A remote build cache for Bazel.

GLOBAL OPTIONS:
   --config_file value                Path to a YAML configuration file. If this flag is specified then all other flags are ignored. [$BAZEL_REMOTE_CONFIG_FILE]
   --dir value                        Directory path where to store the cache contents. This flag is required. [$BAZEL_REMOTE_DIR]
   --max_size value                   The maximum size of the remote cache in GiB. This flag is required. (default: -1) [$BAZEL_REMOTE_MAX_SIZE]
   --host value                       Address to listen on. Listens on all network interfaces by default. [$BAZEL_REMOTE_HOST]
   --port value                       The port the HTTP server listens on. (default: 8080) [$BAZEL_REMOTE_PORT]
   --grpc_port value                  The port the gRPC server listens on. Set to 0 to disable. (default: 9092) [$BAZEL_REMOTE_GRPC_PORT]
   --profile_host value               A host address to listen on for profiling, if enabled by a valid --profile_port setting. (default: "127.0.0.1") [$BAZEL_REMOTE_PROFILE_HOST]
   --profile_port value               If a positive integer, serve /debug/pprof/* URLs from http://profile_host:profile_port. (default: 0, ie profiling disabled) [$BAZEL_REMOTE_PROFILE_PORT]
   --http_read_timeout value          The HTTP read timeout for a client request in seconds (does not apply to the proxy backends or the profiling endpoint) (default: 0s, ie disabled) [$BAZEL_REMOTE_HTTP_READ_TIMEOUT]
   --http_write_timeout value         The HTTP write timeout for a server response in seconds (does not apply to the proxy backends or the profiling endpoint) (default: 0s, ie disabled) [$BAZEL_REMOTE_HTTP_WRITE_TIMEOUT]
   --htpasswd_file value              Path to a .htpasswd file. This flag is optional. Please read https://httpd.apache.org/docs/2.4/programs/htpasswd.html. [$BAZEL_REMOTE_HTPASSWD_FILE]
   --tls_enabled                      This flag has been deprecated. Specify tls_cert_file and tls_key_file instead. (default: false) [$BAZEL_REMOTE_TLS_ENABLED]
   --tls_ca_file value                Optional. Enables mTLS (authenticating client certificates), should be the certificate authority that signed the client certificates. [$BAZEL_REMOTE_TLS_CA_FILE]
   --tls_cert_file value              Path to a pem encoded certificate file. [$BAZEL_REMOTE_TLS_CERT_FILE]
   --tls_key_file value               Path to a pem encoded key file. [$BAZEL_REMOTE_TLS_KEY_FILE]
   --idle_timeout value               The maximum period of having received no request after which the server will shut itself down. (default: 0s, ie disabled) [$BAZEL_REMOTE_IDLE_TIMEOUT]
   --max_queued_uploads value         When using proxy backends, sets the maximum number of objects in queue for upload. If the queue is full, uploads will be skipped until the queue has space again. (default: 1000000) [$BAZEL_REMOTE_MAX_QUEUED_UPLOADS]
   --num_uploaders value              When using proxy backends, sets the number of Goroutines to process parallel uploads to backend. (default: 100) [$BAZEL_REMOTE_NUM_UPLOADERS]
   --s3.endpoint value                The S3/minio endpoint to use when using S3 proxy backend. [$BAZEL_REMOTE_S3_ENDPOINT]
   --s3.bucket value                  The S3/minio bucket to use when using S3 proxy backend. [$BAZEL_REMOTE_S3_BUCKET]
   --s3.prefix value                  The S3/minio object prefix to use when using S3 proxy backend. [$BAZEL_REMOTE_S3_PREFIX]
   --s3.access_key_id value           The S3/minio access key to use when using S3 proxy backend. [$BAZEL_REMOTE_S3_ACCESS_KEY_ID]
   --s3.secret_access_key value       The S3/minio secret access key to use when using S3 proxy backend. [$BAZEL_REMOTE_S3_SECRET_ACCESS_KEY]
   --s3.disable_ssl                   Whether to disable TLS/SSL when using the S3 proxy backend. (default: false, ie enable TLS/SSL) [$BAZEL_REMOTE_S3_DISABLE_SSL]
   --s3.iam_role_endpoint value       Endpoint for using IAM security credentials. By default it will look for credentials in the standard locations for the AWS platform. [$BAZEL_REMOTE_S3_IAM_ROLE_ENDPOINT]
   --s3.region value                  The AWS region. Required when not specifying S3/minio access keys. [$BAZEL_REMOTE_S3_REGION]
   --s3.key_version value             Set to 1 for the legacy flat key format, or 2 for the newer format that reduces the impact of S3 rate limits. (default: 1) [$BAZEL_REMOTE_S3_KEY_VERSION]
   --disable_http_ac_validation       Whether to disable ActionResult validation for HTTP requests. (default: false, ie enable validation) [$BAZEL_REMOTE_DISABLE_HTTP_AC_VALIDATION]
   --disable_grpc_ac_deps_check       Whether to disable ActionResult dependency checks for gRPC GetActionResult requests. (default: false, ie enable ActionCache dependency checks) [$BAZEL_REMOTE_DISABLE_GRPS_AC_DEPS_CHECK]
   --enable_ac_key_instance_mangling  Whether to enable mangling ActionCache keys with non-empty instance names. (default: false, ie disable mangling) [$BAZEL_REMOTE_ENABLE_AC_KEY_INSTANCE_MANGLING]
   --enable_endpoint_metrics          Whether to enable metrics for each HTTP/gRPC endpoint. (default: false, ie disable metrics) [$BAZEL_REMOTE_ENABLE_ENDPOINT_METRICS]
   --experimental_remote_asset_api    Whether to enable the experimental remote asset API implementation. (default: false, ie disable remote asset API) [$BAZEL_REMOTE_EXPERIMENTAL_REMOTE_ASSET_API]
   --help, -h                         show help (default: false)
```

### Example configuration file

```yaml
# These two are the only required options:
dir: path/to/cache-dir
max_size: 100

host: localhost
# The port to use for HTTP/HTTPS:
#port: 8080
# The port to use for gRPC:
#grpc_port: 9092

# If profile_port is specified, then serve /debug/pprof/* URLs here:
#profile_host: 127.0.0.1
#profile_port: 7070

# HTTP read/write timeouts. Note that these do not apply to the proxy
# backends or the profiling endpoint. Reasonable values might be twice
# the length of time that you expect a client to read/write the largest
# likely blob. Units can be one of: "s", "m", "h".
#http_read_timeout: 15s
#http_write_timeout: 20s

# Specify a certificate if you want to use HTTPS:
#tls_cert_file: path/to/tls.cert
#tls_key_file:  path/to/tls.key
# If you want to use mutual TLS with client certificates:
#tls_ca_file: path/to/ca/cert.pem

# Alternatively, you can use simple authentication:
#htpasswd_file: path/to/.htpasswd

# If specified, bazel-remote should exit after being idle
# for this long. Time units can be one of: "s", "m", "h".
#idle_timeout: 45s

# If set to true, do not validate that ActionCache
# items are valid ActionResult protobuf messages.
#disable_http_ac_validation: false

# If set to true, do not check that CAS items referred
# to by ActionResult messages are in the cache.
#disable_grpc_ac_deps_check: false

# If set to true, enable metrics for each HTTP/gRPC endpoint.
#enable_endpoint_metrics: false

# Specify a custom list of histogram buckets for endpoint request duration metrics
#endpoint_metrics_duration_buckets: [.5, 1, 2.5, 5, 10, 20, 40, 80, 160, 320]

# At most one of the proxy backends can be selected:
#
# If this is 0, proxy backends won't upload blobs.
#num_uploaders: 100
# The maximum number of proxy uploads to queue, before dropping uploads.
#max_queued_uploads: 1000000
#
#gcs_proxy:
#  bucket: gcs-bucket
#  use_default_credentials: false
#  json_credentials_file: path/to/creds.json
#
#s3_proxy:
#  endpoint: minio.example.com:9000
#  bucket: test-bucket
#  prefix: test-prefix
#  access_key_id: EXAMPLE_ACCESS_KEY
#  secret_access_key: EXAMPLE_SECRET_KEY
#  disable_ssl: true
#  key_version: 2
#
# Provide either access_key_id/secret_access_key, or iam_role_endpoint/region.
# iam_role_endpoint can also be left empty, and figured out automatically.
#  iam_role_endpoint: http://169.254.169.254
#  region: us-east-1
#
#http_proxy:
#  url: https://remote-cache.com:8080/cache

# If set to a valid port number, then serve /debug/pprof/* URLs here:
#profile_port: 7070
# IP address to use, if profiling is enabled:
#profile_host: 127.0.0.1

# If true, enable experimental remote asset API support:
#experimental_remote_asset_api: true
```

## Docker

### Prebuilt Image

We publish docker images to [DockerHub](https://hub.docker.com/r/buchgr/bazel-remote-cache/)
that you can use with `docker run`. The following commands will start bazel-remote with uid
and gid `1000` on port `9090` for HTTP and `9092` for gRPC, with the default maximum cache
size of `5 GiB`.

```bash
$ docker pull buchgr/bazel-remote-cache
$ docker run -u 1000:1000 -v /path/to/cache/dir:/data \
	-p 9090:8080 -p 9092:9092 buchgr/bazel-remote-cache
```

Note that you will need to change `/path/to/cache/dir` to a valid directory that is readable
and writable by the specified user (or by uid/gid `65532` if no user was specified).

If you want the docker container to run in the background pass the `-d` flag right after `docker run`.

You can adjust the maximum cache size by appending `--max_size=N`, where N is
the maximum size in Gibibytes.

### Kubernetes note

Don't name your deployment `bazel-remote`!

Kubernetes sets some environment variables based on this name, which conflict
with the `BAZEL_REMOTE_*` environment variables that bazel-remote tries to
parse.

### Build your own

The command below will build a docker image from source and install it into your local docker registry.

```bash
$ bazel run :bazel-remote-image
```

### ARM Support

Bazel remote cache server can be run on an ARM architecture (i.e.: on a Raspberry Pi).

To build for ARM, use:

```bash
$ bazel run :bazel-remote-image-arm64
```

## Build a standalone Linux binary

```bash
$ bazel build :bazel-remote
```

### Authentication

In order to pass a `.htpasswd` and/or server key file(s) to the cache
inside a docker container, you first need to mount the file in the
container and pass the path to the cache. The example below also
configures TLS which is technically optional but highly recommended
in order to not send passwords in plain text.

```bash
$ docker run -v /path/to/cache/dir:/data \
	-v /path/to/htpasswd:/etc/bazel-remote/htpasswd \
	-v /path/to/server_cert:/etc/bazel-remote/server_cert \
	-v /path/to/server_key:/etc/bazel-remote/server_key \
	-p 9090:8080 -p 9092:9092 buchgr/bazel-remote-cache \
	--tls_cert_file=/etc/bazel-remote/server_cert \
	--tls_key_file=/etc/bazel-remote/server_key \
	--htpasswd_file /etc/bazel-remote/htpasswd --max_size=5
```

If you prefer not using `.htpasswd` files it is also possible to
authenticate with mTLS (also can be known as "authenticating client
certificates"). You can do this by passing in the the cert/key the
server should use, as well as the certificate authority that signed
the client certificates:

```bash
$ docker run -v /path/to/cache/dir:/data \
	-v /path/to/certificate_authority:/etc/bazel-remote/ca_cert \
	-v /path/to/server_cert:/etc/bazel-remote/server_cert \
	-v /path/to/server_key:/etc/bazel-remote/server_key \
	-p 9090:8080 -p 9092:9092 buchgr/bazel-remote-cache \
	--tls_ca_file=/etc/bazel-remote/ca_cert \
	--tls_cert_file=/etc/bazel-remote/server_cert \
	--tls_key_file=/etc/bazel-remote/server_key \
	--max_size=5
```

### Profiling

To enable pprof profiling, specify a port with `--profile_port`.

If running inside docker, you will also need to set `--profile_host` to a
value other than `127.0.0.1` (`--profile_host=` with an empty value should
work) and add a `-p` mapping to the docker run commandline for the port.

See [Profiling Go programs with pprof](https://jvns.ca/blog/2017/09/24/profiling-go-with-pprof/)
for more details.

## Configuring Bazel

To make bazel use remote cache, use the following flag:
`--remote_cache=http://replace-with-your.host:port`. You can also use the
following protocols instead of http: https, grpc or grpcs (depending on your
bazel-remote configuration).

Basic username/password authentication can be added like so:

`--remote_cache=http://user:pass@replace-with-your.host:port`

To avoid leaking your password in log files, you can place this flag in a
[user-specific (and .gitignore'd) bazelrc file](https://docs.bazel.build/versions/master/best-practices.html#bazelrc).

To use mutual TLS with bazel, use a `grpcs` URL for the `--remote_cache`
argument, and add the following flags:
```bash
	--tls_certificate=path/to/ca.cert
	--tls_client_certificate=path/to/client/cert.cert
	--tls_client_key=path/to/client/cert.key
```

For more details, see Bazel's [remote
caching](https://docs.bazel.build/versions/master/remote-caching.html#run-bazel-using-the-remote-cache)
documentation.

## AWS S3 note

To avoid per-prefix rate limiting with Amazon S3, you may want to try using
`--s3.key_format=2`, which stores blobs across a larger number of prefixes.
Reference:
[Optimizing Amazon S3 Performance](https://docs.aws.amazon.com/AmazonS3/latest/dev/optimizing-performance.html).
