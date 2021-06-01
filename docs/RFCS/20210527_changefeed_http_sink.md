- Feature Name: HTTP Sink for Changefeeds
- Status: draft
- Start Date: 2021-05-27
- Authors: Ryan Min
- RFC PR: #65927
- Cockroach Issue: #65816

# Summary

Currently, creating a changefeed with an HTTP endpoint for a sink 
(`experimental-http`) uses the cloud storage sink implementation (see 
[sink_cloudstorage.go](../../pkg/ccl/changefeedccl/sink_cloudstorage.go)). Many 
Enterprise customers have expressed an interest in a webhook-type HTTP sink as
opposed to the current implementation, which should be reserved for traditional
cloud storage destinations (i.e. GCS/S3). 

# Technical design

## High-level Design

The HTTP sink implements the existing 
[`Sink`](../../pkg/ccl/changefeedccl/sink.go) interface with the same Sink 
functions `EmitRow()`, `Flush()`, etc. and should offer the same guarantees 
i.e. ordering, at-least-once delivery as other sinks. However, this sink 
differs slightly in that it iss webhook-based and therefore stateless, so the 
endpoints don't have to be health-checked and maintained throughout the 
lifespan of the changefeed. So `Dial()` and `Close()`in the `Sink` interface 
are no-ops in this scenario, at least for the sink itself (channel still needs 
to be closed).

The majority of the http connection logic should be wrapped in an 
[`httputil.Client`](../../pkg/util/httputil/client.go) object (wrapper around Go's 
[`http.Client`](https://golang.org/src/net/http/client.go#L57)) which will
handle most of the TLS and other config setup mentioned below. The HTTP sink
struct might look something like this:

```
type httpSink struct {
  u sinkURL
  client *httputil.Client
	mu struct {
		syncutil.Mutex
		inflight int64
		flushCh chan struct{}
	}
}
```

## Concurrency

Each `POST` to the HTTP endpoint is executed asynchronously through calls to 
`EmitRow()` and `EmitResolvedTimestamp()`. We use a channel here, `flushCh`,
which we will send the http response object once it is received, or the error
(if one occurs). We use a channel as opposed to something like a `WaitGroup` to
receive a response (which may be needed for ack functionality later on) or an 
error.

```
func (s *httpSink) EmitRow (...) error {
  ...
  res, err := s.client.Post(...)
  s.mu.Lock()
  s.mu.inflight++
  s.mu.Unlock()
  go func() {
    s.mu.flushCh <- struct {
      res *http.Response
      err error
    } {
      res: res,
      err: err,
    }
  }
  ...
  return err
}
```

On the `Flush()` side, the the `mu` field can act as a semaphore, and allow
reading from the channel until the sink has been flushed.

```
func (s *httpSink) Flush (...) error {
  ...
  s.mu.Lock()
  defer s.mu.Unlock()
  for s.mu.inflight > 0 {
    select {
      case ctx.Done():
        return ctx.Err()
      case flushed := <- s.mu.flushCh:
        s.mu.inflight--
        // can add additional ack logic here with flushed.res if needed
        log.Infof("Sink returned response: %v", flushed.res)
  }
  ...
}
```

## Configurability

The HTTP sink should be configurable through a few different options and sink
URL query params, a few of which are already supported by other sinks:

* `ca_cert` - supply a custom CA cert for server verification
* `insecure_tls_skip_verify` - used to disable client-side verification of CA
  certs (corresponds to Go's 
  [`tls.Config.InsecureSkipVerify`](https://golang.org/src/crypto/tls/common.go#L624) 
  flag)

These TLS-related query params should only be enabled if the provided sink URL
is prefixed with `https`, throwing an error otherwise.

Some HTTP-based options should also be added for `CREATE CHANGEFEED` statement:

* `http_auth_header` - a user-supplied `Authorization` header for either basic 
  base64-encoded auth or an OAuth token. As this contains credentials, it
  should be redacted when appearing in the `description` column of `SHOW JOBS`/
  `SHOW CHANGEFEED JOBS` etc.
* `http_client_timeout` - specifies a timeout (in seconds) for the client to 
  wait for each request to complete (corresponds to Go's 
  [`http.client.Timeout`](https://golang.org/src/net/http/client.go#L105)) 
  If no option is provided, the default timeout is 3 seconds.

Altogether, the command to create a changefeed to an HTTP sink would look 
something like this:

```
$ root@:26257/defaultdb> CREATE CHANGEFEED FOR foo INTO 
                      -> INTO 'https://fake-endpoint:8080?ca_cert=Zm9v...'
                      -> WITH http_auth_header = 'Bearer <token>',
                      -> WITH http_client_timeout = 5;
```

## Testing Scenarios

1. Normally operating HTTP sink
    * Reception by mock server
    * Sink flushed properly
2. Normally operating HTTP sink with TLS (ca cert)
3. Normally operating HTTP sink with options
   * `http_auth_header` - basic auth & OAuth works for correct credentials,
     fails for wrong credentials
   * `http_client_timeout` - setup mock server to sleep longer than provided 
     timeout, observe error
4. Server provides unknown cert (no `ca_cert` specified by user)
    * Observe TLS error
    * Observe success with `insecure_tls_skip_verify` enabled
5. Improper options (wrong type, options missing, incompatible options),
   continuation of `TestChangefeedErrors()` in `changefeed_test.go`
5. Jobs table properly redacts credentials
    * Create changefeed with `http_auth_header` option set
    * Check that running SHOW JOBS/SHOW CHANGEFEED JOBS has the `description` 
      column redacted

## Rationale and Alternatives

A webhook-type HTTP sink was decided on compared to other approaches due to is
simplicity and ease of implementation. Connections do not have to be maintained
or closed and can remain stateless.

In terms of client authentication, customers expressed interest in basic
authentication via HTTP headers, this method is more common and easier to setup,
however client certs can be added as well (`client_cert` param passed to Kafka).
Can easily add this as an option if needed.

# Unresolved questions/Future additions

* Creating another HTTP sink may cause ambiguity, especially for users of the
  old HTTP cloud storage sink. For now, the current naming convention should 
  work (`http(s)` -> new http sink and `experimental-http(s)` -> old cloud 
  storage sink), however there are plans to 
  [drop](https://github.com/cockroachdb/cockroach/issues/53716) the 
  `experimental-` prefix from the cloud storage sink so perhaps an option 
  should be added to use the old sink when this is done. Goal is to be backward
  compatible but also encourage new users to use the new sink as it's more 
  likely to be in line with their expectations.
  

* Query params in the sink URL may conflict with those expected by the server.
  The Kafka sink throws an error when any unknown query params are detected, 
  but this sink should allow unknown query params since they could be necessary
  on the server side. <br />  Additionally, will the TLS query params mentioned
  above have an effect if propagated to the server? It could be advantageous 
  to convert these params to options eventually, both here and for other sinks 
  for consistency going forward.
  

* Retry functionality is particularly important when dealing with webhooks and
  HTTP endpoints in general. The precedent for changefeeds is to mark every 
  error related to communicating with sinks as retryable, however for HTTP 
  endpoints this behavior may be more nuanced. If customers express a desire to
  configure this retry behaviour, we might want to consider adding configurable 
  options to control what errors to try on (http error codes?), as well as how 
  many times to retry (Can be accomplished with something like 
  `retry.WithMaxAttempts`)


* Do we want to support other formats eventually such as Avro? Currently, only
  JSON is supported for simplicity and coupling with HTTP, however the
  [Avro docs](https://avro.apache.org/docs/current/spec.html) specify that the 
  header `content-Type: avro/binary` should be used.
