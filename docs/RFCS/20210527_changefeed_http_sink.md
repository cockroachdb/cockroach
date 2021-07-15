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
Enterprise customers have expressed an interest in a webhook-style HTTP sink
separate from the current implementation. As opposed to the file-based sink,
this sink would allow rows and resolved timestamps to be sent as payloads
from POST requests as they are emitted.

# Technical design

## High-level Overview

From an end-user perspective, the new webhook sink is differentiated from the 
cloud storage sink that supports HTTP endpoints by its scheme prefix. The 
cloud storage sink should have the scheme `experimental-https` (will soon 
be changed to something like `cloudstorage-https`, planned in 
[this ticket](https://github.com/cockroachdb/cockroach/issues/53716), 
and the new sink should be prefixed with `webhook-https`.

In conjunction with our current security efforts, webhook sink will only support
the `https` protocol.

The webhook sink implements the existing
[`Sink`](../../pkg/ccl/changefeedccl/sink.go) interface with the same Sink
functions `EmitRow()`, `Flush()`, etc. and should offer the same guarantees
i.e. ordering, at-least-once delivery as other sinks. However, this sink
differs slightly in that it is stateless, so the endpoints don't have to be
health-checked and maintained throughout the lifespan of the changefeed. So
`Dial()` in the `Sink` interface is a no-op in this scenario.

## Configurability & Authentication

The webhook sink should be configurable through a few different TLS-related 
query params which are already implemented from the Kafka sink, including:

* `ca_cert` - supply a custom base 64-encoded CA cert (raw cert data in PEM form) 
  for server verification
* `insecure_tls_skip_verify` - used to disable client-side verification of CA
  certs (corresponds to Go's
  [`tls.Config.InsecureSkipVerify`](https://golang.org/src/crypto/tls/common.go#L624)
  flag)

To avoid any conflicts with changefeed-unrelated query params, the sink will
remove these params (if they exist) from the query section of the sink URL 
and make requests with any remaining query params intact.

There are also a few different options that should be added for the 
`CREATE CHANGEFEED` statement:

* `webhook_auth_header` - a user-supplied `Authorization` header to attach to the
  request. As this contains credentials, the option value
  should be redacted when appearing in the `description` column of `SHOW JOBS`/
  `SHOW CHANGEFEED JOBS` etc. `i.e. Basic Qwxh... -> Basic redacted` </br>
  * Some user authentication protocols that can be used include Basic (base 64 
    encoded username + password) and OAuth. 
  * Caveat here is that CRDB will not automatically rotate stale credentials,
    so using protocols such as OAuth2 could become troublesome.
* `webhook_client_timeout` - specifies a timeout for the client to wait for 
  each request to complete (corresponds to Go's
  [`http.client.Timeout`](https://golang.org/src/net/http/client.go#L105))
  If no option is provided, the default timeout is 3 seconds. Should be
  provided as an 
  [INTERVAL type](https://www.cockroachlabs.com/docs/v20.2/create-changefeed)
* `topic_in_value` - specifies that the topic name should be embedded in the 
  value. This is set by default for the webhook sink because there is no other 
  way for the topic to be expressed via JSON payloads (Kafka has topics and 
  cloud storage uses the topic name as part of the file name), so customers can
  tell which rows come from which topic (since changefeeds can have multiple 
  topics)
Altogether, the command to create a changefeed to a webhook sink would look
something like this:

```
$ root@:26257/defaultdb> CREATE CHANGEFEED FOR foo 
                      -> INTO 'webhook-https://fake-endpoint:8080?ca_cert=Zm9v...'
                      -> WITH webhook_auth_header = 'Bearer <token>',
                      -> WITH webhook_client_timeout = '5s';
```

## Webhook Client Config

The majority of the HTTP connection logic should be wrapped in an
[`httputil.Client`](../../pkg/util/httputil/client.go) object (wrapper around
Go's [`http.Client`](https://golang.org/src/net/http/client.go#L57)) which will
handle most of the TLS and other config setup mentioned above. 

The client struct also enables keep-alives to reuse connections between requests, 
and the sink will keep the default value of 15 seconds between requests to reuse 
the connection. (configured [here](https://golang.org/src/net/dial.go#L80)) 
</br>  The consequences of enabling keep-alives are that idle connections must 
be closed when the sink is closed. (can be done with 
[Transport.CloseIdleConnections](https://golang.org/pkg/net/http/#Transport.CloseIdleConnections))
Also, each response body must be closed with `res.Body.Close()`.

Batching was considered but ultimately not included for this initial version of
webhook sink, and will likely be added in the future. It will be configured using 
options for max batch size and max batch latency, with the default behavior 
being sending each message at a time (essentially a batch size of 1). Each 
batch would be sent in the form of a JSON array, and the current message format 
uses an array as well (to be compatible with batches in the future).

## Message Formatting

Each emitted row and resolved timestamp should be sent as a JSON payload via
`POST` request to the specified HTTP endpoint. Each payload is contained in a
JSON array with key `payload`. An array is used here to maintain future
formatting compatibility with batched payload messages, which are planned.

JSON payloads will look like this:

```
# Table insert
{
    "payload": [
        {
            "after": {
              "column": "value",
              "rowid": 663854170987364400
            },
            "key": [663854170987364353],
            "topic": "foo"
        }
    ]
}

# Table delete
{
    "payload": [
        {
          "after": null,
          "key": [663535940859199489],
          "topic": "foo"
        }
    ]
}

# Resolved timestamp

{
    "payload": [
        {
          "resolved":
          "1623160834703022000.0000000000"
        }
    ]
}
```

Note: the `key_in_value` option is automatically used here (similar to 
`topic_in_value`) to embed the key in the message, similar to the cloud 
storage sink, since webhook payloads don't have the key/value structure that 
Kafka does.

The above JSON payload should be wrapped in a POST request (in cURL form):

```
curl --location --request POST 'https://fake-endpoint:8080?ca_cert=Zm9v..' \
  --max-time <webhook_client_timeout>
  --connect-timeout <webhook_client_timeout>
  --header 'Content-Type: application/json' \
  --header 'Authorization: <webhook_auth_header>' \
  --data-raw '{
        "payload": [
            {
                "after": {
                  "column": "value",
                  "rowid": 663854170987364400
                },
                "key": [663854170987364353],
                "topic": "foo",
            }
        ]
    }'
```

## Concurrency

Internally, The webhook sink struct might look something like this:

```
type webhookSink struct {
  u sinkURL
  authHeader string
  client *httputil.Client
	mu struct {
		syncutil.Mutex
		inflight   int64
	}
}
```

Each `POST` to the HTTP endpoint is executed asynchronously through calls to 
`EmitRow()` and `EmitResolvedTimestamp()` using `http.NewRequestWithContext`,
which will handle cancellation/shutdown logic (using `Context.withCancel` for
a cancelled changefeed job and `stopper.WithCancelOnQuiesce` for CRDB server
shutdown). The Emit/Flush logic will need to be implemented with per-key and 
global order guarantees in mind.

On the flush side, the number of messages being processed  will be tracked 
using a counter `inflight`. Flush will wait until `inflight` reaches
zero, then return.

## Testing Scenarios

1. Normally operating webhook sink
    * Reception by mock server
    * Sink flushed properly
2. Normally operating webhook sink with options
   * `http_auth_header` - basic auth & OAuth works for correct credentials,
     fails for wrong credentials
   * `http_client_timeout` - setup mock server to sleep longer than provided 
     timeout, observe error
3. Server provides unknown cert (no `ca_cert` specified by user)
    * Observe TLS error
    * Observe success with `insecure_tls_skip_verify` enabled
4. Roachtests (end-to-end and verify ordering guarantees)
5. Improper options (wrong type, options missing, incompatible options),
   continuation of `TestChangefeedErrors()` in `changefeed_test.go`
6. Jobs table properly redacts credentials
    * Create changefeed with `http_auth_header` option set
    * Check that running SHOW JOBS/SHOW CHANGEFEED JOBS has the `description` 
      column redacted
7. Sink properly orders rows (per-key ordering guarantees)

## Rationale and Alternatives

A webhook-style HTTP sink was decided on compared to other approaches due to 
its simplicity and ease of implementation. Connections do not have to be 
maintained or closed and can remain stateless. The sink also becomes more
scalable due to its statelessness.

In terms of client authentication, customers expressed interest in basic
authentication via HTTP headers, this method is more common and easier to setup,
however client certs can be added as well (`client_cert` param passed to Kafka).
Can easily add this as an option if needed.

# Unresolved questions/Future additions

* Query params in the sink URL may conflict with those expected by the server, 
  but only in the edge case where the server hosting the provided HTTP sink
  endpoint happens to also support a query param listed in this doc (`ca_cert`
  or `insecure_tls_skip_verify`). A proposed solution to this would be keeping
  the user-originated query params and the changefeed-supported query params 
  separate by having an option `query_params` to specify user-originated query
  params that should be propagated to the server. The ones in the provided sink
  URL should be either taken as config params for the changefeed or flagged as 
  unknown.
  

* Retry functionality is particularly important when dealing with webhooks and
  webhook endpoints in general. The precedent for changefeeds is to mark every 
  error related to communicating with sinks as retryable, however for HTTP 
  endpoints this behavior may be more nuanced. If customers express a desire to
  configure this retry behaviour, we might want to consider adding configurable 
  options to control what errors to try on (HTTP error codes?), as well as how 
  many times to retry (Can be accomplished with something like 
  `retry.WithMaxAttempts`)


* Do we want to support other formats eventually such as Avro? Currently, only
  JSON is supported for simplicity and coupling with HTTP, however the
  [Avro docs](https://avro.apache.org/docs/current/spec.html) specify that the 
  header `content-Type: avro/binary` should be used.


* Look into other ways of verifying server ownership/authority (other than CA
  certs), example is something like 
  https://w3c.github.io/websub/#hub-verifies-intent 


* More configuration is likely needed to get OAuth2 to run smoothly in the 
  context of a changefeed. OAuth2 tokens are short-lived and need to be 
  regenerated frequently, so the user should instead provide credentials and 
  an endpoint to regenerate a token so that it can be done continuously on sink 
  failure (good future action item). 


* The performance of the webhook sink could also be improved by using
  concurrent worker goroutines to execute webhook requests in parallel. Per-key
  ordering would be ensured by assigning each row with the same primary key
  to the same worker, and queueing requests for each worker in order.
