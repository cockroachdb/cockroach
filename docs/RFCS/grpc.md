- Feature Name: grpc
- Status: completed
- Start Date: 2015-12-07
- RFC PR: [#3352](https://github.com/cockroachdb/cockroach/pull/3352)
- Cockroach Issue: [#2381](https://github.com/cockroachdb/cockroach/pull/2381),
                   [#3013](https://github.com/cockroachdb/cockroach/issues/3013),
                   [#3421](https://github.com/cockroachdb/cockroach/pull/3421)

# Summary

Replace the custom protocol in `cockroach/rpc/codec` with GRPC. This
will affect both internal communication and potential client-facing
APIs (assuming we offer other public APIs besides `pgwire`). GRPC was
previously discussed in #2381 for client-facing APIs; this RFC extends
the proposal to internal RPCs as well.

# Motivation

The primary motivation is to minimize the impact that raft snapshots
have on other RPCs (#3013). Our RPC codec transmits each request as a
single chunk on the network, which can block other requests for a
significant period of time (leading to more severe consequences if
raft heartbeats or range lease operations are blocked for too long).
Since GRPC is based on HTTP/2, it has built-in support for
multiplexing large messages.

Secondary benefits of GRPC include support for bidirectional streaming
channels, which are a good fit for raft and gossip messages that do
not fit the "one request, one response" pattern, and may be useful for
future distributed SQL workloads. There are also benefits to adopting
a more widely-used protocol instead of our current custom one.

# Detailed design

Remove all use of `cockroach/rpc/codec` and `net/rpc`, replacing them
with GRPC. `cockroach/rpc` be removed, or may become a thin wrapper
around GRPC. In either case, interfaces will need to be extended to
allow streaming; it probably makes more sense to pass GRPC objects
around everywhere than to attempt to cover them with our own
abstraction.

# Drawbacks

GRPC is currently slower than our RPC codec ([benchmark results](
https://github.com/cockroachdb/rpc-bench)). It
[looks like](https://github.com/grpc/grpc-go/issues/89) the Go
implementation of GRPC has not yet seen significant performance work.
It should be possible to improve performance, but GRPC is more complex
and it may be difficult to match the performance of our custom codec.

Investigation so far indicates that the performance difference is
primarily due to the fact that GRPC performs two `Write` syscalls per
server response (one for the headers and one for the body) while our
own codec does one. GRPC also spends a bit more time in garbage
collection than we do.

The `grpc-go` server does not currently support serving other HTTP
requests on the same port as GRPC (grpc/grpc-go#75). We would need to
either fix this upstream or listen on two ports (or hack around it: an
HTTP handshake with an `Upgrade` header like our current RPC codec is
possible, but would make us incompatible with standard GRPC).

# Alternatives

We could improve the streaming/multiplexing capabilities of our own
RPC codec, or work around the problem by manually splitting messages
at a higher level (#3421 contains the beginning of this work).

Using protobufs over plain HTTP/2 is in some ways simpler than using
GRPC (in that it removes a layer), and gives us some of the key
benefits including better multiplexing for snapshots. This is likely
to have similar performance to GRPC. For internal use GRPC should be
better than plain HTTP, but for external APIs plain HTTP will be
easier to use from languages where no high-quality GRPC implementation
exists.

# Unresolved questions
