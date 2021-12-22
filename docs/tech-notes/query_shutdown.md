# Query Shutdown

Author: Yahor Yuzefovich

## Introduction

This document aims to explain the protocol for shutting down the distributed
query execution in CockroachDB. It covers both cases when the query completes
successfully (it has returned all necessary rows to the client) and when the
query is canceled for any reason (an error is encountered, a node crashed, the
statement timeout is reached, etc). CockroachDB has two execution engines that
use different components for the distributed execution, and this document
describes both in some details; however, both engines utilize the same
`FlowStream` RPC to communicate between different CockroachDB nodes, and this
RPC is covered first. Additionally, this document points out how all goroutines
that power all the necessary infrastructure are being accounted for.

## FlowStream RPC

In order to communicate the intermediate results of the query evaluation between
different CockroachDB nodes, the [`FlowStream` RPC](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfrapb/api.proto#L163-L173)
is used. The RPC is initiated by the producer (the outbox, the "client" from the
perspective of the RPC) that wants to push those intermediate results to the
consumer (the inbox, the "server" from the perspective of the RPC). Thus,
`FlowStream` RPC sets up a bi-directional gRPC stream between two nodes.

### Outbox side

Each outbox runs in a separate goroutine that is instantiated on the [flow setup](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/flow.go#L396-L398).
After getting the client-side of the gRPC stream by calling `FlowStream`, the
outbox spins up another goroutine to "watch" the stream.

The main goroutine is responsible for `Send`ing data to the consumer until there
is no more data to send (i.e. the outbox tree has been fully exhausted) or until
the drain is requested by the consumer. In the end, the outbox calls `CloseSend`
on the stream to notify the consumer that all data has already been pushed.

Additionally, any error if encountered when performing `Send()` is examined: if
it is `io.EOF`, it indicates that the `FlowStream` RPC was completed gracefully
by the consumer since it no longer needs any more data; if it is non-`io.EOF`,
then the RPC was completed ungracefully. The ungraceful termination indicates
that the whole query should be shutdown as quickly as possible, and it is
achieved by the outbox canceling the context of the whole flow.

The "watchdog" goroutine is responsible for `Recv`ing signals from the consumer.
The consumer sends a handshake message (which is basically ignored by the
outbox) and can also ask the outbox to drain (by sending the corresponding
message). Once drain is requested, the outbox collects all available metadata,
pushes it to the inbox, and then performs `CloseSend`.

Note that the "watchdog" goroutine exits only _after_ receiving an error from
the stream. Because the outbox is the client of the RPC, it cannot unilaterally
close it, and by calling `CloseSend` it advises the server to complete the RPC
call. The error encountered on `Recv`ing is examined in the similar manner as
when `Send`ing.

### Inbox side

On the inbox side, the stream is also utilized by two goroutines: the "stream
handler" and the "reader".

The stream handler goroutine is span up by the gRPC to perform `FlowStream` RPC
that was issued by the outbox side. All stream handler goroutines are tracked by
the wait group of the flow (the setup is done in [`StartInternal`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/flow.go#L382).
These goroutines first wait for the flow (that contains their reader) to arrive
in [`ConnectInboundStream`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql/server.go#L660-L662)
(with the timeout of `sql.distsql.flow_stream_timeout`, 10 seconds by default),
and then block until either the flow context is canceled or their reader tells
the stream handler to exit.

Importantly, the error (or `nil`) returned by
`flowinfra.InboundStreamHandler.Run` becomes the result of the `FlowStream` RPC
call overall. In other words, this is what the "watchdog" goroutine of the
outbox side will `Recv` last. Each stream handler goroutine is released from the
wait group of the flow when it exits from the [`Run` method](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql/server.go#L666).

The reader goroutine is responsible for both `Recv`ing the data produced by the
outbox and for `Send`ing signals (like a drain request) to the producer. See
below for details on when this goroutine is created since it differs between
execution engines.

The reader goroutine keeps on running until it `Recv`es an error which is then
examined in the same way as on the outbox:
- `io.EOF` is received when the outbox called `CloseSend` to indicate that
everything has already been sent across the stream. This results in the stream
handler returning `nil` to indicate the graceful completion of the `FlowStream`
RPC.
- non-`io.EOF` error indicates the ungraceful termination of the gRPC stream
(most likely either because the context of the stream is canceled on the outbox
side or because the connection is broken - possibly because a node crashed).

## Vectorized engine

### Shutdown Protocol

In the vectorized execution engine, the distributed query is shutdown in the
same manner regardless of the reason (i.e. both graceful and ungraceful
completions). The shutdown here relies on the assumption that all components of
the flow use the `context.Context` that is a child of the "flow context"; thus,
canceling the flow context leads to all infrastructure on that node that is part
of the distributed query to exit.

The shutdown protocol is as follows:
- on the gateway node there is a flow coordinator ([`FlowCoordinator`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/flow_coordinator.go#L31)
or [`BatchFlowCoordinator`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/flow_coordinator.go#L184))
which cancels the flow context ([1](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/flow_coordinator.go#L159),
[2](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/flow_coordinator.go#L332))
at the end of the query execution. It doesn't matter whether the query resulted
in an error or not because we know that this flow context cancellation will not
be propagated to the client who issued the query, so it's totally fine that
flows on other nodes can think of this shutdown as ungraceful.
- that flow context cancellation on the gateway terminates ungracefully all
still open gRPC streams for `FlowStream` RPCs for which the gateway node is the
inbox host (the consumer).
- the outboxes on the client side of the ungracefully terminated gRPC streams
cancel the flow context of their hosts. This, in turn, triggers propagation of
the ungraceful termination on other gRPC streams if any are still open on the 
corresponding nodes, etc.

We also have to consider the case when all outboxes on a remote node have
already exited gracefully before the flow context cancellation is performed on
the gateway node. In such case, each outbox [cancels](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/outbox.go#L236-L239)
its own "outbox" context which terminates all of the components (including each
input goroutine of the [parallel unordered synchronizer](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colexec/parallel_unordered_synchronizer.go#L319-L321))
that are part of the tree with this outbox at the root. The fact that the outbox
exited indicates that the stream handler goroutine on the inbox side for the
same gRPC stream has also exited.

Another source of the concurrency in the vectorized flows is the hash router
which runs in a separate goroutine and listens on the flow context for
cancellation. Thus, we separately have to consider how the hash router is being
shutdown. If there are any outboxes still running on the hash router host, then
they will cancel the flow context which will prompt the hash router to exit too.
If all outboxes on the hash router host have exited gracefully already, then
all outboxes have received a zero-length batch. This includes the outboxes that
the hash router is feeding into (if the hash router's input had only a single
local output, then the hash router would have not been planned). The outboxes
that sit on top of `routerOutputOp`s will get a zero-length batch iff the hash
router's input returned a zero-length batch too, which means that the hash
router has already exited as well if there are no outboxes running on the hash
router host.

### Outbox Details

The main goroutine of the outbox is tracked by the wait group of the [flow](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/vectorized_flow.go#L1260-L1268).
It first establishes the connection to the target node and calls `FlowStream`
RPC, importantly, [using the flow context](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/outbox.go#L192).
The usage of the flow context when instantiating the RPC allows the outbox to
cancel its own context without shutting down the gRPC stream prematurely.

Once the gRPC stream is established, another "watchdog" goroutine is spun up.
This goroutine is not tracked by any wait group, but the main goroutine blocks
before exiting until the watchdog routine exits.
TODO(yuzefovich): we should probably track the watchdog goroutine with the
flow's wait group as well as use the stopper to spin it up.

Then the main goroutine goes into [`sendBatches`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/outbox.go#L271)
loop where it reads batches from the input and sends them over the stream to the
consumer. That loop terminates in several ways:
- a zero-length batch is received from the input of the outbox. The outbox
transitions to drain the metadata sources and exits. This is a graceful
termination.
- the watchdog goroutine received a drain signal from the inbox. Same as above.
- the outbox encounters an error when interacting with the stream. The error is
examined as described above, and the corresponding context (either of the flow
or of the outbox) is canceled. Draining is not performed, and the outbox
terminates.
- the outbox encounters an internal error that is not related to the stream. The
error is sent as metadata, the drain is performed, and the outbox terminates.

### Inbox Details

The stream handler goroutine non-blockingly passes the gRPC stream to the reader
goroutine and then performs two blocking actions:
1. it waits for the reader to arrive (which is indicated by the reader sending
its context).
2. then it waits for the reader to exit (by closing the inbox).

If - while waiting - the stream handler observes that the stream context or the
flow context is canceled, then it exits with the corresponding error as well. If
the reader context is canceled, then in order to protect against a possible race
between the reader context and the flow context (which must be the ascendant of
the reader context) being canceled, the flow context is explicitly checked for
cancellation as well.

Listening on the stream context cancellation seems to not be strictly necessary
(because when it is canceled, an error will be observed when `Send`ing/`Recv`ing
on the stream), it seems like a cleaner option.

The reader goroutine is not spun up explicitly by the stream handler, instead,
it is "fused" with the reader's consumer. If the inbox is feeding into a
synchronizer, then it'll be a separate input goroutine created by the parallel
unordered synchronizer or the only goroutine of the serial (either ordered or
unordered) synchronizer. If the inbox is feeding into a `colexecop.Operator`,
then it'll be that operator's goroutine.

The reader goroutine can exit in several ways:
- an error is `Recv`ed from the stream. It is examined as usual, with
non-`io.EOF` errors are marked as ["inbox communication errors"](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L364).
Such errors are propagated both to the consumer of the inbox and to the stream
handler goroutine (so they become the result of `FlowStream` RPC).
- an error is received from the outbox as metadata or is encountered internally
by the inbox. Such errors are propagated to the consumer of the inbox, but not
to the stream handler.

### Note about `statement_timeout` implementation

`statement_timeout` in CockroachDB is implemented by setting up a [timer](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/conn_executor_exec.go#L471-L477)
which upon firing cancels the query. The query cancellation is done by
[canceling](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/exec_util.go#L1781)
the context of the transaction. The flow context on the gateway node is an
descendant of the transaction context, so canceling the latter results in
exactly the shutdown protocol described above.

## Row-by-row engine

### Shutdown Protocol

### Outbox Details

The main goroutine of the outbox is tracked by the wait group of the [flow](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L418-L441).

### Inbox Details

### Note about `statement_timeout` implementation
