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

Quick note is that the common `FlowStream` RPC section doesn't contain many code
pointers since two execution engines use different components; instead, that
section should be read as an abstract in order to get the general understanding
of the control flow. Once the general idea is obtained, the reader is encouraged
to dive into the corresponding section for the execution engine of their
interest where many code pointers can serve as a guide for the details.

## FlowStream RPC

In order to communicate the intermediate results of the query evaluation between
different CockroachDB nodes, the [`FlowStream` RPC](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfrapb/api.proto#L163-L173)
is used. The RPC is initiated by the producer (the outbox, the "client" from the
perspective of the RPC) that wants to push those intermediate results to the
consumer (the inbox, the "server" from the perspective of the RPC). Thus,
`FlowStream` RPC sets up a bi-directional gRPC stream between two nodes.

### Outbox Side

Each outbox runs in a separate goroutine that is instantiated on the [flow setup](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/flow.go#L396-L398).
After getting the client-side of the gRPC stream by calling `FlowStream`, the
outbox spins up another "watchdog" goroutine to "watch" the stream.

The main goroutine is responsible for `Send`ing data to the consumer until there
is no more data to send (i.e. the outbox tree has been fully exhausted) or until
the drain is requested by the consumer. In the end, the outbox calls `CloseSend`
on the stream to notify the consumer that all data has already been pushed.

Additionally, any error if encountered when performing `Send` is examined
- if it is `io.EOF`, it indicates that the `FlowStream` RPC was completed
gracefully
- if it is non-`io.EOF`, then the RPC was completed ungracefully.

The "graceful" termination means that the consumer doesn't need any more data
from the outbox (because it has satisfied its LIMIT, etc), and the query as a
whole is still being executed successfully. The "ungraceful" termination though
indicates that some unrecoverable error has been encountered, so the whole query
should be shutdown as quickly as possible, and it is achieved by the outbox
canceling the context of the whole flow.

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

### Inbox Side

On the inbox side, the stream is also utilized by two goroutines: the "stream
handler" and the "reader".

The stream handler goroutine is spun up by the gRPC to perform `FlowStream` RPC
that was issued by the outbox side. All stream handler goroutines are tracked by
the wait group of the flow (the setup is done in [`StartInternal`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/flow.go#L382)).
These goroutines first wait for the flow (that contains their reader) to arrive
in [`ConnectInboundStream`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql/server.go#L660-L662)
(with the timeout of `sql.distsql.flow_stream_timeout`, 10 seconds by default),
and then block until either the flow context is canceled or their reader tells
the stream handler to exit.

Importantly, the error (or `nil`) returned by
[`flowinfra.InboundStreamHandler.Run`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L32)
becomes the result of the `FlowStream` RPC call overall. In other words, this is
what the "watchdog" goroutine of the outbox side will `Recv` last. Each stream
handler goroutine is released from the wait group of the flow when it exits from
the [`Run` method](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql/server.go#L666).

The reader goroutine is responsible for both `Recv`ing the data produced by the
outbox and for `Send`ing signals (like a drain request) to the producer. See
the corresponding section for details on when this goroutine is created since it
differs between the execution engines.

The reader goroutine keeps on running until it `Recv`es an error which is then
examined in the same way as on the outbox:
- `io.EOF` is received when the outbox called `CloseSend` to indicate that
everything has already been sent across the stream. This results in the stream
handler returning `nil` to indicate the graceful completion of the `FlowStream`
RPC.
- non-`io.EOF` error indicates the ungraceful termination of the gRPC stream
(most likely either because the context of the stream is canceled on the outbox
side or because the connection is broken - possibly because a node crashed).

## Vectorized Engine

### Shutdown Protocol

In the vectorized execution engine, the distributed query is shutdown in the
same manner regardless of the reason (i.e. both graceful and ungraceful
completions). The shutdown here relies on the assumption that all components of
the flow use the `context.Context` that is a descendant of the "flow context";
thus, canceling the flow context leads to all infrastructure on that node that
is part of the distributed query to exit.

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
cancel the flow contexts of their hosts. This, in turn, triggers propagation of
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
shutdown.
- If there are any outboxes still running on the hash router host, then
they will cancel the flow context which will prompt the hash router to exit too.
- If all outboxes on the hash router host have exited gracefully already, then
all outboxes have received a zero-length batch. This includes the outboxes that
the hash router is pushing into (if the hash router's input had only a single
local output, then the hash router would have not been planned). The outboxes
that sit on top of [`routerOutputOp`s](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/routers.go#L103)
(outputs of the hash router) will get a zero-length batch iff the hash router's
input returned a zero-length batch too, which means that the hash router has
already exited as well.

### Outbox Details

The main goroutine of the outbox is tracked by the wait group of the [flow](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/vectorized_flow.go#L1260-L1268).
It first establishes the connection to the target node and calls `FlowStream`
RPC, importantly, [using the flow context](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/outbox.go#L192).
The usage of the flow context when instantiating the RPC allows the outbox to
cancel its own context without shutting down the gRPC stream prematurely.

Once the gRPC stream is established, another "watchdog" goroutine is [spun up](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/outbox.go#L382).
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
or of the outbox) is canceled. Draining is not performed (meaning that
[`DrainMeta`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/outbox.go#L349)
is not called on the sources of metadata), and the outbox terminates.
- the outbox encounters an internal error that is not related to the stream. The
error is sent as metadata, the drain is performed, and the outbox terminates.

See [Metadata handling](#metadata-handling) for some more information about
draining and metadata.

### Inbox Details

The stream handler goroutine non-blockingly [passes](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L229)
the gRPC stream to the reader goroutine and then performs two blocking actions:
- first it waits for the reader to arrive (which is indicated by the reader
[sending its context](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L311)).
- then it waits for the reader to exit (by [closing the inbox](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L204)).

If - while waiting during either of the above two blocking actions - the stream
handler observes that the stream context or the flow context is canceled, then
it exits with the corresponding error ([here](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L237-L243)
and [here](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L253-L266)).
TODO(yuzefovich): rearrange blocks to be in the same order for both blocking
actions.
If the reader context is canceled, then in order to protect against a possible
race between the reader context and the flow context (which must be the ancestor
of the reader context) being canceled, the flow context is [explicitly checked](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L263)
for cancellation as well.

Although we do listen on the stream context cancellation, that seems to not be
strictly necessary (because when the stream context is canceled, an error will
be observed when `Send`ing/`Recv`ing on the stream). Still, it seems cleaner to
explicitly listen on the stream context too.

The reader goroutine is not spun up explicitly by the stream handler, instead,
it is "fused" with the reader's consumer. If the inbox is pushing into a
synchronizer, then it'll be a separate input goroutine [created by](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colexec/parallel_unordered_synchronizer.go#L214)
the parallel unordered synchronizer or the only goroutine of the serial (either
ordered or unordered) synchronizer. If the inbox is feeding into a
`colexecop.Operator`, then it'll be that operator's goroutine.

The reader goroutine can exit in several ways:
- an error is `Recv`ed from the stream. It is examined as usual, with
non-`io.EOF` errors are marked as ["inbox communication errors"](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L364).
Such errors are propagated both to the [consumer of the inbox](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L366)
and to the [stream handler](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L365)
goroutine (so they become the result of `FlowStream` RPC).
- an error is received from the outbox [as metadata](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L374)
or is encountered internally ([here](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L408)
and [here](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L415))
by the inbox. Such errors are propagated to the [consumer of the inbox](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L343),
but not to the stream handler because we do not want to complete the
`FlowStream` RPC ungracefully since that would trigger the immediate
cancellation of all other RPC calls, and we still are interested in draining the
metadata.
TODO(yuzefovich): I think it's not necessary to close the inbox explicitly in
this case - we won't be able to drain the metadata from the outbox since the
stream handler has exited.

### Note about `statement_timeout` implementation

`statement_timeout` in CockroachDB is implemented by setting up a [timer](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/conn_executor_exec.go#L471-L477)
which upon firing cancels the query. The query cancellation is done by
[canceling](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/exec_util.go#L1781)
the context of the transaction. The flow context on the gateway node is a
descendant of the transaction context, so canceling the latter results in
exactly the shutdown protocol described above.

## Row-by-row Engine

### Abstractions

Before we can talk about the shutdown protocol in the row-by-row engine, we need
to mention the abstractions that the engine is using, namely [`RowSource`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L109)
and [`RowReceiver`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L64)
interfaces. The `RowSource` on [`Next`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L146)
call returns a row or a metadata object which is then `Push`ed into the
`RowReceiver`. In return, the `RowReceiver` shares its possibly updated
[`ConsumerStatus`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L41-L51).

Essentially all [`Processor`s](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L36)
implement the `RowSource` interface, and we use different components to connect
the processors together (routers, [`RowChannel`s](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L431),
synchronizers), possibly on different nodes. Such chaining allows for
propagation of the `ConsumerStatus` to all processors of the distributed plan.

Most processors delegate the implementation of [`Processor.Run`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L47)
method to the embedded [`ProcessorBase`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L728)
which `Start`s itself and then goes into the [`Run`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L184)
loop. The loop continuously gets the next row or metadata object from the
`RowSource` (the processor embedding the helper), pushes that to the
`RowReceiver`, and then examines the status to see whether any changes of the
control flow are requested by the consumer.

At the root of the flow on the gateway we have a special [`DistSQLReceiver`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql_running.go#L584)
guy that implements the `RowReceiver` interface, and it is where the results of
the query are ultimately `Push`ed into. The gateway flow is being [`Run`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/flow.go#L104)
which means starting all processors except for the last one in separate
goroutines and running the "root" processor in the [flow's goroutine](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/flow.go#L453).
(There is a caveat that we try to "fuse" components - that feed directly into
each other without using concurrency - to run in the same goroutine, but we'll
ignore that for this discussion.) The output of the root processor is always the
`DistSQLReceiver`.

### Shutdown Protocol

#### Graceful Completion

Let's first discuss the way the distributed query is shutdown gracefully when
all processors have been exhausted fully. In such a scenario each processor goes
from [`StateRunning`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L464)
to [`StateDraining`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L492)
by calling [`MoveToDraining`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L514).
That function notifies the processor's input that it should be drained (by
calling `ConsumerDone`), and then the processor is using [`DrainHelper`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L549)
to first fetch all of the metadata from its input and then return any [trailing
metadata](#metadata-types) for the processor itself. Once all trailing metadata
has been propagated, the processor is closed automatically (by calling
[`InternalClose`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L904)).
At this point the processor's input must have already been closed too since it
was fully exhausted when the metadata was drained.

The same transitions apply to the `DistSQLReceiver`'s input - the root processor
on the gateway: the processor is drained first, and then it is closed. These
transitions are noticed by other processors running on the gateway node because
the `RowReceiver`s that those other processors are pushing into have a change of
the `ConsumerStatus` in that [`Run`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L184)
loop. Right before `Run` exits `ProducerDone` is called on the `RowReceiver`.

Apart from the processors, there are two other sources of concurrency in the
row-based flows: routers and outbox/inbox streams. The routers get notified
about the closure when `ProducerDone` is called, so that case is already
handled; however, the outbox/inbox streams deserve a bit more attention and they
are discussed below, but for now we'll just assume that they also get the
information about these transitions. Thus, all goroutines of all flows of the
distributed query first transition into draining and then exit.

Another way of gracefully completing the query but without fully exhausting the
processors is satisfying the LIMIT clause. This is achieved by the root
processor moving to draining once the limit is reached (this is done in
[`ProcessorBase.ProcessRowHelper`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/processorsbase.go#L707)).

#### Ungraceful Completion

Whenever the query execution encounters an error, we can think of that query
being completed "ungracefully". In such a case, regardless of where the error
occurred, it is propagated as a metadata object and eventually reaches the
[`DistSQLReceiver`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql_running.go#L949).
The `DistSQLReceiver` then [transitions](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql_running.go#L906)
into draining, and the shutdown is performed in the same manner as described
above.

The same process is followed in case of the gRPC infrastructure failure because
an error will eventually be pushed into the `DistSQLReceiver`. Notably, even if
the flow context on the gateway node is canceled (because an outbox on the
gateway observed ungraceful termination of the `FlowStream` RPC), the
`DistSQLReceiver` will still transition to draining first in [`SetError`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql_running.go#L897)
because the `DistSQLReceiver`'s `ctx` is the transaction context, and it hasn't
been canceled.

### Outbox Details

The main goroutine of the outbox is tracked by the wait group of the [flow](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L418-L441).
Similar to the vectorized case, it first establishes the connection to the
target node and performs the `FlowStream` RPC [using the flow context](https://github.com/cockroachdb/cockroach/blob/e976a469d89bc2fdd59dc80bc58a11e9b61714a0/pkg/sql/flowinfra/outbox.go#L253).

Next, a separate "watchdog" goroutine is spun up [using the stopper](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L377)
which is responsible for `Recv`ing from the gRPC stream (mainly to listen for
drain requests as well as to any errors). This goroutine is not tracked
explicitly, and it will exit once an error is `Recv`ed from the gRPC stream.
TODO(yuzefovich): track this goroutine with the wait group.

Tha main goroutine goes into the [loop](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L276)
where it reads from the input (either a processor or a router) and writes to the
gRPC stream. Rows and metadata are encoded in [`AddRow`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L121)
into messages which are [`flushed`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L156)
to the consumer once [16 rows](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L34)
have been accumulated or [100 microseconds](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L35)
have passed since the last flush, the exception is only made to [errors](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L130)
which are flushed immediately.

The main goroutine exits from the loop when one of the following events occurs:
- `ProducerDone` has been called on the outbox's input which resulted in the
closure of the embedded [`RowChannel`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L279).
This includes the case when the draining was requested (the "watchdog" goroutine
notifies the main goroutine about it, and then the main goroutine only calls
`AddRow` for metadata objects after calling [`ConsumerDone` on its input](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L344)).
As a reminder, `ProducerDone` is always called at the end of the [`Run` loop](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L184)
of the processor.
- an error is encountered when [`AddRow` is called](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L308)
(either a communication error because of some gRPC problem or an encoding
error).
- the "watchdog" goroutine `Recv`es an error from the gRPC stream
  - if it is `io.EOF`, then the main goroutine exits [gracefully](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L348).
  - if it is non-`io.EOF`, then the main goroutine cancels the context of the
    flow and exits [ungracefully](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L338).

`ConsumerClosed` is [always](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/outbox.go#L208)
called on the input to the outbox regardless of the way it exited its main loop.

### Inbox Details

Unlike in the vectorized engine, the row-by-row engine doesn't have an explicit
component that is responsible for the consumer-side of the `FlowStream` RPC;
however, it still involves two goroutines.

The "stream handler" goroutine (that is spun up by the gRPC framework) first
[creates](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L133)
the "reader" goroutine and then proceeds to [block](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L163-L168)
until either the flow context is canceled or the reader has communicated that it
is done. TODO(yuzefovich): use the stopper to spin up the reader goroutine.

The "reader" goroutine is tracked by the wait group of the [flow](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L132-L134),
and it keeps on [`Recv`ing](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L136)
from the gRPC stream. Any error, if encountered upon `Recv`ing, is examined in
the same fashion as usual and is communicated to the stream handler.
Additionally, non-`io.EOF` errors are marked as ["inbox communication errors"](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L140).
The received messages are [decoded](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L217)
and then [pushed](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L236)
to the consumer of this inbound stream. If that consumer asks for draining, then
the drain signal is [sent](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/flowinfra/inbound.go#L246)
on the gRPC stream.

### Note about `statement_timeout` implementation

As described in the same section for the vectorized engine above,
`statement_timeout` is implemented by canceling the context of the transaction.
Notably, this is an ancestor of the context that the `DistSQLReceiver`
[operates with](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql_running.go#L585).
Once the transaction context is canceled, the `DistSQLReceiver` does not itself
immediately notify the query infrastructure about the cancellation; however, the
next time anything is pushed into the `DistSQLReceiver`, it notices the
[cancellation](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql_running.go#L1039-L1041).
The `ConsumerStatus` is [changed](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/distsql_running.go#L903)
to `ConsumerClosed` which triggers the shutdown protocol described above.

Additionally, since the flow context of the gateway node is canceled (because it
is a descendant of the transaction context), all open gRPC streams for which the
gateway is the inbox host terminate ungracefully. This, in turns, makes the
corresponding outboxes to cancel the flow contexts of their hosts and exit too.
This context cancellation propagation speeds up the query shutdown.

## Additional Information

### Metadata Types

All metadata objects (parts of the "control" plane of the query execution) can
be divided into two types:
- "regular" metadata that is collected and propagated during the query
execution. For example, this includes a [progress update](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/rowexec/sampler.go#L269-L271)
that is created during the table statistics collection in order to tell the
client about how far the stats collection job has progressed.
- "trailing" metadata that is collected at the very end of the query execution.
For example, this includes [`LeafTxnFinalState`](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L292)
which contains all KV spans that have been read on a node and that need to be
propagated to the gateway to perform a refresh.

### Metadata Handling

A quick note on the way metadata is handled is worthwhile: unlike in the
row-by-row engine where the metadata is propagated alongside the actual rows of
data (i.e. both "data" and "control" planes operate through the same [`Next`
method](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/execinfra/base.go#L146)
although not simultaneously), in the vectorized engine the metadata that
originates during the query execution is buffered in the "metadata sources".
Those "sources" are then drained after all the actual data has been processed,
at the end of the query execution. The only exception is made to the errors
propagated as metadata which are not buffered and are [thrown](https://github.com/cockroachdb/cockroach/blob/daf29fd72896e46c1c40ddc5b62e504a8ee77b68/pkg/sql/colflow/colrpc/inbox.go#L378)
immediately.
