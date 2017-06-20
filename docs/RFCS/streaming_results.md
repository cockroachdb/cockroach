- Feature Name: Streaming results through pgwire
- Status: draft
- Start Date: 2017-06-15
- Authors: Tristan Ohlson
- RFC PR: TBD
- Cockroach Issue: [#7775](https://github.com/cockroachdb/cockroach/issues/7775)

# Summary

At the time of writing the results of a SQL query are buffered into memory
prior to being sent back to the client through pgwire. This RFC outlines a
plan for streaming results through pgwire.

# Motivation and background

It is currently impossible to do queries with large result sets, since we will
exceed our memory budget. A nice side effect of changing to a streaming
interface is lowering the time to first byte in some cases.

Executor currently provides an interface which takes in a query and returns a
list of results. The interaction between executor and pgwire (`v3.go`) will
have to be changed.

The main concern for streaming results is handling error cases – especially
retryable errors. It's important to note that once we've sent any results to
the client we are unable to automatically retry a query.

# Detailed design

## Executor's interface

I propose modifying executor's interface to provide Start/Next/Close methods.
In our code `v3.go` already has the concept of a list of results (we loop
through the statement results when we send them to the client), so having it
call Next repeatedly is not a big change.

## Automatic retries

The largest change that will have to happen for `v3.go` comes from automatic
retry handling. Since we wish to keep automatic retries this logic will need
to be pushed into `v3.go`.

Adding a buffer which holds a configurable amount of rows before sending
results to the client alleviates automatic retry concerns for queries which
return a result set less than the configured amount.

## Downsides

The first downside is that this implementation pushes retry logic into `v3.go`
which is not ideal.

The second downside is a side-effect of being synchronous. We are unable to
find more results while we are sending existing results back to the client.

# Alternatives

## Give executor an interface to write to

A naive approach for demonstration is to simply pass a channel from pgwire into
executor which it can use to send each result it gets rather than accumulating.

### Downsides

The first downside to this approach is that executor may do unnecessary work.
In the case where the connection closes pgwire is unable to inform executor
until executor attempts to send data through pgwire.

The second downside is that it will introduce asynchronicity to the pgwire
and executor interaction.

# TODO

- How buffer size should be configured: it might be nice to have a buffer whose
  size can be configured per query?
