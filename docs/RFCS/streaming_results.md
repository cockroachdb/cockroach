- Feature Name: Streaming results through pgwire
- Status: draft
- Start Date: 2017-06-15
- Authors: Tristan Ohlson
- RFC PR: [#16626](https://github.com/cockroachdb/cockroach/pull/16626)
- Cockroach Issue: [#7775](https://github.com/cockroachdb/cockroach/issues/7775)

# Summary

At the time of writing the results of a SQL query are buffered into memory
prior to being sent back to the client. This RFC outlines a minimum viable
plan for streaming results through pgwire.

# Motivation and background

It is currently impossible to do queries with large result sets, since we will
exceed our memory budget. A nice side effect of changing to a streaming
interface is lowering the time to first byte in some cases.

Executor currently provides an interface which takes in a query and returns a
list of results. The interaction between executor and pgwire (`v3.go`) will
have to be changed.

A concern for streaming results is handling error cases – especially retryable
errors. It's important to note that we cannot automaticaly retry a query whose
results contradict those that we've already sent to the client.

# Non-Goals

While nice to have, some of these goals may delay the initial implementation.
If they end up in the first pass then awesome, otherwise most of them will be
explored in follow ups.

- Improving efficiency through encoding results directly to pgwire output
  format.
- Removing the pagination code for the `cockroach dump` command and relying on
  streaming results instead.
- Configurable buffer size.
- Starting to stream results after the buffer has been non-empty for some
  amount of time.
- Supporting both streaming and non-streaming behavior.
- Decoupling the client communication and query processing by adding
  asynchronicity.

# Detailed design

I propose creating a result writer interface which `v3.go` can pass into
executor to write results to. The result writer interface will look roughly
like this:
``` {.go}
type ResultWriter interface {
    BeginResult(pgTag string, statementType parser.StatementType)
    SetColumns(columns sqlbase.ResultColumns)
    Type() parser.StatementType
    SetRowsAffected(rowsAffected int)
    AddRow(ctx context.Context, row parser.Datums) error
    EndResult()
    Error(err error)
}
```

## Automatic retries

Adding a buffer which holds some amount of rows before sending results to the
client alleviates automatic retry concerns for queries which return a result
set less than the size of the buffer. Data collected on June 5th 2017 indicates
that automatic retries are predominately used for queries which return between
0 and 20 rows. As such I would initially propose a buffer size of 20.

Executor can remain in charge of its automatic retries as long as it can
determine whether the result writer has sent results back to the client.
Conveniently, we now have the ability to change the transaction state from
`FirstBatch` to `Open` to indicate to executor that the query cannot be
automatically retried.

## Postgres wire protocol

Postgres states that "a frontend must be prepared to accept
`ErrorResponse` and `NoticeResponse` messages whenever it is expecting any other
type of message"<sup>[1]</sup>. Due to this it is safe to send an error midway
through streaming results to the client.

# Alternatives

## Change executor's interface to provide Start/Next/Close methods

Modifying executor's interface to provide Start/Next/Close methods is another
possible approach. In our code `v3.go` already has the concept of a list of
results (we loop through the statement results when we send them to the
client), so having it call Next repeatedly would not a big change.

### Automatic retries

The largest change that would have to happen for `v3.go` comes from automatic
retry handling. Since we want to keep automatic retries this logic would need
to be pushed into `v3.go`.

Adding a buffer which holds a configurable amount of rows before sending
results to the client would alleviate automatic retry concerns for queries
which return a result set less than the configured amount.

### Downsides

The first downside is that this implementation pushes retry logic into `v3.go`
which is not ideal.

The second downside is a side-effect of being synchronous. We are unable to
find more results while we are sending existing results back to the client.

The last downside is that this implementation could require a lot of changes
and executor is not the easiest code to modify.

# Footnotes

\[1\]: https://www.postgresql.org/docs/9.6/static/protocol-flow.html

[1]: https://www.postgresql.org/docs/9.6/static/protocol-flow.html
