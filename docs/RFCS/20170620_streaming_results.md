- Feature Name: Streaming results through pgwire
- Status: In-progress
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
errors. It's important to note that we cannot automatically retry a query whose
results contradict those that we've already sent to the client.

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
set less than the size of the buffer.

Executor can remain in charge of its automatic retries as long as it can
determine whether the result writer has sent results back to the client.
Conveniently, we now have the ability to change the transaction state from
`FirstBatch` to `Open` to indicate to executor that the query cannot be
automatically retried.

While technically "we cannot automatically retry a query whose results
contradict those that we've already sent to the client", in practice we will
not automatically retry after we've sent _any_ results to the client.

## Configuration

Buffer size will be configured per-session, with a default configured, using
a cluster setting.

I propose a default of 20 due to data collected on June 5th 2017 which 
indicates that automatic retries are predominately used for queries which
return between 0 and 20 rows.

A user would be able to disable streaming in practice by setting the buffer
size to `MaxInt64`. 

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

- Pushes retry logic into `v3.go` which is not ideal
- Could require a lot of changes to executor, which is not the easiest code to
  modify

# Possible future steps

- Buffer directly into the pgwire output format
- Remove the pagination code for the `cockroach dump` command and rely on
  streaming results instead
- Start streaming results after the buffer has been non-empty for some amount
  of time
- Decouple the client communication and query processing by adding
  asynchronicity

# Potential additional benefits

- In the Spanner SQL paper they mention that clients are able to get paginated
  results without sorting by using streaming results.

# Footnotes

\[1\]: https://www.postgresql.org/docs/9.6/static/protocol-flow.html

[1]: https://www.postgresql.org/docs/9.6/static/protocol-flow.html
