- Feature Name: Index Lookups Memory Limits and Parallelism
- Status: accepted
- Start Date: 2021-06-17
- Authors: Andrei Matei
- RFC PR: [#67040](https://github.com/cockroachdb/cockroach/pull/67040)
- Cockroach Issue: [#54680](https://github.com/cockroachdb/cockroach/pull/54680)

# Summary

This RFC discusses the needs of index joiners and lookup joiners with respect to
KV execution. More broadly, it discusses the desire to evaluate KV reads within
a transaction in parallel, while at the same time putting memory limits on the
amount of data that's in-flight throughout the cluster (and in particular on the
coordinator node) due to these requests. At the moment, the `DistSender` forces
clients to choose, at the level of each `BatchRequest`, whether to get
parallelism or memory limits when evaluating the requests in a batch; this RFC
wants to make joins not have to make that decision and, instead, get both memory
budgets and a fairly-parallel execution.

The RFC then proposes implementing a library that sits on top of the existing KV
API and offers clients a different-looking API: instead of a batch-oriented API,
this library would offer a streaming-oriented one. This API would provide fewer
ordering guarantees than the batch-oriented API but, on the flip side, it would
offer control over memory usage, deliver partial results faster, eliminate a
type of "head-of-line blocking", and allow for more pipelining.

# Motivation

Currently, `Scans` and `Gets` that are part of batches configured with either a
key or memory limits cannot be parallelized by the `DistSender`. This presents a
major problem for SQL, which would like to set memory limits on all the requests
it sends in order to protect the sending node from OOMs caused by receiving many
responses which, when taken together, are too large to fit in memory. SQL also
wants to get a high degree of internal parallelism for some of its requests --
in particular for lookup requests sent by the index and lookup joiners. The
index and lookup joiners actually share an implementation, so we'll just talk
about the "lookup joiner"/`joinReader` from now on. This joiner is configured
with an input (a `RowSource` for, say, its left side) and a target table or
index on which to perform lookups for keys coming from the input-side. The
joiner works by repeatedly consuming/accumulating rows from the input according
to a [memory
budget](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/rowexec/joinreader.go#L357)
between 10KB and 4MB depending on the type of join; let's call these accumulated
rows an *input chunk*. To join this input chunk, the joiner (through the
`row.Fetcher` stack) builds one big `BatchRequest` with all the lookups and
executes it. We'll call this a *lookup batch*.
The lookup batch might be executed with limits (see below), in which case it can
repeatedly return paginated, partial results and need re-execution for the
remainder.

For each input row, the respective lookup takes the form of a `Get` or a `Scan`.
It's a `Scan` when either the lookup key is not known to be unique across rows
(so the lookup might return multiple rows), or when the looked-up rows are made
up of multiple column families. In the case of an index join, the key is known
to correspond to exactly one row (since the key includes the row's PK), though
not necessarily one key if the lookup table has multiple column families.

These lookup batches of `Scans` or `Gets` tend to divide randomly across many
ranges in the cluster. Evaluating them with a high degree of parallelism (for
example, parallelizing across ranges as the `DistSender` does), is crucial for
joining performance.

As things stand today, the joiner is forced to choose between setting memory
limits on the lookup batches (thus protecting the joiner node from OOMs) and
getting `DistSender`-level parallelism for the evaluation of these batches. If
memory limits are used, the inter-range parallelism is lost because of current
implementation limits. The joiner sometimes chooses limits, sometimes chooses
parallelism: if each lookup is guaranteed to return at most one row, then it
chooses parallelism. Otherwise, it chooses memory limits
([code](https://github.com/cockroachdb/cockroach/blob/bc95d8f5e79576e38208b89af65a2050ab52b982/pkg/sql/rowexec/joinreader.go#L225-L228)).
As a consequence, we've been known to OOM whenever the non-limited `BatchResponse`
proves to be too big to hold in memory at once. Or, more commonly, when
sufficiently many large-ish queries run concurrently - for example, with 1k
concurrent queries, each reading 10k rows at a time, and each row taking 1KB,
that's a 10GB memory footprint that these rows can take at any point in time.
We've also been known to execute the memory-limited lookups too slowly (e.g. [in
our TPC-E
implementation](https://github.com/cockroachdb/cockroach/issues/54680#issuecomment-858776769)). 

In the case when the `joinReader` chooses limits over parallelism, the limits are [10k
keys](https://github.com/cockroachdb/cockroach/blob/d6d394bf5c4974d79e21efe8c03f65ebf0bc10fa/pkg/sql/row/kv_batch_fetcher.go#L51)
as well as a
[10MB](https://github.com/cockroachdb/cockroach/blob/d6d394bf5c4974d79e21efe8c03f65ebf0bc10fa/pkg/sql/row/kv_batch_fetcher.go#L327)
size limit per lookup batch. And, of course, there's always some indirect limit
coming from the fact that the keys included in a lookup batch are coming from an
input chunk that was size-limited as described above.

Besides forcing the joiner into an impossible choice between size limits and
parallelism, the KV API doesn't seem to suit the joiner well (and perhaps other
processors) from another perspective: by forcing the joiner into this
one-BatchRequest-at-a-time execution model, the progress of a joiner gets
needlessly blocked by slow requests. When the lookup batch is split by the
`DistSender`, any sub-batch being slow blocks the whole lookup batch. Only after
all sub-batches finish can the joiner get some results, do processing, consume
more results from the input side, and send the next `BatchRequest`.

To understand whether this execution model is right or not, we should analyze
two cases:

1. The joiner wants to produce results in order (i.e. in the order of the rows
   on the input-side of the join). In this case, it seems that the fact that each
   batch of lookups acts as a barrier (and, moreover, that the slowest
   sub-batch of lookups within the batch acts as a barrier) is OK, since
   all those lookups need to finish before results for rows that were not part
   of the lookup can be produced. Still, the fact that further results cannot be
   produced doesn't mean that work can't still be pipelined - in particular,
   more lookups can be performed and results buffered, within a memory/disk budget.
2. The joiner can produce results in any order - perhaps because the results
   need to be re-sorted anyway, or because there's no particular ordering
   requirement in the query. In this case, ideally, we don't want any barriers;
   if the lookup of a particular row is slow, we want to go forth with lookups
   of other rows. The impact of a slow lookup should be limited to holding a
   memory reservation for the rows that might be returned by that lookup. Going
   further with exploiting the lack of ordering requirements, we could imagine
   that even holding the respective memory reservation for long could sometimes
   be avoided: if it looks like a particular lookup is blocked on a lock, we
   could cancel it and retry later when, hopefully, the latency will be better.
3. A sub-case of 2) that wants barriers even less is when there's no ordering
   requirement on the joiner and also there's a row limit. In this case, we could
   cancel slow lookups when that limit is satisfied by other, faster lookups. We
   can even imagine speculatively cancelling slow lookups before the limit is
   satisfied by betting that the limit will eventually be satisfied by faster
   requests.

A better execution model would be the following:
- A joiner should request as many rows at once as its memory budget allows for.
  In other words, absent `LIMIT` clauses, all limits should be expressed in
  bytes, not number of keys or rows; we should only ever set `TargetBytes` on
  the underlying KV requests, not `MaxSpanRequestKeys`.
- Since the joiner is in charge of not requesting too many rows at once, all the
  requests that it does make should be executed in parallel.
- As results come back, they make room in the budget for more rows to be
  requested. The joiner should take advantage of this budget opening up and
  request more rows. These new requests should be executed in parallel with the
  previous requests that are still in-flight. The exact time when the memory
  budget reserved for a particular request/response opens up again depends on
  the joiners ordering requirements:
  - If the joiner needs to buffer up a response because it needs to produce
    results in order and that result was received out of order, then the budget
    needs to stay allocated during this buffering period.
  - If the joiner produces results out of order, then the budget taken by any
    response can be released quickly.
    
This RFC focuses on the lookup joiner, but the streaming/parallel KV API it
proposes can also be used for other cases where parallel KV reads are useful.
For example, imagine a query like `SELECT ... WHERE id IN (...list of 1000
ids...)`. At the moment, we'd run this by planning a single `TableReader` on the
gateway. This `TableReader` has a similar choice to make to the `joinReader` -
parallelize the lookups or not (or, rather, the optimizer makes that choice for
the `TableReader`). Currently, we choose parallelism if the `TableReader` is
known to not return more than [10,000
rows](https://github.com/cockroachdb/cockroach/blob/49a5d88f4810b89ce564c29c13137f0bf89fd4c7/pkg/sql/opt/exec/execbuilder/builder.go#L29).
Ideally, it would always try to get parallelism, subject to memory limits. By
the way, currently, even when the `TableReader` wants parallelism, it never
actually gets it within a range.

Also imagine a scan of a large table with a selective filter applied on top of
it. At the moment, this query is split across nodes according to table range
leases. At the level of each node, there are alternating phases of waiting for
the results of a limited `Scan` and applying the filter on all those results (in
a vectorized manner). It seems we'd benefit from a) the ability to pipeline the
`Scanning` with the filtering and b) to `Scan` multiple (local) ranges in
parallel in order to saturate the filtering capacity. The streaming API proposed
in this RFC does not fit this use case perfectly, but we can imagine that it'd
be extended to support it.

The proposal focuses mostly on eliminating the need to choose between memory
limits and parallelism. The component proposed is supposed to also support
pipelining of requests. My hope is that this support will come pretty much "for
free" but, in case it doesn't, the implementation might choose to defer it.
    
# The `BatchRequest` API

This section is a description of the existing `BatchRequest` API and, in
particular, its facilities for response limits and parallelism.

KV's client/server API consists of `BatchRequests`, with a batch consisting of
multiple requests. Each request operates on a key (e.g. `Get`, `Put`) or a key
span (`Scan`, `DeleteRange`). The result of a `BatchRequest` is a
`BatchResponse`; the response either contains results for all the requests in
the batch or only for some of them, identifying the ones with missing responses.
Some responses can be "partial responses"; see below. If some responses are
missing all-together, or if some are partial, and if the client wants the rest
of the results, it needs to send another `BatchRequest`. An important fact is
that the protocol is request/response; there's no streaming of responses from
the "server" to the "client". A streaming KV RPC has been [long
desired](https://github.com/cockroachdb/cockroach/issues/8360) for a) being able
to return an evaluation result and a separate replication result for the same
write request and b) for streaming the results of large `Scans` in order to save
on memory buffering. A streaming RPC is orthogonal to this RFC.

The `BatchRequest` has two types of limits: a key limit (`MaxSpanRequestKeys`)
and a byte limit (`TargetBytes`). It is because of these limits that a response
might have missing or partial results; in the absence of limits, responses are
complete. A partial response identifies a "resume key", which the client is
supposed to use in the next request.

### Responses for limited batches

1) If the requests in a limited batch are non-overlapping and ordered by their
keys (ascendingly for `Scans` and descendingly for `ReverseScans`; mixing the
two types of requests in a batch is not permitted), then the response will
consist of a sequence of complete results, a partial result, and a sequence of
empty results.
2) If the requests in a batch are overlapping (but otherwise ordered), then the
response might have multiple partial results, but it still has a prefix of
complete results and a suffix of empty results. A key that's returned within two
different results counts as two keys for the purposes of the limits.
3) If the requests in a batch are not ordered, then the response can have empty
results interspersed with non-empty ones. If the requests are non-overlapping,
there will be at most one partial result.
4) If the requests in a batch are both overlapping and unordered, there can be
both empty and complete results interspersed, as well as multiple partial
results interspersed.
   
We've talked about empty results and partial results distinctly. Technically,
they're represented in the same way in the `BatchResponse`: they both have
`ResumeSpans`, and an empty result has the `ResumeSpan` equal to the request's
span (i.e. "resume from the start").
   
### DistSender execution model

The fairly complex contract above flows from the way in which the `DistSender`
executes batches: it iterates in key order over all the ranges touched by at
least a request in the batch. For each range, it will send one RPC with a
sub-batch containing the "intersection" between the range and the full batch -
i.e. containing all the requests that overlap the range, where each request is
truncated to the range boundary. Results are then re-ordered and re-assembled to
produce the `BatchResponse`. Note that the sub-batches are sent in range order,
which is different from the request order if the requests are... unordered.

The execution differs greatly between limited batches and unlimited ones. For
unlimited ones, the execution of sub-batches is parallel (up to the limit of a
node-wide semaphore). The `BatchResponse` is returned once all the sub-batches
have returned.

Limited batches don't permit parallel execution (and therein lies the tragedy
this RFC is trying to address). Sub-batches are executed one after another, in
range order. After executing each sub-batch, the limits for the remaining ones
are adjusted downwards in accordance to the size of the last response. When a
limit is exhausted, the `DistSender` iteration stops, and all the requests that
haven't run are filled with empty responses.

At the level of a sub-batch, the limits are enforced on the server side.
Requests within a sub-batch are evaluated in the sub-batch order (which is the
same as the order of the original, unsplit `BatchRequest`). When a sub-batch's
limit is exhausted, the request that was evaluating gets a partial response,
and all the subsequent ones get empty responses. So, at the level of a
sub-batch, there can be at most one partial response.

When the limit is exceeded, at the level of the whole batch there are 4
categories of requests:
1) Requests that were completely satisfied.
2) Requests that were not evaluated at all.
3) At most one request for which a sub-batch returned a partial response.
4) Requests for which some sub-batches were completely satisfied, but other
sub-batches didn't get to run at all.
   
Category 4 is the insidious reason why we can have multiple requests with
partial results when the requests are overlapping: for these requests, the
DistSender fills in the `ResumeSpan` to point to the sub-batches that haven't
run yet. An example:

```
Ranges: [a, b), [b, c), [c, d)
Keys: a1, a2, a3, b1, b2, b3, c1, c2, c3
Batch: {Scan([a,d)), Scan([b,g), Scan([c,d)))} key limit 7
Results: Scan([a,d)) -> a1,a2,a3,b1,b2,b3; ResumeSpan: [c,d)
         Scan([b,g)) -> b1                 ResumeSpan: [next(b1), g)
         Scan([c,d)) -> empty              ResumeSpan: [c,d)
 ```
In this example we see two partial responses and an empty one.

It's interesting to discuss a particular difficulty of the current API: it
doesn't permit multiple `ResumeSpans` per `ScanResponse`, and it doesn't permit
a `ResumeSpan` in the middle of the results of a `Scan` (in other words, the
`ResumeSpan.EndKey` must always be equal to the scan's `EndKey`; we cannot
return results that look like `a1, ResumeSpan [next(a1), b), b1, b2, b3,
ResumeSpan [next(b3), d)`). So, if a `Scan` were to be split into two sub-scans
(for two ranges), and the first one would return a `ResumeSpan`, then the
results of the second one would probably need to be thrown away (or, at least,
it's not clear what to do with them). Of course, at the moment we don't have
this problem since a limited `Scan` is never split into concurrently-executing
sub-scans, but we'll return to this issue.


#  A stream-oriented API

This RFC is proposing the introduction of a new library/API for the benefit of
the lookup joiner (and possibly others) - the `Streamer`.

Requirements:
1. Submit requests and receive results in a streaming fashion.
2. Integrate with a memory budget tracker and don't allow in-flight results to
   exceed the budget.
3. Dynamically tune the parallelism of the requests such that throughput is
   maximized while staying under the memory budget.
4. Achieve parity with or exceed the optimizations currently present in the
  combination of `joinReader` + `joinReaderIndexJoinStrategy/jrNoOrderingStrategy/jrOrderingStrategy`.
   
For 2), we'll build on the existing memory limit in the `BatchRequest` API. 

For 3), we'll parallelize above the `DistSender` by sending multiple read
`BatchRequests` in parallel. To overcome the general limitations of the
`TxnCoordSender` which generally doesn't like concurrent requests, we'll use
`LeafTxns`.

For 4), we'll teach the `Streamer` to sometimes buffer responses received out of
order, to sometimes sort requests in key order to take advantage of low-level
Pebble locality optimizations, and perhaps to sometimes cache responses to
short-circuit repeated lookups.

A basic decision that needs to be made is about the degree to which the library
integrates its memory management with its environment. To frame the discussion,
let's consider what is the hardest case to support for the library: the case
when the joiner needs to produce its joined rows in order (i.e. in input-side
order). For high throughput, we want the `Streamer` to parallelize its lookups,
which opens the door to results being delivered out of order. Somebody needs to
buffer these results, and so the question is who should do that? Should they be
buffered inside the `Streamer` or outside of it, in the `Streamer's` client?
While buffered, the heap footprint of these results needs to be tracked
somewhere. If it's the `Streamer` that's doing the buffering, then it seems that
their accounting can lay solely within the `Streamer`. If, on the other hand,
it's the client that's doing the buffering, then it seems that the footprint of
each of these results needs to be tracked continuously from the moment when the
Streamer requests it (at which point, the tracking is really a "reservation"),
and up to the point when the client makes that result available to GC.  
It seems attractive to do the buffering in the client and devise a scheme by
which the `Streamer's` budget is integrated with the client's budget: once a
client is done with a result (i.e. after that result is no longer out-of-order
and it has processed all prior results too), then it can release some bytes,
which would notify the `Streamer` that there's new budget available to play with
(i.e. to start new requests). This kind of integrated tracking would match the
lifetime of results in earnest. However, one requirement that pops up here is to
maintain the ability to discard out-of-order results when times are tough.
Imagine that the `Streamer` has requested keys 1..10, and it has received
results for 6..10. Maybe the values for 1..5 are really big, and so their
requests need to be re-executed with a bigger budget. As long as 6..10 (the
out-of-order results) are buffered, this higher budget is not available. What
the `Streamer` should do, it seems, is to throw away 6..10, and make as much
room as possible for 1..5. If 6..10 are buffered by the client, it seems
difficult for the `Streamer` to coordinate their discarding - we'd need to
introduce some sort of claw-back mechanism.

The `Streamer` also seems to be a better candidate for buffering because it
knows about the outstanding partial scans and their relative ordering. This
allows it to easily keep track of what scan is going to deliver the
head-of-line response, and when the head-of-line results can be flushed to the
client. If a client were to track this itself, it'd be burdened with a lot of
state.

The proposal is to do the buffering in the `Streamer`, but only buffer if the
client cannot consume out-of-order rows. So, the `Streamer` gets configured with
an `InOrder`/`OurOfOrder` execution mode, and buffering only happens in
`InOrder` mode. Even with buffering done in the `Streamer`, the memory tracking
for results doesn't end when results are delivered to the client. Instead, the
client needs to explicitly `Release()` the respective memory once the client is
either done with the result or, if the client wants to hold on to the result,
once the client has accounted for the memory in its own account. Note, however,
that the client is expected to `Release()` results quickly after it gets them;
the client is not allowed to block indefinitely in between receiving a result
and releasing its memory - thus preventing deadlocks.


## Library prototype

```golang
type Streamer interface {
	// SetMemoryBudget controls how much memory the Streamer is allowed to use.
	// The more memory it has, the higher its internal concurrency and throughput.
	SetMemoryBudget(Budget)
	
	// Hint can be used to hint the aggressiveness of the caching
	// policy. In particular, it can be used to disable caching when the client
	// knows that all looked-up keys are unique (e.g. in the case of an
	// index-join).
	Hint(StreamerHints)
	
	// SetOperationMode controls the order in which results are delivered to the
	//   client.
	//
	// InOrder: results are delivered in the order in which the requests were
	//   handed off to the Streamer. This mode forces the Streamer
	//   to buffer the results it produces through its internal out-of-order
	//   execution. Out-of-order results might have to be dropped (resulting in
	//   wasted/duplicate work) when the budget limit is reached and the size
	//   estimates that lead to too much OoO execution were wrong.
	//
	// OutOfOrder: results are delivered in the order in which they're produced.
	//   The caller can use the baggage field to associate a result with its
	//   corresponding request. This mode of operation lets the Streamer reuse the
	//   memory budget as quickly as possible; when possible, prefer OutOfOrder
	//   execution.
	SetOperationMode(InOrder/OutOfOrder)
	
	// Enqueue dispatches multiple requests for execution. Results are delivered
	// through the GetResults call. If keys is not nil, it needs to contain one ID
	// for each request; responses will reference that ID so that the client can
	// associate them to the requests. In OutOrOrder mode it's mandatory to
	// specify keys.
	//
	// Multiple requests can specify the same key. In this case, their respective
	// responses will also reference the same key. This is useful, for example,
	// for "range-based lookup joins" where multiple spans are read in the context
	// of the same input-side row (see multiSpanGenerator implementation of
	// rowexec.joinReaderSpanGenerator interface for more details).
	//
	// In InOrder mode, responses will be delivered in reqs order.
	//
	// Initially, enqueuing new requests while there are still requests in
	// progress from the previous invocation might be prohibited. But ultimately
	// this kind of pipelining should be permitted; see the Pipelining section.
	Enqueue(reqs []RequestUnion, keys []int)
	
	// GetResults blocks until at least one result is available. If the operation
	// mode is OutOfOrder, any result will do. For InOrder, only head-of-line
	// results will do.
	GetResults(context.Context) []Result
	
	// Cancel all in-flight operations; discard all buffered results if operating
	// in InOrder mode.
	Cancel()
	
	// TODO: some method for collecting transaction metadata to be sent along
	// on the DistSQL flow
}

type Result struct {
	// GetResp and ScanResp represent the result to a request. Only one of the two
	// will be populated.
	//
	// The responses are to be considered immutable; the Streamer might hold on to
	// the respective memory.
	GetResp roachpb.GetResponse
	// ScanResp can have a ResumeSpan in it. In that case, there will be a further
	// result with the continuation; that result will use the same Key.
	ScanResp roachpb.ScanResponse
	// If the Result represents a scan result, ScanComplete indicates whether this
	// is the last response for the respective scan, or if there are more
	// responses to come. In any case, ScanResp never contains partial rows (i.e.
	// a single row is never split into different Results).
	//
	// When running in InOrder mode, Results for a single scan will be delivered
	// in key order (in addition to results for different scans being delivered in
	// request order). When running in OutOfOrder mode, Results for a single scan
	// can be delivered out of key order (in addition to results for different
	// scans being delivered out of request order).
	ScanComplete bool
	// Keys identifies the requests that this Result satisfies. In OutOfOrder
	// mode, a single Result can satisfy multiple identical requests. In InOrder
	// mode a Result can only satisfy multiple consecutive requests.
	Keys []int
	// MemoryTok.Release() needs to be called by the recipient once it's not
	// referencing this Result any more. If this was the last (or only) reference
	// to this Result, the memory used by this Result is made available in the
	// Streamer's budget.
	//
	// Internally, Results are refcounted. Multiple Results referencing the same
	// GetResp/ScanResp can be returned from separate `GetResults()` calls, and
	// the Streamer internally does bufferring and caching of Results - which also
	// contributes to the refcounts.
	MemoryTok ResultMemoryToken
}

type RequestUnion struct {
	// Only one of the two is populated.
	Get *roachpb.GetRequest
	Scan *roachpb.ScanRequest
}

// ResultMemoryToken represents a handle to a Result's memory tracking. The
// recipient of a Result is required to call Release() when the Result is not in
// use any more so that its memory is returned to the Streamer's Budget.
//
// ResultMemoryToken is thread-safe.
type ResultMemoryToken interface {
	// Release decrements the refcount.
	Release()
}

// Budget abstracts the memory budget that is provided to a Streamer by its
// client.
type Budget interface {
	// Available returns how many bytes are currently available in the budget. The
	// answer can be negative, in case the Streamer has used un-budgeted memory
	// (e.g. one result was very large).
	Available() int64
	// Consume draws bytes from the available budget.
	Consume(bytes int64)
	// Release returns bytes to the available budget.
	Release(bytes in64)
	// WaitForBudget blocks until the next Release() call.
	WaitForBudget(context.Context)
	// Shrink lowers the memory reservation represented by this Budget, giving
	// memory back to the parent pool.
	//
	// giveBackBytes has to be below Available().
	Shrink(giveBackBytes int64)
}

type StreamerHints struct {
	// UniqueRequests tells the Streamer that the requests will be unique. As
	// such, there's no point in de-duping them or caching results.
	UniqueRequests bool
}
```

The `Streamer` will dynamically maintain an estimate for how many requests can
be in flight at a time such that their responses fit below the memory budget.
This estimate will start with a constant (say, assume that each response take
1KB), and go up and down as responses are actually received. We can also imagine
starting from an estimate provided by the query optimizer. Under-estimating the
sizes of responses generally will not lead to exceeding the budget, but will
lead to wasted work. There is one case where the budget can be exceeded - in
order to always assure progress, the `Streamer` will always have a request
in-flight guaranteed to return at least one row, corresponding to the oldest
request. This one row is exempt from budget checks. See the [Avoiding wasted
work section](#avoid-wasted-work) for details.


## Streamer implementation

On the inside, the `Streamer` has a couple of concerns:

1. Send `BatchRequests` to fulfill the requests. There is a fixed overhead
   per-batch, both on the client and on the server. As such, we don't want to
   naively create one `BatchRequest` per `GetRequest` if we can avoid it. 
2. Don't send too many `BatchRequests` at once. The `Streamer` will maintain an
   estimate of how large each row is (or, in the case of joins that are not 1-1,
   the estimate will be per lookup-side `Scan`). This estimate will dictate how
   many `BatchRequests` can be in-flight at any point; each `BatchRequests`
   weights differently depending on how many requests are inside it.
3. Make sure that under-estimates in step 2 don't cause the budget to be
   exceeded. So, even if we end up sending `BatchRequests` that we should not
   have sent at the same time, we don't want to blow up. Each `BatchRequest`
   will be assigned a `TargetBytes` equal to the `Streamer`'s estimate for that
   batch's responses. If any given batch exceeds its budget, then it will only
   return results for some (possibly zero) of its `Gets`. The `Streamer` will
   keep track of which `Gets` have been satisfied and which haven't, and the
   ones that have gotten no response will be part of the future batches.  
4. Don't waste too much work. Work gets wasted in two situations:
   1. When a batch ends up returning zero rows because its `TargetBytes` are
      exceeded by the very first row.
   2. In `InOrder` execution mode, when there's insufficient budget to
      efficiently gather results around the start of the queue because too many
      results from the end of the queue are buffered.
   
We've mostly discussed the `Streamer` performing `Gets` in this text, but a note
about it performing `Scans` in `OutOfOrder` mode is important: when operating in
`OutOfOrder` mode, the `Streamer's` ability to process multiple partial
`ScanResponses` for a single `ScanRequest` is a big improvement over the
`BatchRequest`'s model. Even if the `DistSender` would allow parallel execution
of sub-batches in limited `BatchRequests`, we'd probably be forced to throw away
results whenever a `Scan` is split into two sub-batches (two ranges), and the
first one returns a `ResumeSpan`. Imagine that the first sub-scan returns a
`ResumeSpan` and the 2nd is fully fulfilled. There's no way to return the
results from the 2nd sub-scan; the current `BatchRequest` api doesn't support
multiple resume spans per `ScanResponse`, or even a single `ResumeSpan` with
further results after it. Extending the `BatchRequest` API to support this seems
hard.

Note that, even in `InOrder` mode, we can get significant pipelining benefits
(compared to a `BatchRequest` execution model) because, even if the head of the
line lookup is slow, the joiner's input loop will still push requests for as
long as the input budget and the `Streamer` budgets allow. You can imagine that,
if rows in some tables are small, millions of out-of-order results can be
buffered while the head-of-line request is blocked.

### Result delivery modes: OutOfOrder vs InOrder (result buffering)

As hinted to before, the `Streamer` can be configured in one of two modes:
`InOrder` and `OutOfOrder`. `OutOfOrder` is the simpler one: results are
returned by the `Streamer` to the client in the order in which they're produced
(which order might not correspond to the request order). Results are 
buffered by the `Streamer` only until the client calls `GetResults()` to read
them. Results might, however, be cached by the `Streamer` even after that; see
the [Result caching](#result-caching) section.

The `OutOfOrder` mode would be used by the `joinReader` with the
`joinReaderNoOrderingPolicy`. It's also be used by the
`joinReaderIndexJoinStrategy` when ordering is not needed; currently,
`joinReaderIndexJoinStrategy` is ordering-agnostic and the `joinReader` above it
controls whether results will be delivered in input order or not by sorting (or
not) the lookup key spans.  
`OutOfOrder` would also be used by, say, a `TableReader` configured with 1000
spans but with no ordering requirement.

In `InOrder` mode, the `Streamer` internally buffers results so that they're
returned to the client in the order of requests. While buffered, results hold up
memory budget that could otherwise be used to increase the concurrency. Under
budget pressure, the buffer will be partially spilled to disk to free up memory.

`InOrder` mode would be used by the `joinReader` with the
`joinReaderOrderingPolicy`. Currently, this policy looks-up rows out of order,
then buffers looked-up rows to restore the desired order. This buffering inside
the `joinReader` would be replaced with buffering inside the `Streamer`. In
addition, `joinReaderOrderingPolicy` de-dupes lookup rows and fans out the
response for identical requests. In the `joinReader`, this de-duping only
happens at the level of an input chunk. We propose to keep a similar de-duping
mechanism in the `Streamer`. However, the `Streamer` will also have a result
cache with a scope beyond that of the de-duping mechanism. See the [Request
de-duping](#request-de-duping) and [Result caching section](#result-caching)
sections for details.

One thing to note is that, currently, the `joinReader's` buffering is rather
inflexible: it needs to buffer all the lookup responses (for an input chunk)
and, only once everything has been buffered, it starts emitting rows and
eventually clears the buffer. No rows are joined and emitted until after all the
lookups are done. In contrast, the `Streamer's` `InOrder` mode streams results
out of it, but only in the specified order.

The buffering would be implemented in such a way that multiple entries in the
buffer can share a `Result` - in case the requests are identical. To support
this, the buffered results will be refcounted (the same refcount incremented
also when a `Result` is returned to a client and also when a result is cached).

See the [Scan requests section](#scan-requests) for a discussion about how
partial scan results interact with the ordering modes.

### Request batching

To address point 1) above (amortize the fixed cost of a `BatchRequest`) we
should try to batch individual requests. This is why the `Streamer` offers a
batch-oriented `Enqueue()` interface (in fact, it's the only interface). One
`Enqueue()` call represents the window over which requests going to the same
range are grouped into a single `BatchRequest` using a best-effort
`RangeIterator`. We'll call one of these batches of requests that go to a single
range a "single-range batch". At the level of a single-range batch, we have the
opportunity to sort the requests in order to take advantage of locality
optimizations in Pebble.

There's a discussion to be had here about whether the `DistSender's` transparent
splitting of batches into per-range sub-batches is desirable by the `Streamer`.
Given that the `Streamer` tries to do its own splitting, should we still allow
the `DistSender` to do its own splitting? In other words, should we treat the
`Streamer's` splitting as best-effort, and accept that the `DistSender` might
hide our mistakes, at a performance cost? The performance cost consists of
un-necessary blocking: when the `DistSender` splits a batch into 100, it'll then
wait for all 100 sub-batches to return before returning any results, and it will
execute them one by one (because of the `TargetBytes` limit). More discussion
about this in the [Unresolved issues](#unresolved-issues) section.

It's worth noting that the batching described here and the budgets assigned to
different batches are two separate axes. As we'll describe in the [Memory budget
policy](#memory-budget-policy) section, a single-range request containing 10
lookups might be issued with a budget that we expect to only cover one lookup.

### Memory budget policy

Point 2) says that the `Streamer` should manage its budget *B* such that it
maximizes throughput and minimizes wasted work. To do that, the `Streamer` will
maintain an estimate *P* for the size of responses to individual requests. This
estimate can start from the assumption that each `Get/Scan` request returns,
say, 1KB worth of rows. Perhaps we'll evolve to be smarter and start with
Optimizer-provided estimates - size of rows, number of rows per response (this
is 1 in case of an index-join).

The `Streamer` will constantly maintain around *B/P* lookups in-flight. This
makes it very clear that there's a direct relationship between *B* and Streamer
throughput. Whenever there's budget available, the `Streamer` sends out new
requests (subject to Nagle, see below). In `InOrder` mode, we want to prioritize
lookup towards the head of the line, and so this *B/P* essentially determines a
sort of "reorder window": for example, if we have 1000 queued lookups, and *B/P*
is 100, then we'll issue lookups for the 100 requests at the head of line. Thus,
as *P* grows, the `Streamer's` execution becomes more and more "in order", as
the "out of order execution window" shrinks.

The requests are batched (into single-range batches), so the actual number of
in-flight RPCs will be smaller than *B/P*; a batch weighs according to the
number of requests in it (*n*). Each batch consumes some budget - *n * P*. This
budget constitutes its `TargetBytes`. When a response is received for a batch,
we can see if the `TargetBytes` estimate was over or under. Generally, responses
will use less than `TargetBytes`, and so we can immediately release `TargetBytes - actual size` 
back to the `Streamer's` budget. The bulk of the response's bytes
cannot be released immediately; they're only released once the respective
results have been delivered to the client (which can be delayed, according to
the ordering setting) or even later (see section [Accounting for results
memory](#accounting-for-results-memory)). The estimate *P* is updated as
responses come in - after the initial responses, *P* will be tracking the
average response size. In fact, the average will be a moving average to account
for dynamics where larger rows are gotten last because of `TargetBytes` limits.

In `InOrder` mode, the decision about what budget to give each single-range
batch is not easy, given that the batches can contain lookups that are not
within the current reorder window. For example, assume that the client has
queued up lookups for 1000 requests. With each request identified by its
position within the results needed for `InOrder` execution, assume that requests
`{1, 2, 1000}` fall on the same range - so they can be part of the same
single-range batch. Say that *B/P* = 100 (i.e. the reorder window is a hundred
lookups wide). Request 1000 is thus out of this window - meaning that, if
everything else was equal, we'd like to perform other lookups before it. But
everything else is not equal and, so, under this scenario, it's unclear whether
we should send the single-range batch as `{1,2}` or as `{1,2,1000}`. Furthermore,
if we send the batch as `{1,2,1000}`, it's unclear if the batch's budget should
be *2P* or *3P*. There are two choices:
1. Give a batch with *n* lookups a budget of *n * P* (subject to the currently
available budget, though). In other words, give it a budget that's expected to
be sufficient for all the lookups in the batch. Since we expect all the lookups
in the batch to be within budget, we can sort the lookups in key order such that
we exploit Pebble's locality optimizations.
2. Give a batch a budget reflecting only the number of lookups that are within
the reorder window (e.g. two in our example). So, we don't expect responses for
all of the lookups. We still send all the lookups as a form of opportunism: if
the results end up being small, we might as well satisfy lookups that we didn't
expect to satisfy. Since we don't expect responses for all the lookups, it
becomes important to *not* sort them in key order, and leave them ordered
according to their ordinals.
   
The upside of 1) is that we can do the sorting, which should improve throughput.
The downside is that we expect to buffer results outside the reorder window. The
upside of 2) is that we'd expect less head-of-line blocking: lookups are
performed more in-order, so the (constantly-advancing) head of the line can
generally be delivered quicker to the client.

I don't know how to make a principled choice here, so I propose we pick whatever
is easier to implement - which I think is option 1). So, the algorithm for
issuing batches is to iteratively (as long as there's budget) pick the next
request in line lookup and issue its whole batch with a budget enough for all of
its contents. We would always sort lookups within batches in key order, with one
exception: for the special case of the batch containing the current head-of-line
lookup, I would keep the head-of-line as the first request in the batch such
that, in the degenerate case where there's not enough budget for even a single
row (and thus we only get exactly one row back from this head-of-line batch),
that one row is the head-of-line row.

Because of `TargetBytes`, batch responses can be incomplete. The `Streamer` will
keep track of which requests have not gotten responses yet and these requests go
back into the request pool, waiting for budget to open up.

There's a tension between the desire to effectively batch requests into
single-range batches, and the desire to send requests as soon as there's some
budget available. Simply sending a `BatchRequest` with all the requests
targetting a range is not enough; that `BatchRequest` also needs to have a
decent budget. Otherwise, we might as well have sent the requests one by one. If
there's very little budget available, we're better off waiting for more budget
to become available. We could define a minimum budget required in order to send
a single-batch request, similarly to Nagle's algorithm for TCP. In particular,
we generally don't want to send out requests with a budget less than *P* (and
maybe actually much more than that).

#### Accounting for results memory

This section talks about the lifecycle of the memory taken by `Results`. Each
request performed by the `Streamer` starts up with a memory reservation of *P*
(except in the degenerate case when the request is made for a single row because
there's no budget). Once a response is received, the reservation is adjusted to
the actual size of the results. Generally, the adjustment will be in the
direction a bigger reservation giving back unused memory. Results can only
exceed their reservation in the context of the head-of-the-line request, which
is configured to return a single row no matter the size (see the [Avoiding wasted work](#avoiding-wasted-work) section for details).

Once a particular result is received, the ownership of its memory might be shared:
- when the `Result` is returned to the client (through `GetResults()`), the
client takes a reference
- if we're buffering results (`InOrder` execution), the buffer takes a reference
(or possibly multiple references if the `Result` satisifies multiple duplicate
requests)
- if the result is cached, the cache takes a reference

For its part, the client is responsable for destroying its reference when it's
done with the `Result`. It does this through the `MemoryToken` encorporated in
the `Result`. This is generally expected to happen very soon after the
`GetResults()` call - the caller will generally take over the results under its
own accounting/budget quickly. The caller is, however, allowed to delay - but
not indefinitely, as the `Streamer` will starve. We've considered an interface
by which the `Streamer` could call into the client and ask the client to return
memory to the budget, but dropped that as it didn't seem necessary if we give
the `Streamer` the ability to cache results internally.

### Avoiding wasted work

Work gets wasted in two cases:
1. When a batch ends up returning zero rows because its `TargetBytes` are
   exceeded by the very first row.
2. In `InOrder` execution mode, when there's insufficient budget to
   efficiently gather results around the start of the queue because too many
   results from the end of the queue are buffered.

Number 1) assumes that a request with `TargetBytes` can return zero rows. That's
not currently the case - a request with `TargetBytes` returns at least one row
even if that row overshoots the budget. We're going to expand the API to give
the caller control over this behavior. We're going to make it such that, when a
`TargetBytes`-batch returns no results, it still returns an estimate of the size
of a row. More generally, whenever `TargetBytes` caused the request to be
truncated, the response is going to include the size of the next row so that the
`Streamer` can use it for subsequent requests.

In order to minimize 1), we're going to still keep the ability to ask for at
least one row to be returned. This option is going to be used for the batch
containing the head-of-line request. Doing so ensures that the `Streamer` is
constantly making progress: the head-of-line request (which, in `OutOfOrder`
mode, is an arbitrarily-chosen request) is guaranteed to always return at least
one row. In addition, as an exception to the Nagle clause from the [Memory
budget policy](#memory-budget-policy) section, the oldest request that the
client has submitted is sent out even if the current *P* estimate says that we
don't expect to have room for even one row. However, we'll only do this when the
`Streamer` has its full budget at its disposal - i.e. after the client has
released the budget for all responses that were handed to it, and after we've
spilled the buffer to disk (see below). The idea is that, if the `Streamer`'s
whole budget is 10MB, and a single row is `20MB` large, we want to read that
row.

Let's discuss 2): the "focusing on the front of the line" is hampered by the
buffering in the `InOrder` case. Since the buffer takes up budget, the more
results are buffered, the lower the throughput for the request towards the front
of the queue. At the limit, if there's no budget left, the `Streamer` is forced
to either throw buffered results away (and redo the work of retrieving them
later), or to spill the buffer to disk. We could consider the disk to not be
infinite, so there's also a possible hybrid answer where we spill to disk up to
some disk budget, after which we start throwing results away.

The proposal is to start by spilling to disk with no disk-budget limit. Assume
that there's no budget left, there's a front of the line of *n* unsatisfied
requests, and there's a buffer of *m* responses. We'll spill to disk as many of
these *m* results as necessary to free up *n * P* bytes. Responses will be
spilled in the order presented in the [Buffer management
details](#buffer-management-details) section. In other words, buffered results
will start spilling as soon as it appears that there's not enough budget for all
the requests in front of them. This is not the only possible option; at the
extreme, as long as there's budget for one row, we could read one row at a time
(the first one), deliver it to the client, read the next row, etc., without
spilling the buffer.

When some buffered results need to be spilled, they should be spilled in the
order of frequency of access (lowest spill first). In other words, if the
de-duping process (see the [Request de-duping](#request-de-duping) section)
established that entry `a` satisfies 10 lookups and entry `b` only satisfies 1,
then `b` should spill first.

### Request de-duping

The `joinReader`, when combined with the `OrderingStrategy` or
`NoOrderingStrategy`, has an interesting optimization: at the level of an input
chunk, the lookup keys are de-duplicated. For example, consider a joiner that
needs to perform lookups for input rows `(1, red), (2, blue), (3, red)` (the
lookups are to be performed on the 2nd column) Notice that the 1st and the 3rd
input rows share the lookup key "red". The joiner will perform the `red` lookup
only once. Depending on `Ordering` vs `NoOrdering`, this de-duplication has
major implications: in `NoOrdering` mode, things are easy - a result is
fanned-out to the duplicate requests, and joined rows for rows 1 and 3 are
emitted immediately. In `Ordering` mode, however, the joiner cannot emit row 3
immediately. Instead, the `joiner` takes advantage of the buffering of looked-up
rows that it does anyway (to handle re-ordered results); the `red` lookup
result is thus buffered (like all the other results) in a disk-backed
container and used after the result for row 2 was emitted. We propose to keep
this de-duping behavior in the `Streamer`.

In the `joinReader`, currently, the de-duping is limited to requests within the
same input chunk: if the 1st chunk has 1000 lookups for key "a", they'll all get
coalesced, but then if the 2nd chunk wants key "a" again, that'll be a 2nd
lookup. The `Streamer's` deduplication, by itself, has the same issue: it
operates at the level of `Enqueue()` batches. As an extension, the proposal is
for the `Streamer` to also get a response cache that's not tied to these
batches. This would be in addition to the de-duping, and in addition to the
buffering that the `Streamer` does for a different purpose. A cached response
short-circuits a new request for it. Details in the [Result
caching](#result-caching) section.

## Scan requests

When the `Streamer` is performing `Scans` (as opposed to `Gets`), the `Streamer`
will split the `Scan` into per-range sub-scans. This is useful when these scans
are individually large - for example imagine FedEx running the query
```sql
SELECT * FROM shipments INNER JOIN shippers ON shipment.shipperID = shippers.ID
WHERE shippers.name = 'Amazon'
```

*P* will represent the estimate about the size of the complete results of every
sub-scan.

When the `Streamer` is performing `Scans` (as opposed to `Gets`), we need to
discuss the order of delivering partial results for a single `Scan`. For
simplicity, we make this order match the order in which results for different
`Scans` are delivered: in `InOrder` mode, results for a single scan are
delivered in key order (i.e. the sub-scans are re-assembled). In `OutOfOrder`
mode, results for a single `Scan` can be delivered out of order (i.e. result
from different sub-scans can be inter-mixed). In any case, individual rows are
never split between partial results (TODO: is this already the behavior of
`TargetBytes` or does that need tweaking?). In other words, it's not possible to
ask the `Streamer` to produce results in-order for different requests but
out-of-order at the level of a single `Scan`, or vice-versa.

As explained in a prior section, the fact that, in `OutOfOrder` mode, the
`Streamer` can return out-of-order partial results for different `Scans` is an
improvement over what one might hope to get out of the `DistSender`. The key
here is that, for the `Streamer`, the client can ask for this `OutOfOrder`
execution.

The `Streamer` will interpret the `ResumeSpans` of its sub-scans and keep track
of when each sub-scan, and when each `Scan`, is complete.

# Integration between the Streamers' budgets and the broader environment

So far we've discussed how a `Streamer` is configured with a budget, how it uses
its budget internally, and how the budget can be shared with its caller. We
haven't touched on how this budget fits into the broader server - how a
`Streamer` affects the general memory usage of the server and how different
`Streamers` affect each other. This is what this section is about.

 It seems there are two things we want from the `Streamer's` integration into
 the broader memory management story:

1) Memory used by a `Streamer` should act as push-back on traffic in general
   (think admission control).
2) `Streamers` start out with budgets which represent memory reservations. The
   point of this budget being generous is to allow the `Streamer` to provide
   good throughput to its client. If an individual `Streamer` figures out that
   it can sustain good throughput with a lesser budget, it should give part of
   the reservation back to the broader server.

Point 1) is generally how our memory monitors work - they form hierarchies that
have a common root pool corresponding to the machine's RAM. As one monitor pulls
more memory from this root, there's less memory for the others and allocations
fail when there's not enough memory in the pool. The Streamer's budget are no
exception - they come from some pool. One peculiarity of the `Streamers` is,
though, that we said that they're allowed to go "into memory debt" - they're
allowed to use memory that they don't have a reservation for because they're
allowed to always read one more row. The amount of debt a `Streamer` can go in
is limited by the throughput that it sustains when operating in
one-row-at-a-time mode. We accept a `Streamer` going into debt because the
alternative is deadlock. One thing we can do, though, is make sure that this
debt acts as pushback on the server, by accounting the debt in the memory pool
from which the `Streamer's` original budget came. So, if a `Streamer`
accumulates a great debt, as some point the respective node will stop accepting
new queries in order to prevent OOMs. Of course, if many `Streamers` go into
debt at the same time, we will OOM - so debt's not great.

Point 2) is about improving on the budget reservation tradeoff. When the budget
for a `Streamer` is created, the creator reserves a quantity of memory. The more
it reserves, the more likely that the `Streamer` will provide good throughput.
So, you don't want to reserve too little. But you also don't want to reserve too
much, particularly if the reservation is going to be long-lived. The `Streamer`
might not need all the memory in order to "provide good throughput". So, we propose that
the `Streamer` is able to give back some of this reservation. This is what the
`Budget.Shrink()` method is about. The `Streamer` will do so if it detects that
it can provide good throughput with less memory. What is "good throughput" / how
good is good enough? I don't really know. I guess that `Streamer` can detect the
client's consuming pace after a warmup period and, if it finds itself buffering
results that are ready to be delivered, it can decide to lower its
concurrency/throughput by shrinking its budget by some factor. In theory, the
consumer's consumption rate is limited by its own consumers downstream -
sometimes there might be CPU limits for (single-threaded) CPU-heavy computation,
sometimes some results need to make it over the network in a DistSQL stream,
sometimes results need to be joined with another slow flow, and ultimately the
client application controls the pace for result-heavy queries through its pgwire
reading rate.

## Hiding ReadWithinUncertaintyInterval errors

The `Streamer` will use `LeafTxns` in order to support sending concurrent read
requests. `LeafTxns`
[disable](https://github.com/cockroachdb/cockroach/blob/cfb4b95d94e717e985423d5313c58187fbde5da4/pkg/kv/kvclient/kvcoord/txn_coord_sender.go#L302)
the automatic refreshing of read spans and the `BatchRequest`-level retries done
on retriable errors because, usually, `LeafTxns` don't have a complete view of
the read spans and also because refreshing cannot be performed during concurrent
requests. This is generally a problem for DistSQL
([#24798](https://github.com/cockroachdb/cockroach/issues/24798)).

Since both in the general DistSQL case and in the `Streamer` case we're dealing
only with reads, the only possible retriable error is
`ReadWithinUncertaintyInterval`. It's a frequent condition, though.

In the general case of a distributed DistSQL flow, figuring out how to refresh
transparently is a hard problem. But it should be tractable when the query is
not actually distributed (i.e. when there's a single `Streamer`, on a single
node - on the gateway, most likely). We shouldn't allow for a regression here;
instead, we'll make it possible for the `Streamer` to coordinate refreshes.

Depending on the topology of the overall DistSQL flow using a `Streamer` (or
many of them), the `Streamer` will be configured with either a `root` or `leaf`
txn. Similarly to the DistSQL processors, the `Streamer` will be configured with
a `root` if nobody else uses the `client.Txn` concurrently with the `Streamer` -
neither on other nodes (through DistSQL distribution) nor within one node
(through concurrency between DistSQL processors that haven't been fused, or
through concurrency between the `Streamer` and the `Streamer's` client - i.e.
pipelining. It is this `root` txn case that we want to optimize; the `leaf` case
is harder.

Regardless of whether the `Streamer` is handed a `root` or a `leaf` txn, it will
perform its operations in a `leaf` in order to support concurrency. Given that
the `Streamer` has full visibility across all the requests being performed, it
can overcome the limitations of a `LeafTxn` and get back the ability to refresh.
In order to do so, it needs to implement synchronization between requests being
sent out and `ReadWithinUncertaintyErrors` being returned. Whenever an error is
returned, the `Streamer` will make that error act as a request barrier:
in-flight requests will be drained out and future requests will only be sent out
if and when the error has been dealt with. Dealing with the error means
collecting all the read spans from all the leaves (after in-flight requests have
been drained), importing them into the `root`, and asking the `root` to refresh
the txn to the timestamp indicated by the error. This "ask to refresh" will be
done through a new `client.Txn` interface. If the refresh is successful, the
`Streamer` can retry the request that generated the error, and resume normal
operation. If the refresh is not successful, the retriable error needs to be
bubbled to the client.

If multiple concurrent requests encounter retriable errors, the refresh needs to
be done up to the high-water timestamp of the errors and, on success, all such
requests need to be retried. As soon as an error is received, the `Streamer`
also has the option of canceling any in-flight request and retrying them all
after the refresh.

## The Streamer and the joinReader

This section enumerates the modes in which the `Streamer` will be used by the
`joinReader`, as a reference.

1. `IndexJoinStrategy` -> `InOrder` or `OutOfOrder`, depending on the index-join's
ordering requirements. Caching will be inhibited, since lookup keys are unique,
being PK values. Similarly, de-duping is inhibited.

2. `NoOrderingStrategy` -> `OutOfOrder`. The `Streamer` in `OutOfOrder` mode
internally de-dupes lookup spans for concurrent lookups and caches results.
Being `OutOfOrder`, there's no buffering of results (but there might be
caching). Being `OutOfOrder`, the `Streamer` returns results in whatever order
its concurrent lookups finish.

3. `OrderingStrategy` -> `InOrder`. Results cannot be coalesced (except in the
special case of consecutive identical requests). The `Streamer` does buffering,
de-duping, and caching.


# Optional features

## Pipelining

Pipelining refers to the ability to call `Streamer.Enqueue()` repeatedly without
waiting for all results pertaining to the previous invocation to be delivered.
As hinted before, allowing this in `OutOfOrder` mode could be very beneficial in
hiding the latency of a couple of slow single-range batches (think lock
contention). Even in `InOrder` mode, pipelining lookups can be beneficial, even
if a slow request does block the delivery of results (but it doesn't need to
immediately block performing and buffering new lookups).

I think the `Streamer` should offer pipelinig, although not necessarily from
the start. My hope is that, depending on the details of the implementation,
pipelining would come pretty much for free: given that the `Streamer` by nature
needs to keep track of requests that are in different phases of completion, does
adding new requests at arbitrary times cause extra complexity? Sumeer brought up
the case of the inverted-index joiner, which wants to send overlapping (but not
necessarily identical) scans. In that case, having a complete view of all the
requests to perform can be useful because the `Streamer` could intersect all the
requests in order to decide the granularity of shared spans for buffering
reasons. Of course, if "complete view" only means "complete within one
`Enqueue()` call", is that useful / would we really be worse off if we allow
pipelining but some buffering opportunities are missed? See the [Overlapping
scans](#overlapping-scans) section for more.

There's an interesting question about how exactly would a component like the joiner
take advantage of the `Streamer's` pipelining capability. The `joinReader` is
not a vectorized processor; it uses the `RowSource` interface for reading its
input-side. `RowSource` returns one row at a time and is a blocking interface.
So, how is the `joiner` supposed to build a batch of input rows in order to use
`Streamer.BatchGet()`? The way in which it currently does it is not great - the
`joinReader` decided how big of a batch it wants to accumulate and then blocks
until it fills up that batch. What we'd want instead is for the `joinReader` to
read its input until it blocks and, once blocked, perform lookups on the
accumulated batch. Should we add a non-blocking version of `RowSource.Next()`?

## Result caching

A feature that could prove quite useful in some cases: attaching a result cache
to the `Streamer` in order to avoid repeated identical lookups across
`Enqueue()` calls (at the level of a single call, duplicates are handled through
the de-duping + buffering mechanism).

Frequently accessed (or perhaps frequently and recently accessed) results would
be kept in this cache, memory permitting. In case the number of distinct lookups
is small, this cache could completely eliminate any lookups for whole
`Enqueue()` calls. One heuristic for populating this cache could be to keep
around the top k most popular lookups from the last `Enqueue()` batch.

In case the `Streamer` is buffering (i.e. in `InOrder` mode), there's some
overlap between the cache and the buffer that we'll discuss below.

A fair question would be what makes the `Streamer` a reasonable level to place a
cache at. There are other options with their own merits - for example we could
imagine a cache sitting at the `client.Txn` level. I'll give a few answers, but
I don't know if they're very convincing:

1. Currently, the `joinReader` does request de-duping across relatively big
   batches (between 10KB and 4MB worth of input rows, depending on the type of
   join). The `Streamer` is imagined to frequently operate over smaller windows of
   input rows, and so there's the possibility that we'd be regressing (in terms of
   work performed) if we don't have a way to avoid repeating lookups over longer
   windows.

2. In addition, one thing that makes caching at this level seem like a
   reasonable choice is the fact that, when the `Streamer` is used with a joiner,
   all the lookups will be performed on the same table/index - so it seems
   reasonable to expect that there'll be collisions.

3. The `Streamer` aims to have relatively sophisticated management of its memory
   budget, so putting the cache under its purview lets this management extend to
   the cache too.

4. Since the `Streamer` performs request de-duping, it has a view into what keys
   were de-duped that lower levels wouldn't have. This can serve as input in its
   choice of what result seems more valuable to cache.

When the `Streamer` is under memory pressure, the cache (or parts of it) can be
dropped or spilled to disk, similarly to buffered rows.

Results caching become even more important if we implement support for
pipelining in the `Streamer` (see the [Pipelining](#pipelining) section). More
generally, the smaller the batches handed to `Enqueue()` (and thus the smaller
the opportunity to de-dup requests within a batch), the more important it
becomes to share results across batches through caching.

#### Buffer vs cache

In case the `Streamer` is buffering (i.e. in `InOrder` mode), there's some
overlap between the buffer and the cache. Both structures hold results in order
to avoid doing work in the future. In the case of the buffer, the future work is
"guaranteed" - we know that there's a request that will need that result. In the
case of the cache, the future work is speculated.

A newly-arriving request will be checked against both the buffer and the cache.
If the result is found in the buffer, a new buffered entry is created
referencing the existing `Result`. If the result is found in the cache, it is
moved into the buffer (or shared between the cache and the buffer? I'm not sure
about the mechanics here; whose budget should this shared result count towards
now?).

When it comes to clearing the buffer and cache because of memory pressure, it
stands to reason that we'd clear the cache first.

TODO: figure out the actual caching policy and the cache's memory budget

## Overlapping scans

The geospatial inverted-index joiner does lookups on overlapping (and not
necessarily identical) spans. This raises interesting complications for the
buffering logic:

1. Buffering becomes useful even in the `OutOfOrder` mode. Consider, for
example, the following scans: `{[a,b), [a,c)}`. If the `Streamer` gets a result
for `[a,b)`, the full result for `[a,c)` is not yet ready, but the `[a,b)` part
is useful; we don't need to redo that scan.
   
2. "De-duping" needs to be extended: instead of two requests being identical or
disjoint, now they can be overlapping.
   
The `Streamer` would handle these cases by intersecting all queued requests, and
splitting out the common parts. By giving the common parts their own identity,
the `Streamer` would be able to buffer and/or cache them.

# Unresolved issues

1. Even with the `Streamer` learning how to cache results internally for reuse,
there seem to be cases where the client wants to emit `Streamer` results in
order, but there's still possibly benefits to delivering the results
out-of-order from the `Streamer` and buffering in the client. If, say, a
`TableReader` has a selective filter which discard most of the rows, we'd
benefit from running this filter on each result as quickly as possible to
quickly discard most results so that they don't hold up memory while buffered.
What is this `TableReader` supposed to do? It could put the `Streamer` in
`OutOfOrder` mode, but then the `TableReader` would need its own budget for
buffer, separate from the `Streamer's`. It seems tempting to have the
`TableReader` not release memory back to the `Streamer`, but that's going to
starve the `Streamer`. To avoid that, we'd need to bring back the claw-back
mechanism from the `Streamer` to the client that an earlier version of the RFC
had. Better still, perhaps we should leave the `Streamer` in `InOrder` mode and
push the filter into the `Streamer` in the form of a per-result callback?
   
2. Should the `Streamer` disable the `DistSender's` transparent splitting of
`BatchRequests` into per-range sub-batches? Given that the `Streamer` tries to do
its own splitting, should we still allow the `DistSender` to do its own
splitting? In other words, should we treat the `Streamer's` splitting as
best-effort, and accept that the `DistSender` might hide our mistakes, at a
performance cost? The performance cost consists of un-necessary blocking: when
the `DistSender` splits a batch into 100, it'll then wait for all 100
sub-batches to return before returning any results, and it will execute them one
by one (because of the `TargetBytes` limit).  
An alternative is to have the `Streamer` disable the `DistSender` transparent
splitting (through a new mechanism) and instead opt-into getting errors for
`BatchRequests` that need splitting. When receiving such an error, the
`Streamer` could rely on the range cache used by its range iterator to have been
updated with the split range, and thus the `Streamer` could split the batch
again with better results.
   
3. What's there to be done for cases where we have a hard or a soft limit on the
total number of rows to be returned by the `Streamer`. Can the `Streamer` set
some `MaxSpanRequestKeys` on the requests? This is tracked in
[#67885](https://github.com/cockroachdb/cockroach/issues/67885).

# Alternatives considered

When [#54680](https://github.com/cockroachdb/cockroach/pull/54680) was filed, it
seemed to be implied for a while that whatever we'll do, we'll do it at the
`DistSender` level. Doing something at that level seems attractive because, in
principle, it'd benefit all the `BatchRequest` users. But when you dig into the
details, exactly what the `DistSender` should do becomes overwhelming. By
introducing the `Streamer`, we force clients to explicitly be able to deal with
partial results, we allow clients more control over their ordering needs (and
the associated costs), and we eliminate the barrier behavior of big
`BatchRequests`.
