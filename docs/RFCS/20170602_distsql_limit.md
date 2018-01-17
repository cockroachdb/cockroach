- Feature Name: Limit handling in DistSQL
- Status: in-progress
- Start Date: 2017-06-02
- Authors: Radu Berinde
- RFC PR: [#16287](https://github.com/cockroachdb/cockroach/pull/16287)
- Cockroach Issue: (one or more # from the issue tracker)

# Summary

This RFC proposes a set of mechanisms for efficiently dealing with `LIMIT` in
DistSQL.

# Motivation

First, we describe the current state of things.

### Local SQL ###

There are two types of limits that can apply to a planNode:
 - a *hard limit* is one where we know we only need **X** rows from a node.
   Example: `SELECT * FROM t LIMIT 10`
 - a *soft limit* is one where we have only an optimistic estimation for how
   many rows we need. Notable cases which lead to soft limits:

   * limits with filters. `SELECT * FROM t WHERE color = 'red' LIMIT 10`. Here
     the limit of 10 at the scanNode is soft because we may need more rows if
     some don't pass the filter.

   * partial orders: `SELECT * FROM t ORDER BY pk1,val1 LIMIT 10`: we need to
     sort but we already have a partial order (let's assume the table is ordered
     by `pk1,pk2`). If our sort can take advantage of the partial order (and
     only sort groups of rows that are equal on `pk1`), we only need to read 10
     rows plus any rows that have the same `pk1` value as the 10th row. This
     would ideally lead to a soft limit of 10 or slightly more on the
     `scanNode`.

     *Note: The DistSQL sorter is aware of partial orders but the local SQL
     sortNode is not, so this is not actually implemented yet.*

How are the limits actually handled at the scanNode level? The row fetcher
receives a "hint" about how many rows we need. If we have a hard limit of **X**,
this is simply **X**; if we have a soft limit of **Y** this is currently **2Y**.
In the future the multiplier here needs to be determined based on statistics
(e.g. an estimation of the selectivity of a filter). The row fetcher calculates
the maximum number of keys per row, based on the family configuration (or simply
1 key per row for secondary indexes) and sizes the first KV batch accordingly.
The scanNode returns all rows before scanning another batch. In the case of hard
limits, we never need to scan another batch. In the case of a soft limit, we
could; if that happens, the second batch is significantly larger. The third
and subsequent batches have the maximum batch size (like unlimited scans). The
growth of the batch sizes is a trade-off between performance in "good" scenarios
(when we are close to the soft limit) and in "bad" scenarios (if the soft limit
estimation turns out to be way off).

### Distributed SQL ###

DistSQL supports limits semantically: it can run any query with limits and it
will produce the correct results. However, there are hurdles in implementing the
kind of optimizations explained above, which can lead to dramatic performance
degradation in many cases:
 - DistSQL processors (a processor is roughly one of potentially multiple
   instances corresponding to a planNode) output rows to streams, in parallel
   with the consumers of those streams. We have flow control on streams -
   provided by buffered channels on local streams, and by gRPC on remote
   streams; the gRPC window sizes are quite large, necessary for high
   throughput/high latency links. These buffering effects can cause a processor
   to emit more rows than needed before the consumer even has a chance to
   receive any of them. There is no sequencing step where a processor can stop
   and wait and see if the consumer needs more rows. The TableReaders use the
   same row fetching mechanism explained above, which means we will almost
   always read a second batch even if we don't need to.
 - there are multiple instances of TableReaders for a given table, each working
   on the ranges held by that node. Without a special way to plan limits, we
   would read **X** rows from every node.
 - the planning process itself has to go through all the ranges in a table,
   which may be comparatively expensive if we only need to read a few rows
   (especially as we scale up in cluster and table size).

As a consequence, by default we don't use DistSQL for statements which involve
limits that get propagated all the way to the scanNode. Note that there are 
useful classes of statements that have limits at a higher level but where we
can't put an (even optimistic) limit on scanNodes; for example
`SELECT * FROM t ORDER BY v LIMIT 10`, where we have to read the entire table in
all cases. These classes of queries are run via DistSQL.

The goal for this RFC is to find ways to work around the hurdles above to
achieve efficiency comparable with local SQL in all limit cases.
Specifically:
 - read no more than **X** rows in hard limit cases, and only involve the
   respective hosts in the computation;
 - don't execute more batches than needed in soft limit cases.

In addition, we want our solution to work well for the partial ordering case
described above.

The main focus here is not performance in terms of the latency of a single
query, but avoiding wasted work (which generally translates to better
overall throughput).

# Detailed design

The proposal has two main aspects: new ways to plan limit queries, and a
mechanism for limiting batches for soft limits.

## Planning ##

We have two proposals here: one is simple and should be implemented in the
short-term; the other is more complex and should be considered in the long
(medium?) term.

### Short-term proposal ###

The simple proposal is to not distribute TableReaders for limited scans.
Instead, plan a single TableReader on the host that holds the first range to
scan. In the best case, we will only need to scan that first range. In the worst
case, we may end up reading the entire table, and many of the reads will be
remote reads.

We believe that this solution is good for many common cases. It is at least as
good as local SQL for queries that only read rows from a single range. It also
has the advantage that we only need to resolve one range during planning. The
main problem is that in bad cases the table data will be "routed" through that
first node; this can be very bad if that first node happens to be in a far away
zone. This will need to be addressed as we become more optimized for widely
distributed scenarios.

There is also a possibility that the node holding the first range will become a
hotspot if we have many similar queries running concurrently. We may need the
planning process to be sensitive to load and minimize the amount of computation
happening on that node if it is overloaded.

Note that there will be a cut-off for what we consider a "limited scan" - a very
large `LIMIT` will be treated like an unlimited scan.

#### When order is not required ####

In cases like `SELECT * FROM t LIMIT 10`, we can return any 10 rows; so we are
free to read ranges in any order. An optimization to the proposed solution is to
place the single TableReader on one of the nodes that have many range leases for
the table, and read from those ranges first. In widely distributed clusters, we
can always choose among the nodes that are close to the gateway. We may need the
node choice to be sensitive to load; e.g. choose the least loaded node that has
some range leases; or choose randomly according to a weighted distribution where
each node's weight is proportional to the number of range leases and inversely
proportional with load.

### Long-term proposal ###

The second proposal is to build a new *planning synchronizer*. A synchronizer is
an existing concept in DistSQL: it merges multiple input streams into a single
stream that a processor can use. This new synchronizer will operate as follows:
 - it initially has one input stream (normally connected to a TableReader
   configured to read the first block of ranges held by a node);
 - once this stream completes (i.e. producer has no more rows), the synchronizer
   (which is pre-configured with a list of spans) resolves the spans into the
   next batch of ranges, and sets up a processor on the corresponding node;
 - the synchronizer can proceed in this manner, setting up a new processor when
   the last one completes; or it can do more advanced things like set up new
   processors before the last stream completes, or set up multiple new
   processors at the same time. It can monitor the number of rows passing
   through in relation to the limit to make these decisions.

Implementing this will require infrastructure for augmenting running flows with
new processors (or running multiple flows per node for the same query). Note
that we already need something along those lines for tolerating dying hosts.

This solution comes with a planning question: on which node do we put this
planning synchronizer? A reasonable first implementation is to always use the
gateway node. More advanced decisions are possible here though (e.g. if the
table is confined to a zone, the synchronizer should be in that
zone).

#### When order is not required ####

When order does not matter, we order the streams so that we start with the
closer nodes first, and among them with nodes that have more ranges.

## Soft limit mechanism ##

In addition to the new planning methods, we introduce a mechanism that allows
processors to "pause" their work until the consumer tells us that it needs more
data. This works as follows:
 - once a TableReader with a soft limit sends all the rows in the current KV
   batch, it sends a special control message that means "I may have more rows,
   but I'm waiting for you to tell me you need them".
 - a processor that receives this message can send back a control message
   informing the producer that more rows are needed.
 - all processors need to be able to handle this message and forward it to their
   consumers as necessary. In many cases the processors can forward this to
   their consumer and then react to the consumer's request for more rows. In
   other cases (e.g. sorting with no partial order, aggregation), the processor
   will always need to request more rows (this can also be used as a simple but
   correct "default" implementation to allow incremental implementation.

Note that we already have infrastructure to send control messages over processor
streams (in both directions).

One thing that we will need for implementing this is to enrich the RowFetcher
interface to give the caller hints about batch boundaries. This is useful for
local SQL as well: index-joins would benefit from aligning primary table batch
boundaries with index batch boundaries.

# Drawbacks

# Alternatives

#### When order is not required ####

In cases like `SELECT * FROM t LIMIT 10`, we could start processors on all nodes
that have ranges and stop the flow after the first 10 rows are received. This
may achieve better latency but it will have a (probably larger) negative impact
on overall throughput - we would waste a lot of work reading rows that we throw
away, and we set up operations on more nodes than we have to. We may consider
this kind of plan for some cases (perhaps tunable by a knob).

# Unresolved questions
