- Feature Name: resource bounds
- Status: draft
- Start Date: 2016-07-06
- Authors: knz
- RFC PR: 
- Cockroach Issue: #2524 #7572 #7573 #7608 - perhaps also #5807, #5968, #243

# Summary

We need to bound the resources (time, memory) used by SQL
queries. This RFC is intended to:

- provide a high-level overview of the problem;
- outline the proposed *mechanisms*;
- analyze and discuss the alternative *policies* and pick
  one for a first implementation.

Note that the discussion in this RFC was started by
usage of main memory, but would be applicable just as well even
if the per-query/per-session temp data was stored on disk (e.g. via #5807)

# Motivation

Currently a user can cause a server to crash using legitimate,
relatively simple SQL queries. This is both a security vulnerability
(DoS attacks are trivial) and a major usability bug.

The two problems:

- the SQL executor holds transient data in server memory on behalf of
  clients. The amount of memory allocated is controlled by the
  client. If memory cannot be allocated, the entire server panics and
  crashes.
  
- a long-running query that does not produce result rows (for example
  updata on a large table, but also large joins) are not terminated
  when the client connection drops. The server can be overloaded by
  e.g. a single client opening a connection, issuing a long-running
  query and dropping its connection, in a loop.

# Mechanism vs. policy

The *mechanism* is the plumbing in the code that connects the parts in
the code where allocations (for memory) and computation (for time)
happen, to the part of the code where the decision is taken whether to
let the query continue.

The *policy* is the formula that answers the following question:
"given the resources allocated so far, and given this new demand
during query execution, can the demand be satisfied and the query
allowed to continue?"

Although in practice it is possible to interleave the implementation
of mechanism and policy, this RFC distinguishes them to separate
topics of discussion.

# Quantifying memory usage

Go does not allow an application to introspect "how much memory was
allocated by this package in the context of this client connection"
from the language's run-time system. The crux of this limitation is
that Go's memory allocator cannot be overloaded and does not track its
points of use.

What can be done instead? 

General observation: potential designs for tracking memory usage
exercise a trade off between accuracy and complexity.

At one extreme, we can obtain near-maximum accuracy by mandating
manual allocation *and de-allocation* for all heap-allocated objects
(i.e. define `makeXXX()` and `freeXXX()` for all relevant types `XXX`,
including all `Expr` types, all `Datum` types, strings, arrays, etc.,
and mandating proper invocations of these functions throughout the
server). However this would make the product excessively difficult to
maintain and would seriously hinder performance.

At the other extreme we can have maximum simplicity by saying "each
client connection consumes 1 byte" however this is unlikely to help
solve the problem at hand.

To tame the design spectrum I think it is useful to state the obvious:

client memory usage = session-dependent usage + session-independent usage.

Once this is stated I propose the following *assumptions*:

1. session-independent usage is asymptotically constant, with an upper
   bound shared by all client connections independent of workload.

   (Discussion: is this first assumption reasonable?)

2. session-dependent usage is asymptotically O(N) where N is a number
   that can be tracked both efficiently and exactly in the SQL
   executor, and where both the asymptotic constant factor and
   coefficient have an upper bound shared by all clients independent of
   workload.

The 2nd assumption is motivated below.

Supposing the two assumptions hold, we can model client memory usage
as the combination of an asymptotically constant part, which can be
conservatively estimated using traditional approaches (statistics,
testing), and a linear part for which the linear coefficient can also
be conservatively estimated.

This implies that we can conservatively estimate global client memory
usage as a linear function of the variable factor N in each client
connection, and that it is possible to reliably prevent out-of-memory
errors by limiting the growth of the variable factor N in each client.

The search for mechanisms can thus be limited to those mechanisms that
track a N suitable for the 2nd assumption above.

# Dynamic memory usage in query handling

The two key observations are:

- the design of the SQL executor is based on stream processing. SQL
  execution works, except for a few exceptions, using fixed-size data
  structures.
  
- the exceptions to the preceding rules can be fully enumerated
  and instrumented to track the variable components to memory usage.

  Namely, we have:

  - Things that are O(N) in memory where N is the size in bytes of each
    input SQL query string and set of placeholders:

	- internal memory usage in the query parser;

    - the planNode hierarchy, including all Expr and constant Datum
      sub-nodes;
	  
	- the maximum stack depth of the query processing code.
	
  - Things that are O(N) in memory where N is a session-specific
    number of rows manipulated during the transaction:

    - the list of write intents in the transaction handler;
    - the map of seen prefixes in distinctNode;
	- the set of values being sorted in sortNode;
	- the set of aggregate functions being computed in groupNode;
	- the set of in-memory right-side rows in joinNode;
	- the copy of the result set held in `Executor.execStmt()` until
      the query completes.

(Discussion: if we have any doubts about our ability to enumerate all
points where variable memory growth occurs, that would be a point of
concern here as it would invalidate the assumption made above.)

# Proposed mechanism

A prototype implementation is available here:
https://github.com/cockroachdb/cockroach/pull/7608

(This is not complete as the time of this writing; it only tracks the
dynamic factor in sortNode, groupNode and joinNode. Will be extended
in time.)

This code tracks the variable factor and reports its by
means of callbacks `ReserveMemory()` and `ReleaseMemory()` to
an instance of `MemoryUsageMonitor`, which is implemented by `sql.planner`. 

The memory reservation policy can then be implemented in `planner`,
where the bulk of per-session management already occurs.

# Mechanism for time reservation

Similarly to memory usage, a callback `ComputeStep()` could be called
at those points where no data is emitted, for example when rows are filtered
out in `selectNode` or `joinNode`, and whose policy code returns an error
to indicate the query is not allowed to continue (= consume more time).

The implementation of this is out of the scope of this RFC.

# Policies

Assuming a mechanism is in place, how to decide whether
`ReserveMemory()` and `ComputeStep()` should return an error or not?

## Policy for time

This one is comparatively easier. I propose the following:

1. A query must not be allowed to
   continue computing if the client connection has been closed.

2. For open connections, no timeout should occur as
   long as clients sends stuff to the server or results are being
   communicated back. 

3. we could give users the option to introduce a query timeout per
   session, for queries that run for a long time without returning
   results (such as a full table scan on a large table with few
   matches), which would default to unlimited (= as long as the
   connection is active).

(Discussion: each of these points?)

## Policy for space

To simplify/clarify the discussion let's assume here for a moment that
we could track per-client memory usage exactly. When is a client using
"too much memory"?

There are a few parameters to consider:

1. *fairness within one application*. A long-running query with a
   large memory usage should not cause many subsequent queries,
   especially relatively "small" queries memory-wise, to fail.

2. *ungraceful degradation under high load*. Hard disks are slow, and
   the OS paging logic is a bottleneck. If any one query cause the
   database to start using swap memory to store results, the
   performance of the entire node falls off a cliff. This may be
   acceptable if there is only 1 client using the node and the client
   knows what they are doing, but allowing the server to start
   swapping out is a no-no if there are multiple clients per node.

3. *fairness between applications*. If a cluster is shared between
   multiple applications, an application issuing many small queries
   should not be blocking out another application issuing a larger
   single query.

4. *variable memory usage for the server itself*. The total amount
   of RSS memory is not composed only of the sum of per-client
   tracked memory usage. 
   
Within these parameters the two most urgent goals to reach are:

- no query should be able to crash the server with an out-of-memory
  error;
   
- users should be informed clearly when their SQL workloads are
  pushing the memory envelope (as opposed to memory usage by the
  CockroachDB internals).

**To avoid crashes** means the policy needs to coordinate allocations
across all clients against **a common maximum**. Which maximum to use?

Proposal:

- take the total number A of allocated bytes as reportd by Go's runtime
  (which we already collect);
  
- deduct the number of bytes C allocated across all clients (we're still assuming
  we can do this exactly in this RFC section); this gives us
  a measure of client-independent server memory size S = (A - C)

- take the total amount of physical memory available in the system M,
  excluding swap.

- set the common client memory usage upper bound T to a percentage of (M -
  S), for example 75%.
  
- revise T dynamically regularly to track changes in server memory
  usage. We assume that although the amount of memory per client will
  change a lot, and thus quantity C, the formula will smooth this out
  and reveal a more slowly evolving server memory usage S.

**To ensure fairness* means we should also implement a maximum
per... per what? What should be the accounting unit for fairness?

Proposal (inspired by comments from @dt): bill memory usage to user
account. That is:

- support a per-user limit U, configurable by administrators;

- during execution, monitor total client-specific memory usage for a
  given user across all connections authenticated as that user;
  
- refuse allocations that exceed the minimum of U and T (the common
  maximum defined above).

Note that this section was assuming memory usage can be tracked
exactly. If that is not the case, but the assumption described above
holds, then we can safely determine another coefficient on M and U
(that virtually reduces them) to account for the constant per-client overhead
and linear amplification coefficient.

# Drawbacks

not sure

# Alternatives

not sure

Allow swap or not? Proposal is to compute M without swap. Swap yields
ungraceful performance degradation. Do we want to risk that for the
sake of running big queries that don't fit in RAM?

# Unresolved questions

- the memory policy relies on a conservative estimate for the quantity
  T. It is not clear whether the computation described above this will
  protect the server against out-of-memory errors if a query causes
  memory usage to grow suddenly by a large amount.

- not sure how to check allocations in the code of `ReserveMemory()`
  against a globally variable maximum in a way that's both efficient,
  conservative and thread-safe.
