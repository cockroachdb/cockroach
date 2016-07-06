- Feature Name: resource bounds
- Status: draft
- Start Date: 2016-07-06
- Authors: knz
- RFC PR: #7657
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
  updata on a large table, but also large joins) is not terminated
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
allocated by this package in the context of this client connection (or
transaction, or query)" from the language's run-time system. The crux
of this limitation is that Go's memory allocator cannot be overloaded
and does not track its points of use.

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
query consumes 1 byte" however this is unlikely to help
solve the problem at hand.

To tame the design spectrum I think it is useful to state the obvious:

client memory usage = pgwire memory + SQL memory

SQL memory = memory allocated outside of transactions + memory allocated inside of transactions

Once this is stated I propose the following *assumptions*:

1. pgwire memory is O(P) where P is a linear combination of the number
   of prepared statements, the number of bound statements (where the
   parameters are stored server-side) and the sizes of the stored
   items.

2. SQL memory outside of transactions (incl. connection & session
   overhead) is asymptotically constant, with an upper bound shared by
   all client connections independent of workload.

   (Discussion: is this first assumption reasonable?)

3. memory inside transactions is asymptotically O(W) + O(Q) + O(R) where:

   - W is the number of write intents already laid in the txn; this number is already known;
   - Q is the size of query/statement strings inside the txn; this number is known at each execStmt invocation;
   - R is the database-dependent (as opposed to constant,
     planNode-specific) number of Datums held into memory by various
     plan nodes, which can be efficiently and accurately tracked
     during query execution;

   In addition, both the asymptotic constant factor and 3 linear
   coefficients have an upper bound shared by all clients independent
   of workload.

The 3rd assumption is motivated below.

Supposing the assumptions hold, we can model client memory usage
as the combination of an asymptotically constant part, which can be
conservatively estimated using traditional approaches (statistics,
testing), and a linear part which can also be conservatively
estimated.

This implies that we can reliably prevent out-of-memory errors by
limiting the growth of the variable parameters P/W/Q/R in each connection.

The search for mechanisms can thus be limited to those mechanisms that
track these parameters, especially R which is currently not tracked in
the code.

# Dynamic memory usage in query handling

The two key observations are:

- the design of the SQL executor is based on stream processing. SQL
  execution works, except for a few exceptions, using fixed-size data
  structures.

- the exceptions to the preceding rules can be fully enumerated
  and instrumented to track the variable components to memory usage.

  Namely, we have:

  - Things that are O(Q) in memory where Q is the size in bytes of each
    input SQL query string and set of placeholders:

	- internal memory usage in the query parser;

    - the planNode hierarchy, including all Expr and constant Datum
      sub-nodes;
	  
	- the maximum stack depth of the query processing code.
	
  - Things that are O(R) in memory where R is a session-specific
    number of rows manipulated during the transaction:

    - the map of seen prefixes in distinctNode;
	- the set of values being sorted in sortNode;
	- the set of aggregate functions being computed in groupNode;
	- the set of in-memory right-side rows in joinNode;
	- the copy of the result set held in `Executor.execStmt()` until
      the query completes.

  - The set of write intents kept in the SQL gateway for each
    transaction, which grows linearly with the number of writes W.
  
(Discussion: if we have any doubts about our ability to enumerate all
points where variable memory growth occurs, that would be a point of
concern here as it would invalidate the assumption made above.)

# Proposed mechanism

A prototype implementation is available here:
https://github.com/cockroachdb/cockroach/pull/8691

This code tracks the variable factor R and reports its by
means of callbacks `ReserveMemory()` and `ReleaseMemory()` to
an instance of `MemoryUsageMonitor`, which is embedded in `sql.Session`. 

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

2. For open connections, no timeout should occur as long as either 1)
   no transaction is currently open or 2) clients sends stuff to the
   server or results are being communicated back.

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
   large memory usage should not cause many subsequent queries via
   other connections, especially relatively "small" queries
   memory-wise, to fail.

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
   
5. *idle connections*. It is customary for applications to have many
   idle connections open simultaneously, with the "real workload"
   coming from a few of these connections over time. Which connections
   cause work load may change over time. 

# Alternatives

The following approaches have been considered:

A. track the amount of dynamic memory allocated per transaction, then
   reject (with an error) allocations beyond a configurable
   per-transaction maximum.
   
   Pros: simple to implement, low overhead.
   
   Cons: blow-ups still possible (issue many small queries
   concurrently via separate connections
   connections). Single-connection workloads (nodes dedicated to large
   queries) need custom configuration.
   
B. same as A, comparing allocations to a global (node-wide) maximum.

   Pros: relatively simple to implement. Largely mitigates blow-outs.
   
   Cons: need to tally allocations atomically in a shared counter
   across all connections; this may become a contention point.
   
   DoS vulnerability: a long-running large transaction will cause many
   subsequently issued transactions, even small ones, to fail.

C. hybrid: guaranteed minimum per transaction + extra checked against
   a global maximum. Principles:
   
   - each opened transaction gets a minimum allocation M guaranteed to
     satisfy "simple" queries (without a dynamic component in
     distinct/group/sort/join, + small amount of write intents).

   - everything above that minimum is limited against a global
     maximum.
	 
   To make this work we need another mechanism to guarantee the
   minimum per transaction. Ideas considered:
   
   C.1. pre-allocate M for each incoming *connection*; refuse new
        connections when M cannot be allocated against the remaining
        global maximum. Release M upon connection close.
		
		Pros: simple to implement
		
		Cons: too conservative, does not account for idle connections
        which consume next to no memory.
		
   C.2. pre-allocate M for each opened *transaction*; reject new
        transactions with an error when M cannot be allocated agains
        the remaining global maximum. Release M upon txn
        COMMIT/ROLLBACK.
		
		Pros: simple to implement, accounts properly for idle connections
		
		Cons: users will be surprised to see txns failing to begin
        although the connection was opened successfully.  Does not
        account for variable allocations outside of transactions, such
        as prepared statements.
		
   C.3. either C.1 or C.2 with a *semaphore*: the thing that needs a
        minimum (either connection or txn) *waits* until that minimum is
        available.

        Pros: txns/connections don't fail with an error
		
		Cons: potentially more jitter in connection/txn start-up
        latency. Potential for deadlocks.

D. Same alternatives as A/B/C, but using separate global maximums per
   user or database. This achieves resource separation. 


# Drawbacks

not sure

# Unresolved questions

- the memory policy relies on a conservative estimate for the quantity
  T. It is not clear whether the computation described above this will
  protect the server against out-of-memory errors if a query causes
  memory usage to grow suddenly by a large amount.

- not sure how to check allocations in the code of `ReserveMemory()`
  against a globally variable maximum in a way that's both efficient,
  conservative and thread-safe.
