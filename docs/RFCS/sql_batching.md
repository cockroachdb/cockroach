- Feature Name: SQL Statement Batching
- Status: draft
- Start Date: 2017-01-18
- Authors: Nathan VanBenschoten
- RFC PR: [#13160](https://github.com/cockroachdb/cockroach/issues/13160)
- Cockroach Issue: [#10057](https://github.com/cockroachdb/cockroach/issues/10057),
                   [#2210](https://github.com/cockroachdb/cockroach/issues/2210),
                   [#4403](https://github.com/cockroachdb/cockroach/issues/4403),
                   [#5259](https://github.com/cockroachdb/cockroach/issues/5259)

# Summary

This RFC proposes a method to support the batching of multiple SQL statements for parallel
execution within a single transaction. If two or more adjacent SQL statements are provably
independent, batching their KV operations and running them together can yield up to a linear
speedup. This will serve as a critical optimization in cases where replicas have
high link latencies and round-trip communication costs dominates system performance.


# Motivation

In positioning CockroachDB as a globally-distributed database, high communication
latency between nodes in Cockroach clusters is likely to be a reality for many
real-world deployments. This use case is, therefore, one that we should make sure
to handle well. In this RFC, we focus on an individual aspect of this larger goal:
reducing aggregate latency resulting from high communication cost for SQL
transactions with multiple independent statements.

A SQL transaction is generally composed of a series of SQL statements. In some
cases, these statements depend on one another and rely on strictly ordered serial
execution for correctness. However, in many other cases, the statements are either
partially or fully independent of one another. This independence between statements
provides an opportunity to exploit [intrinsic intra-statement parallelism](#independent-statements),
where the statements can safely be executed concurrently. In a compute-bound system,
this would provide little benefit. However, in a system where communication latency
dominates performance, this pipelining of SQL statements over the network could amortize
round-trip latency between nodes and provide up to a linear speedup for fully-independent
transactions.


# Detailed design

## SQL Statement Dependencies

### Independent Statements

We say that two SQL statements are _"independent"_ if their execution could be safely
reordered without having an effect on their execution semantics or on their results.
We can extend this definition to sets of ordered SQL statements, where internally
the set may rely on the serial execution of statements, but externally the entire
group may be reordered with another set of statements.

To reason about statement independence, we first say that all statements have a
(possibly empty) set of data that they read from and a (possibly empty) set of data
that they write to. We call these the statement's **read-set (R)** and **write-set (W)**.
We can then say that two statements s<sub>1</sub> and s<sub>2</sub> are independent, and
therefore can be safely reordered, iff the following two rules are met:

1. (R<sub>1</sub> ∪ W<sub>1</sub>) ∩ W<sub>2</sub> = Ø
1. W<sub>1</sub> ∩ (R<sub>2</sub> ∪ W<sub>2</sub>) = Ø

In other words, two statements are independent if their write-sets do not overlap, and
if each statement's read-set is disjoint from the other's write-set. The first consequence
of these rules is straightforward and intuitive; two independent statements cannot modify
the same data. The second consequence requires a bit more justification. If one statement
modifies some piece of data, a second statement cannot safely base any decisions off
of the same data, even if it does not plan to change the data itself. This means that
sequential reads and writes to the same piece of data must always execute in order.
Interestingly, these rules for statement independence are somewhat analogous to the rules
we use for constructing the dependency graph in our `CommandQueue`.

Again, we can extend this reasoning to groups of SQL statements. We also say that a
group of statements has a **read-set (R)** and **write-set (W)** which are respectively
the unions of the read-set and the write-set of all statements in the group. When
determining if a group of statements is independent of another group of statements,
we can use the same rules as above using each entity's read and write-sets.

It is easy to reason that simple SELECT statements and other SQL statements that are strictly
read-only are always independent of one another, regardless of any overlap in the data
they touch. These statements perform no externally-visible state mutation, and will not
interfere with one another (from a correctness standpoint at least, see
[below](#disrupting-data-locality-and-other-unintended-performance-consequences) for a
discussion on indirect statement interaction). Using our previous notation, each statement
will have an empty write-set (W<sub>1</sub> = W<sub>2</sub> = Ø), so all both intersections
in the statement independence rules will produce the empty set.

Perhaps more interestingly, we can also reason that two statements that perform mutations
(INSERT, UPDATE, DELETE, etc.) but that touch disjoint sets of data are also independent
of one another. It is important to note here that the set of data that a statement _touches_
includes any that the statement mutates and any that the statement reads while performing
validation and filtering.

Taking this a step further, we can use our statement independence rules to find that two
mutating statements can be independent even if their touched data sets overlap, as long
as only their read-sets overlap. A practical example of this result is two statements that
insert rows into two different tables, but each validates a foreign key in the same third
table. These two statements are independent because only their read-sets overlap.

Finally, we can reason that if there is overlap between the touched sets of data between
any two statements where at least one of the two performs mutation in the overlapping region,
then the statements are not independent.

Examples:

```
SELECT * FROM a
SELECT * FROM b
SELECT * FROM a
```
All three statements are **independent** because they have empty write-sets.

```
INSERT INTO a VALUES (1)
INSERT INTO b VALUES (1)
```
Statements are **independent** because they have disjoint write-sets and empty read-sets.

```
UPDATE a SET x = 1 WHERE y = false
UPDATE a SET x = 2 WHERE y = true
```
Statements are **independent** because they have disjoint write-sets and their read-sets
do not overlap with the other's write-sets. (note that parallelizing this example will not
actually be supported by the initial proposal, see [Dependency Granularity](#dependency-granularity))

```
UPDATE a SET b = 2 WHERE y = 1
UPDATE a SET b = 3 WHERE y = 1
```
Statements are **dependent** because write-sets overlap.

```
UPDATE a SET y = true  WHERE y = false
UPDATE a SET y = false WHERE y = true
```
Statements are **dependent** because while write-sets are disjoint, the read-sets for
each statement overlap with the other statement's write-set.

```
UPDATE a SET x = 1    WHERE y = false
UPDATE a SET y = true WHERE x = 1
```
Statements are **dependent** because while write-sets are disjoint, the read-sets for
each statement overlap with the other statement's write-set.

```
INSERT INTO a VALUES (1)
INSERT INTO a VALUES (2)
```
Statements are **independent** because they write to different sets of data. However,
these will not be considered independent by this proposal because writes to the same
table may implicitly mutate state on the entire table (e.g. a `SERIAL` counter),
meaning that to be conservative their write-sets need to actually include the entire
table. Again, see [Dependency Granularity](#dependency-granularity).

### Dependency Detection

#### Dependency Granularity

These definitions hold for any granularity of data in a SQL database. For instance,
the definitions provided above could apply to data at the _cell-level_, the _row-level_,
the _table-level_, or even the _database-level_, as long as we are careful to track all
reads and writes, explicit or otherwise. By increasing the granularity at which we track
these statement dependencies, we can increase the number of statements that can be considered
independent and thus run concurrently. However, increasing the granularity of dependency
tracking also comes with added complexity and added risk of error.

For these reasons, this RFC proposes to track dependencies between statements at the
**table-level** of granularity. While this decision is conservative, it means that the
initial version of SQL Statement Batching will have a simpler implementation that
is easier to reason about by implementers and users. Tracking dependencies at the
table-level also means that we will not need to worry about implicit table-level
dependencies between seemingly independent statements. These implicit dependencies can
take forms like SERIAL columns, column constraints, and unique indexes. These implicit
row dependencies cause the ordering of mutations to different rows in a table, even
without an associated filter, to be dependent.

#### Algorithm

Before discussing the algorithm used to determine independent statements that can
safely be run in parallel, we will first make a simplifying design decision. For our
initial implementation of dependencies detection, we only plan to run adjacent independent
statements in parallel. This means that we will make no effort to parallelize sets of two or
more statements that internally must be run sequentially but that can be run concurrently with
other sets of statements. For instance, if statements s<sub>1</sub> and s<sub>2</sub> are
dependent and statements s<sub>3</sub> and s<sub>4</sub> are dependent, but the combination
of the first two statements is independent from the combination of the second two statements,
we will not try to run (s<sub>1</sub>, s<sub>2</sub>) concurrently with
(s<sub>3</sub>, s<sub>4</sub>). This is an optimization that could be made in the future,
but is not an important use case initially and will not be possible if we decide to support
statement batching through [SQL's conversational API](#conversational-api-support).

With this simplifying assumption, the algorithm becomes fairly straightforward. During
statement evaluation, a batch of pending statements will be collected, along with the read-set
and write-set for the batch. This batch of statements and its read and write-sets will begin
empty. Note that because dependencies will be detected at the table-level of granularity, the
read and write-sets for our pending batch can simply hold qualified table names. The
`sql.Executor` will iterate over provided statements one at a time, as it does now in
`execStmtsInCurrentTxn`. For each subsequent statement, the Executor will first check
if it is one of the [statement types that we support](#batchable-statements-types) for
statement batching. If not, the previous batch of statements will execute and the current
statement will execute after they are done.

If the current statement is supported through statement batching, we first create its `planNode`
and then collect the [tables](#dependency-granularity) that it reads and writes to. This will
be accomplished using a new `DependencyCollector` type that will implement the `planObserver`
interface and traverse over the statement's `planNode`. The read and write-sets collected from
the plan will then be compared against the current batch's read and write-sets using the rules for
[independent statements](#independent-statements). If the statement is deemed to be independent
of the current batch of statements, then it is added to the current batch and its read and
write-sets are merged with the current batch's. If the statement is deemed dependent on the
current batch of statements, then the current batch is [executed in parallel](#sqlkv-interaction).
Once this completes, the batch and its read and write-sets are all cleared and the new statement is
added as the only member of the current batch, with its read and write-sets replacing the
batch's. Note that because no DDL statement types will be batchable, the planNode for this
statement will not need to be reinitialized.

The integration of this algorithm in the `sql.Executor` needs to be adjusted slightly if we
decide to support batching in a [conversational API](#conversational-api-support), but the
general algorithm remains the same.

## Batchable Statements Types

Initially, there will be **5** types of statements that we will look to parallelize
when possible:
- SELECT *
- INSERT
- UPDATE
- DELETE
- UPSERT **

These statements make up the majority of use cases where statement batching would be
useful, and provide fairly straightforward semantics about read and write-sets. Another
important quality of these statements is that none of them mutate SQL schemas, meaning that
the execution or lack of execution of one of these statements will never necessitate the
re-initialization of the `planNode` for another. _Is this true even if we allow UPDATEs on
system tables?_

\* SELECT statements are included because they should be relatively easy to support and fit
into the model we've laid out above. However, if this turns out not to be the case, it is ok
to remove them initially as they are not an integral part of the targeted workflow.

\*\* As mentioned [below](#coalesced-batches), UPSERT statements may not be easily supported
using coalesced batches.

## Programmatic Interface

There are two different approaches that we are considering to expose this new feature to
SQL clients. At its current stage, this RFC is not intending to choose between the two
alternatives, but instead, present the benefits and challenges of each to prompt discussion.

### Semicolon-Separated Statement Support

Our SQL engine currently allows users to provide a semicolon-separated list of
SQL statements in a single string, and will execute all statements before returning
multiple result sets back in a single response to the user. Since the SQL Executor
has access to multiple statements in a transaction all at once when a user provides a
semicolon-separated list of statements, this interface would be a natural fit
to extend for statement batching. A user would simply activate statement batching
through some kind of [setting](#opt-in-vs-always-on) and would then send groups of
statements separated by semicolons in a single request. Our SQL engine would detect
statements that could be run in parallel and would do so whenever possible. It would
then return a single response with multiple result sets, like it already does for grouped
statements like these. A list of pros and cons to this approach are listed below:

- [+] Drivers are already expecting multiple result sets all at once when they send
  semicolon-separated statements
- [+] Never needs to mock out statement results
- [+] Allows [error handling](#error-handling) to remain relatively straightforward
  because the executor has all statements on hand at the time of batch execution and
  will not prematurely testify to the success or failure of any statement before its
  execution
- [+] Fits a nice mental model/easy to reason about
- [+] Does not need to be opt-in, although we probably still want it to be, at least
  at first
- [+] More generally applicable
- [+] Could support batched reads
- [-] May not be possible to use with ORMs (or may at least require custom handling)
- [-] Inflexible with regard to allowing users to decide which statements get executed
  in parallel

### Conversational API Support

In contrast to sending a list of statements separated by semicolons, most SQL clients
talk to databases using a single statement at-a-time conversational API. This means that
the clients send a statement and wait for the response before proceeding. They expect at the
point of a response for a statement that the statement has already been applied. An
alternate approach to batching statements for semicolon-separated lists is to build
batches between statement declarations when clients use this more standard conversational
API. To do so, we would need to buffer statements instead of executing them immediately
when we decide (or are told) to batch statements. We would then mock out a successful response
value to immediately send back to the client. After this, the `sql.Executor` would wait
for the next statement in the conversation to decide whether to continue building up a
collection of statements or to execute all pending statements. The decision to either execute
the accumulated statements or add to the batch would come down to if the new statement was
[independent](#independent-statements) from the previous set of statements. Additionally,
we would always need to execute the pending batch on read queries and transaction commit
statements.

To perform statement batching using this interface, users would need to specify which
statements could be batched. They would do so using some new syntax, for instance appending
`RETURNING NOTHING` to the end of INSERT and UPDATE statements. In turn, adding this
syntax would indicate that the SQL executor could return a fake response (`RowsAffected = 1`)
for this statement immediately and that the client would not expect the result to have any
real meaning. A list of pros and cons to this approach are listed below:

- [+] Fits the communication model ORMs usually work with
- [+] Fits the communication model other SQL clients usually work with
- [+] Users get more control over which statements are executed in a batch
- [-] Requires us to mock out results. This will not be possible for more complicated
  mutations that contain clauses like `RETURNING <values>`. It is an open question how
  we should mock a response for statements like UPDATE, that are supposed to return the
  number of rows affected
- [-] The mocking out of results also means that the statement batching feature would be
  unusable for any situation where the real statement result is needed
- [-] Complicates error handling tremendously. How do we associate errors with the
  statement that caused them if we have already responded that the statement succeeded?
- [-] Requires us to introduce new syntax
- [-] Does not permit parallel execution of read queries
- [-] Expected statement execution times become difficult to predict and reason about. For
  instance, a large update set in batch mode would return immediately. If later a small read
  query was issued, it would block for an unexpectedly large amount of time while under-the-hood
  the update would actually be executing. This is not a huge issue because this feature will
  be opt-in, but it could still lead to some surprising behavior for users in much the same way
  lazy evaluation can surprise users.

## SQL/KV Interaction

In order to parallelize the execution of SQL statements, we need a way to allow
multiple SQL statements working within the same `client.Txn` to concurrently interact
with the KV layer.

There are two competing approaches for how we can accomplish this. These
approaches have more or less the same effect but will require somewhat intrusive code
changes in different parts of the code. In trying to weigh which approach seems more
reasonable, we've discussed the benefits and challenges of each. As with the previous
section, it would be helpful to get input from domain experts on the assumptions we've
made.

### Coalesced Batches

The canonical use case of batching KV operations within a single `client.Txn` is to
construct a `client.Batch`, add all necessary operations to the batch, and then
run this batch through the `client.Txn`'s `Run` method. This is how the SQL layer currently
interacts with the KV layer, and it works quite well when the execution of a SQL statement
needs to perform a few operations.

However, the current semantics of `client.Txn` and `client.Batch` have one property that
makes it suboptimal for this use case: only a single `client.Batch` can be executed at a
time for a given `client.Txn`. This means that in order to run multiple statements concurrently,
we would need to build up a single `client.Batch` between them and then run this batch when
all statements are ready.

Doing so would not be terribly difficult, but would be fairly intrusive into code surrounding
the `sql.tableWriter` interface. The `sql.Executor` would need to inject some new object
(`BatchCoalescer`?) that wraps a single `client.Batch`, packages multiple client requests
from multiple goroutines into the batch, sends the batch only when all goroutines were ready,
and then delegates the results out to the appropriate goroutines when the batch response arrives.
During this implementation, it would almost certainly make sense to pull out a new interface
in the `internal/client` package that incorporates the creation/execution of client requests
(Get, Put, etc.), since these methods are already shared across `client.DB`, `client.Txn`, and
`client.Batch`. Our new batch delegation object could then also implement this interface to
allow for conditional indirection in the SQL layer.

This approach gets more complicated when statements that issue multiple KV batches are
considered. These cases are not rare, and arise for all statements that deal with foreign
keys. This is also the case for UPSERT statements. The optimal solution here is not clear,
but may reduce to the serial execution of these parts of the statement evaluation, or the
decision to simply not parallelize any statement that requires more than one `client.Batch`
execution at all.

### Concurrent Batches

An alternative approach looks to gain concurrency support at a layer below SQL, in KV logic.
If the restriction that only one `client.Batch` can execute in a `client.Txn` at a time was
lifted then SQL statement execution logic would not have to change between serially executed
statements and concurrently executed statements. Each independent statement could run in its own
goroutine and use it's own `client.Batch`

This offers a number of benefits including that it is easier to reason about from the SQL
layer, it eliminates the issue about statements that execute multiple batches, it is
a generally useful improvement to the KV client API that could be beneficial elsewhere,
and it removes the restriction of [coalesced batches](#coalesced-batches) that statements
need to access the KV layer in lockstep. This last point means that this alternative should
also perform better.

However, it is unclear how difficult lifting this restriction would be. At the very least,
it would require `client.Txn` to manage its state in a thread-safe manner, and would also
require changes to `kv.TxnCoordSender`.

## Error Handling

Error handling is a major concern with the batching of SQL statements. Regardless of whether
statements are batched together or not, their error semantics should behave as if they were
executed serially. This has a few consequences that can be demonstrated using an example of
two ordered statements S<sub>1</sub> and S<sub>2</sub> that are being run in parallel. If during
parallel execution we see an error for S<sub>2</sub>, we must still wait for all earlier
statements (in this case, just S<sub>1</sub>) to finish. Only then can we determine which error
to return, which should always correspond to the "earliest" error from the ordered statements
that are being run in parallel. Likewise, if during execution we see an error for S<sub>1</sub>,
we should attempt to actively cancel the execution of all later statements. We must also
be sure that no statements go into effect if any of the parallel-executing statements throw
an error. Conveniently, the atomicity property of transactions means that we can simply abort
the transaction and achieve this behavior.

If we make the feature opt-in, then it may be ok to relax these constraints and simplify
the error handling. For instance, we may be able to loosen the guarantee that the earliest
error seen in a parallel batch will be the one returned. This is not an unreasonable relaxation
because parallel batches will only contain independent statements, and therefore the successful
execution of one statement should never depend on the result of another earlier in the transaction
but within the same batch. Still, it could be confusing to users to fix an error in one statement,
only to find that there was also an error in an earlier statement.

## Other Design Considerations

### Opt-in vs. Always-on

Regardless of the [programmatic interface](#programmatic-interface) we decide to go with,
it seems prudent to activate this batching functionality on an opt-in basis. This will
probably involve introducing a new SQL session variable. We propose the name `STATEMENT_BATCHING`
(`EXPERIMENTAL_STATEMENT_BATCHING` during development), which will have a default value of
false.

The question of turning statement batching on by default, or even removing the option to
disable it entirely can be reconsidered in the future.

### EXPLAIN support

Regardless of the programmatic interface we go with, we will also want to support
introspection into the behavior and effectiveness of statement batching through
the `EXPLAIN` statement. By exposing these details to users, they can learn more about
the behavior of their queries and the impact statement batching is having on those
queries' execution.

It is not immediately obvious how this should work, and it will likely be different
depending on the batching interface. For now, the details of how `EXPLAIN` will interact
with statement batching remains an open question, but the ability to use `EXPLAIN` to
get insight into statement batching remains a design goal.

### Thread-Safe sql.Sessions

`sql.Session` currently assumes the single-threaded execution of SQL statements. This could
be problematic when attempting to run execute multiple statements concurrently because certain
Session variables can be used and modified during statement execution, such as `Location`,
`Database`, and `virtualSchemas`. However, none of these variables should ever be mutated by
the [limited set of SQL statements](#batchable-statements-types) we allow to be batched, so we
do not believe any further synchronization methods will need to be employed in order to keep
these accesses safe. Still, we should be aware of this during implementation.


# Drawbacks

### Disrupting Data Locality (and other unintended performance consequences)

Parallelizing SQL statements will necessarily alter data access patterns and locality
for transactions. This is because statements that previously executed in series, implicitly
isolated from each other, will now be executing concurrently. In practice, this means
that two statements, although proven independent in terms of data dependencies, may subtly
interact with one another down the stack beneath the SQL layer. These interactions could range
from altering cache utilization in RocksDB to changing network communication characteristics.
In most cases, we expect that these effects will be negligible. Furthermore, these effects
should be no different than those seen between any other non-contentious statements executing
concurrently. Nevertheless, users should be aware of this and benchmark any benefit they get
from parallel statement execution before expecting a certain level of performance improvement.


# Alternatives

### Perform no Dependency Detection

An alternative option to [detecting statement independence](#dependency-detection) and
performing batching based off this analysis is to push all decision-making down to the user.
This approach has the benefit that CockroachDB will no longer be responsible for deciding
if two statements are safe to execute in parallel. Instead, we shift all of this
responsibility to the user. With this approach, we would instead need to decide on a
new interface for users to specify that they want multiple statements within the same
transaction to execute in parallel. We have two main objections to this approach. First,
in many cases, it may actually be easier for us to decide if two statements are independent
than for a user to reason about it, especially in the case of obscure data dependencies like
foreign keys. Secondly, the automatic detection of parallelizable SQL statements by our
SQL engine would be a powerful feature for many users, and would fit with our theme of
"making data easy".


# Unresolved questions

### Distributed SQL Interaction

The dependency detection algorithm proposed here applies just as well to distributed
SQL as it does to standard SQL. Likewise, the question of programmatically interfacing
with this new feature applies to both SQL engines as well. However, it remains to be
explored if distributed SQL will need any special handling for parallel statement execution.

### Parallel Foreign Key Validation

There is a [TODO](https://github.com/cockroachdb/cockroach/blob/b3e11b238327c2625c519503691361850c2bb261/pkg/sql/fk.go#L294)
in foreign key validation code to batch the checks for multiple rows. While not directly
related to this RFC, infrastructure developed here could be directly applicable to solving
that issue.
