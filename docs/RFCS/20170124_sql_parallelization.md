- Feature Name: Parallel SQL Statement Execution
- Status: completed
- Start Date: 2017-01-18
- Authors: Nathan VanBenschoten
- RFC PR: [#13160](https://github.com/cockroachdb/cockroach/issues/13160)
- Cockroach Issue: [#10057](https://github.com/cockroachdb/cockroach/issues/10057),
                   [#2210](https://github.com/cockroachdb/cockroach/issues/2210),
                   [#4403](https://github.com/cockroachdb/cockroach/issues/4403),
                   [#5259](https://github.com/cockroachdb/cockroach/issues/5259)

# Summary

This RFC proposes a method to support the batching of multiple SQL statements
for parallel execution within a single transaction. If two or more adjacent SQL
statements are provably independent, executing their KV operations in parallel
can yield up to a linear speedup. This will serve as a critical optimization in
cases where replicas have high link latencies and round-trip communication costs
dominates system performance.


# Motivation

In positioning CockroachDB as a globally-distributed database, high
communication latency between nodes in Cockroach clusters is likely to be a
reality for many real-world deployments. This use case is, then, one that we
should make sure to handle well. In this RFC, we focus on an individual aspect
of this larger goal: reducing aggregate latency resulting from high
communication cost for SQL transactions with multiple independent statements.

A SQL transaction is generally composed of a series of SQL statements. In some
cases, these statements depend on one another and rely on strictly ordered
serial execution for correctness. However, in many other cases, the statements
are either partially or fully independent of one another. This independence
between statements provides an opportunity to exploit [intrinsic inter-statement
parallelism](#independent-statements), where the statements can safely be
executed concurrently. In a compute-bound system, this would provide little
benefit. However, in a system where communication cost dominates performance,
this parallel execution of SQL statements over the network could amortize
round-trip latency between nodes and provide up to a linear speedup with respect
to the number of statements in a fully-independent transaction.


# Detailed design

## SQL Statement Dependencies

### Independent Statements

We say that two SQL statements are _"independent"_ if their execution could be
safely reordered without having an effect on their execution semantics or on
their results. We can extend this definition to sets of ordered SQL statements,
where internally the set may rely on the serial execution of statements, but
externally the entire group may be reordered with another set of statements.

To reason about statement independence, we first say that all statements have a
(possibly empty) set of data that they read from and a (possibly empty) set of
data that they write to. We call these the statement's **read-set (R)** and
**write-set (W)**. We can then say that two statements s<sub>1</sub> and
s<sub>2</sub> are independent, and therefore can be safely reordered, if the
following three rules are met:

1. W<sub>1</sub> ∩ W<sub>2</sub> = Ø
2. R<sub>1</sub> ∩ W<sub>2</sub> = Ø
3. R<sub>2</sub> ∩ W<sub>1</sub> = Ø

In other words, two statements are independent if their write-sets do not
overlap, and if each statement's read-set is disjoint from the other's
write-set. The first consequence of these rules is straightforward and
intuitive; two independent statements cannot modify the same data. The second
consequence requires a bit more justification. If one statement modifies some
piece of data, a second statement cannot safely base any decisions off of the
same data, even if it does not plan to change the data itself. This means that
sequential reads and writes to the same piece of data must always execute in
order. Interestingly, these rules for statement independence are somewhat
analogous to the rules we use for constructing the dependency graph in our
`CommandQueue`.

Again, we can extend this reasoning to groups of SQL statements. We also say
that a group of statements has a **read-set (R)** and **write-set (W)** which
are respectively the unions of the read-set and the write-set of all statements
in the group. When determining if a group of statements is independent of
another group of statements, we can use the same rules as above using each
entity's read and write-sets.

It is easy to reason that simple SELECT statements and other SQL statements that
are strictly read-only are always independent of one another, regardless of any
overlap in the data they touch. These statements perform no externally-visible
state mutation, and will not interfere with one another (from a correctness
standpoint at least, see
[below](#disrupting-data-locality-and-other-unintended-performance-consequences)
for a discussion on indirect statement interaction). Using our previous
notation, each statement will have an empty write-set (W<sub>1</sub> =
W<sub>2</sub> = Ø), so both W ∩ R intersections in the statement independence
rules will produce the empty set.

Perhaps more interestingly, we can also reason that two statements that perform
mutations (INSERT, UPDATE, DELETE, etc.) but that touch disjoint sets of data
are also independent of one another. It is important to note here that the set
of data that a statement _touches_ includes any that the statement mutates and
any that the statement reads while performing validation and filtering.

Taking this a step further, we can use our statement independence rules to find
that two mutating statements can be independent even if their touched data sets
overlap, as long as only their read-sets overlap. A practical example of this
result is a pair of statements that insert rows into two different tables, but
each validates a foreign key in the same third table. These two statements are
independent because only their read-sets overlap.

Finally, we can reason that if there is overlap between the touched sets of data
between any two statements where at least one of the two performs mutation in
the overlapping region, then the statements are not independent.

#### Read and Write-Set Entries

Up until this point, we have been intentionally vague about the "data" stored in
the read and write-sets of statements. The reason for this is that the
definition of "independence" assumes a more general notion of data than just
that stored in the rows of a SQL table. Instead, this data can include any
physical or log­i­cal information that makes up the external-facing state of the
database. Using this definition, all of the following can fall into the read or
write-sets of statements:

- Cells/rows in a table
- Table indexes
- Table constraint state
- Table schemas
- Table and database privileges
- Inter-statement constraint state

Defining these as different data values that can all be part of statements read
and write-sets creates some interesting results. For instance, it creates a
dependence between any statement that implicitly behaves based on some
meta-level data and any statement that explicitly modifies that meta-level data.
An example of this is a query statement to a table and a subsequent schema
change statement on that table. In this example, the initial query's execution
semantics are dictated by the table schema, so we add this data to the query's
read set. Later, the other statement modifies this schema, so we add this data
to the query's write set. Because the read-set of the first query intersects
with the write-set of the second query, the two statements must be dependent. It
becomes clear that even simple queries have a number of implicit data accesses,
which need to be accounted for accurately in order to safely determine
statements independence.

#### Examples

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
Statements are **independent** because they have disjoint write-sets and empty
read-sets.

```
UPDATE a SET x = 1 WHERE y = false
UPDATE a SET x = 2 WHERE y = true
```
Statements are **independent** because they have disjoint write-sets and their
read-sets do not overlap with the other's write-sets. (note that parallelizing
this example will not actually be supported by the initial proposal, see
[Dependency Granularity](#dependency-granularity))

```
UPDATE a SET b = 2 WHERE y = 1
UPDATE a SET b = 3 WHERE y = 1
```
Statements are **dependent** because write-sets overlap.

```
UPDATE a SET y = true  WHERE y = false
UPDATE a SET y = false WHERE y = true
```
Statements are **dependent** because while write-sets are disjoint, the
read-sets for each statement overlap with the other statement's write-set.

```
UPDATE a SET x = 1    WHERE y = false
UPDATE a SET y = true WHERE x = 1
```
Statements are **dependent** because while write-sets are disjoint, the
read-sets for each statement overlap with the other statement's write-set.

```
INSERT INTO a VALUES (1)
INSERT INTO a VALUES (2)
```
Statements are **independent** because they write to different sets of data.

```
SELECT * FROM a
REVOKE SELECT ON a
SELECT * FROM a
```
All three statements are **dependent** because permission changes perform a
write on the permissions for table *a* that the two queries implicitly read.

```
SELECT * FROM a
ALTER TABLE a DROP COLUMN x
SELECT * FROM a
```
All three statements are **dependent** because schema changes perform a write on
the schema for table *a* that the two queries implicitly read.

```
SELECT statement_timestamp()
SELECT statement_timestamp()
```
Statements are **dependent** because a call to `statement_timestamp` implicitly
mutates state scoped to the current transaction. This falls under the category
of "inter-statement constraint state".

### Dependency Detection

#### Dependency Granularity

These definitions hold for any granularity of data in a SQL database. For
instance, the definitions provided above could apply to data at the
_cell-level_, the _row-level_, the _table-level_, or even the _database-level_,
as long as we are careful to track all reads and writes, explicit or otherwise.
By increasing the granularity at which we track these statement dependencies, we
can increase the number of statements that can be considered independent and
thus run concurrently. However, increasing the granularity of dependency
tracking also comes with added complexity and added risk of error.

However, instead of working in the domain of SQL constructs, this RFC proposes
to track dependencies between statements at the `roachpb.Key` level. The reason
for this is that our SQL layer already maps SQL logic into the `roachpb.Key`
domain, which is totally-ordered and is more straightforward to work with. In
addition, `roachpb.Keys` can also be grouped into `roachpb.Span`, which already
have set semantics defined for them. So, by first leveraging existing
infrastructure to map SQL into reads and writes within `roachpb.Spans`, we can
perform dependency analysis on SQL statements by determining if sets of
`roachpb.Spans` overlap.

#### Algorithm

The algorithm to manage statement execution so that independent statements can
be run in parallel is fairly straightforward. During statement evaluation, a
list of asynchronously executable statements is maintained, comprised of both
currently executing statements and pending statements. Parallelizable statements
are appended to the list as they arrive, and they are only allowed to begin
execution if they don't depend on any statement ahead of them in the list.
Whenever a statement finishes execution, it is removed from the list, and we
check if any pending statements can now begin execution. Note that because
dependencies are detected at the table-level of granularity, the read and
write-sets used for our statement independence detection within this list can
simply hold table IDs.

When new statements arrive, the `sql.Executor` iterates over them as it does now
in `execStmtsInCurrentTxn`. For each statement, the Executor first checks if it
is one of the [statement types that we
support](#parallelizable-statements-types) 
for statement parallelized execution.
If the statement type is not parallelizable or if the statement has not [opted
into](#programmatic-interface) parallelization, the executor blocks until the
queue of parallelized statements clears before executing the new statement
synchronously. Notice that these semantics reduce to our current statement
execution rules when no statements opt-in to parallelization.

If the current statement is supported through statement parallelization, we
first create its `planNode` and then collect the
[tables](#dependency-granularity) that it reads and writes to. This will be
accomplished using a new `DependencyCollector` type that will implement the
`planObserver` interface and traverse over the statement's `planNode`. The read
and write-sets collected from the plan will then be compared against the current
statement queue's read and write-sets using the rules for [independent
statements](#independent-statements). If the statement is deemed to be
independent of the current batch of statements, then it can immediately begin
execution in a new goroutine. If the statement is deemed dependent on the
statements ahead of it in the queue, then it is added as a pending statement in
the queue. Note that because no DDL statement types will be parallelizable, the
`planNode`s for statements in the queue will never need to be reinitialized.

## Parallelizable Statements Types

Initially, there will be **4** types of statements that we will allow to be
parallelized:
- INSERT
- UPDATE
- DELETE
- UPSERT

These statements make up the majority of use cases where statement
parallelization would be useful, and provide fairly straightforward semantics
about read and write-sets. Another important quality of these statements is that
none of them mutate SQL schemas, meaning that the execution or lack of execution
of one of these statements will never necessitate the re-initialization of the
`planNode` for another. This may not be true of UPDATEs on system tables, so we
will need to take special care to disallow that case?

## Programmatic Interface

Most SQL clients talk to databases using a single statement at-a-time
conversational API. This means that the clients send a statement and wait for
the response before proceeding. They expect at the point of a response for a
statement that the statement has already been applied. To fit this execution
model while allowing for multiple statements to run in parallel, we would need
to mock out a successful response to parallelized statements and immediately
send the value back to the client. The decision to either execute the statement
or block on other asynchronously executing statements would come down to if the
new statement was [independent](#independent-statements) from the previous set
of statements. Additionally, we would always need to let all parallelized
statements finish executing on read queries and transaction commit statements.

To perform statement parallelization using this interface, users would need to
specify which statements could be executed in parallel. They would do so using
some new syntax, for instance appending `RETURNING NOTHING` to the end of INSERT
and UPDATE statements. In turn, adding this syntax would indicate that the SQL
executor could return a fake response (`RowsAffected = 1`) for this statement
immediately and that the client would not expect the result to have any real
meaning. A list of pros and cons to this approach are listed below:

- [+] Fits the communication model ORMs usually work with
- [+] Fits the communication model other SQL clients usually work with
- [+] Users get more control over which statements are executed concurrently
- [-] Requires us to mock out results.
- [-] Mocking out result also means that the statement parallelization feature
  would be unusable for any situation where a client is interested in the real
  results of a statement, event when these results do not imply a dependence
  between statements.
- [-] Complicates error handling if we need to associate errors with statements.
  If we allow ourselves to loosen error semantics for parallelized statements,
  as discussed [below](#error-handling) this issue goes away.
- [-] Requires us to introduce new syntax
- [-] Does not permit parallel execution of read queries
- [-] Expected statement execution times become difficult to predict and reason
  about. For instance, a large update set in parallelized execution mode would
  return immediately. If later a small read query was issued, it would block for
  an unexpectedly large amount of time while under-the-hood the update would
  actually be executing. This is not a huge issue because this feature will be
  opt-in, but it could still lead to some surprising behavior for users in much
  the same way lazy evaluation can surprise users.

## SQL/KV Interaction

In order to parallelize the execution of SQL statements, we need a way to allow
multiple SQL statements working within the same `client.Txn` to concurrently
interact with the KV layer.

The canonical way to batch KV operations within a single `client.Txn` is to
construct a `client.Batch`, add all necessary operations to the batch, and then
run this batch through the `client.Txn`'s `Run` method. This is how the SQL
layer currently interacts with the KV layer, and it works quite well when the
execution of a SQL statement needs to perform a few operations. However, the
current semantics of `client.Txn` and `client.Batch` have one property that
makes it suboptimal for our use case here: only a single `client.Batch` can be
executed at a time for a given `client.Txn`.

If the restriction that only one `client.Batch` can execute in a `client.Txn` at
a time was lifted then SQL statement execution logic would not have to change
between serially executed statements and concurrently executed statements. Each
independent statement could run in its own goroutine and use its own
`client.Batch`

This offers a number of benefits including that it is easier to reason about
from the SQL layer, it eliminates the issue about statements that execute
multiple batches, it is a generally useful improvement to the KV client API that
could be beneficial elsewhere, and it removes the restriction of [coalesced
batches](#coalesced-batches) that statements need to access the KV layer in
lockstep. This last point means that this approach should also perform better
than the alternative.

However, it is unclear how difficult lifting this restriction would be. At the
very least, it would require `client.Txn` to manage its state in a thread-safe
manner, and would also require changes to `kv.TxnCoordSender`.

## Error Handling

Error handling is a major concern with the parallelization of SQL statements.
However, because Parallel SQL Statement Execution will be an opt-in feature, we
deem it ok to relax constraints and simplify error handling. For instance, we
may be able to loosen the guarantee that the earliest error seen in a parallel
batch will be the one returned. This is not an unreasonable relaxation because
parallel-executing statements will always be independent, and therefore the
successful execution of one statement should never depend on the result of
another earlier in the transaction executing at the same time.

Furthermore, in CockroachDB errors invalidate the entire transaction, so there
is little benefit to performing fine-grained error handling in the application.
We can contrast this to MySQL, where it is common to try an INSERT, catch any
constraint violation, and fall back to an UPDATE. That doesn't work for us
because the transaction is in an undefined state after any error and can only be
retried from the beginning.

## Other Design Considerations

### EXPLAIN support

Regardless of the programmatic interface we go with, we will also want to
support introspection into the behavior and effectiveness of statement
parallelization through the `EXPLAIN` statement. By exposing these details to
users, they can learn more about the behavior of their queries and the impact
parallelized statement execution is having on those queries' execution.

It is not immediately obvious how this should work, and it will likely be
different depending on the parallelization interface. For now, the details of
how `EXPLAIN` will interact with statement parallelization remains an open
question, but the ability to use `EXPLAIN` to get insight into parallelized
statement execution remains a design goal.

### Thread-Safe sql.Sessions

`sql.Session` currently assumes the single-threaded execution of SQL statements.
This could be problematic when attempting to run execute multiple statements
concurrently because certain Session variables can be used and modified during
statement execution, such as `Location`, `Database`, and `virtualSchemas`.
However, none of these variables should ever be mutated by the [limited set of
SQL statements](#parallelizable-statements-types) we allow to be parallelized,
so we do not believe any further synchronization methods will need to be
employed in order to keep these accesses safe. Still, we should be aware of this
during implementation.

## Motivating Example Analysis

Below is one of the motivating examples for this change. Five statements are
executed sequentially in a transaction, and work on the schema created directly
above them.

```
CREATE TABLE IF NOT EXISTS account (
  id INT,
  balance BIGINT NOT NULL,
  name STRING,

  PRIMARY KEY (id),
  UNIQUE INDEX byName (name)
);

CREATE TABLE IF NOT EXISTS transaction (
  id INT,
  booking_date TIMESTAMP DEFAULT NOW(),
  txn_date TIMESTAMP DEFAULT NOW(),
  txn_ref STRING,

  PRIMARY KEY (id),
  UNIQUE INDEX byTxnRef (txn_ref)
);

CREATE TABLE IF NOT EXISTS transaction_leg (
  id BYTES DEFAULT uuid_v4(),
  account_id INT,
  amount BIGINT NOT NULL,
  running_balance BIGINT NOT NULL,
  txn_id INT,

  PRIMARY KEY (id)
);

BEGIN;
SELECT id, balance FROM account WHERE id IN ($1, $2); -- result used by client
INSERT INTO transaction (id, txn_ref) VALUES ($1, $2);
INSERT INTO transaction_leg (account_id, amount, running_balance, txn_id) VALUES ($1, $2, $3, $4);
UPDATE account SET balance = $1 WHERE id = $2;
UPDATE account SET balance = $1 WHERE id = $2;
COMMIT;
```

At present time, each of these five statements executes sequentially. The
execution timeline of this transaction looks like:

```
BEGIN S1-----\S1 S2-----\S2 S3-----\S3 S4-----\S4 S5-----\S5 COMMIT
```

It is interesting to explore how this newly proposed parallelized statement
execution functionality could be used to speed up this transaction. For now, we
will assume that the programmatic interface decided upon was the `RETURN
NOTHING` proposal.

First, we note that the SELECT statement's results are used by the client, so
even if our proposal supported read queries, it would not be useful here.
However, this is the only statement where the results are used. Because of that,
the client can add the `RETURNING NOTHING` clause to the end of the four
mutating statements to indicate to CockroachDB that the results of the
statements are not needed and that Cockroach should try to parallelize their
execution. At this point, the transaction looks like this:

```
BEGIN;
SELECT id, balance FROM account WHERE id IN ($1, $2);
INSERT INTO transaction (id, txn_ref) VALUES ($1, $2) RETURNING NOTHING;
INSERT INTO transaction_leg (account_id, amount, running_balance, txn_id) VALUES ($1, $2, $3, $4) RETURNING NOTHING;
UPDATE account SET balance = $1 WHERE id = $2 RETURNING NOTHING;
UPDATE account SET balance = $1 WHERE id = $2 RETURNING NOTHING;
COMMIT;
```

We see that the two INSERT statements go to different tables. Because of this,
and because they share no implicit writes, they fit the definition for
independent statements. Furthermore, the following UPDATE is also to a different
table, and it can be proven to be independent of the union of the two INSERT
statements. However, because the second UPDATE is not to a unique table, it is
not independent of the previous three statements. It is true that the first and
second UPDATE statements could be merged with some clever `CASE` trickery, but
for now we will ignore this optimization because it is not representative of a
query that most ORMs would execute. So, with no other changes, the execution
timeline of this statement will now look like:

```
BEGIN S1-----\S1 S2-----\S2   S5-----\S5 COMMIT
                  S3-----\S3
                   S4-----\S4
```

This is already a huge improvement. Assuming all statements take roughly the
same time to execute and that client-server latency is negligible, we've
effectively cut the processing time for the transaction by 40%!

### Future Optimization

While our current proposal already speeds up the motivating example
substantially, there is still room for improvement. Specially, we can reason
that while the last two UPDATE statements mutate the same table, as long as
their `$2` parameters are different, they will not overlap and are actually
independent. The reason for our "false" dependency classification is the
[conservative dependency detection granularity](#dependency-granularity)
proposed in our initial implementation of statement parallelization. While in
theory increasing this granularity to the row-level across the board would solve
this issue, it would also come with a number of complications. Alternatively,
one proposed solution is to selectively increase this granularity if and only if
all statements operating on the same table also fit some predefined pattern. One
such pattern might be the "single-row-WHERE-matches-primary-key" style, which
makes it much easier to analyze inter-statement dependencies than in the general
case. If such an optimization did exist, we could detect that the two UPDATEs
were actually independent, we could cut processing time for the transaction by
60% using the following execution timeline:

```
BEGIN S1-----\S1 S2-----\S2    COMMIT
                  S3-----\S3
                   S4-----\S4
                    S5-----\S5
```

# Drawbacks

### Disrupting Data Locality (and other unintended performance consequences)

Parallelizing SQL statements will necessarily alter data access patterns and
locality for transactions. This is because statements that previously executed
in series, implicitly isolated from each other, will now be executing
concurrently. In practice, this means that two statements, although proven
independent in terms of data dependencies, may subtly interact with one another
down the stack beneath the SQL layer. These interactions could range from
altering cache utilization in RocksDB to changing network communication
characteristics. In most cases, we expect that these effects will be negligible.
Furthermore, these effects should be no different than those seen between any
other non-contentious statements executing concurrently. Nevertheless, users
should be aware of this and benchmark any benefit they get from parallel
statement execution before expecting a certain level of performance improvement.


# Alternatives

### Perform no Dependency Detection

An alternative option to [detecting statement
independence](#dependency-detection) and performing batching based off this
analysis is to push all decision-making down to the user. This approach has the
benefit that CockroachDB will no longer be responsible for deciding if two
statements are safe to execute in parallel. Instead, we shift all of this
responsibility to the user. With this approach, we would instead need to decide
on a new interface for users to specify that they want multiple statements
within the same transaction to execute in parallel. We have two main objections
to this approach. First, in many cases, it may actually be easier for us to
decide if two statements are independent than for a user to reason about it,
especially in the case of obscure data dependencies like foreign keys. Secondly,
the automatic detection of parallelizable SQL statements by our SQL engine would
be a powerful feature for many users, and would fit with our theme of "making
data easy".

### Parallelizing Semicolon-Separated Statements

Our SQL engine currently allows users to provide a semicolon-separated list of
SQL statements in a single string, and will execute all statements before
returning multiple result sets back in a single response to the user. Since the
SQL Executor has access to multiple statements in a transaction all at once when
a user provides a semicolon-separated list of statements, this interface would
be a natural fit to extend for parallelized statement execution. An alternate
approach to parallelizing statements through a standard conversational API is to
parallelize statements sent within a semicolon-separated statement list. A user
would simply activate statement parallelization through some kind of session
variable setting and would then send groups of statements separated by
semicolons in a single request. Our SQL engine would detect statements that
could be run in parallel and would do so whenever possible. It would then return
a single response with multiple result sets, like it already does for grouped
statements like these. A list of pros and cons to this approach are listed
below:

- [+] Drivers are already expecting multiple result sets all at once when they
  send semicolon-separated statements
- [+] Never needs to mock out statement results
- [+] Allows [error handling](#error-handling) to remain relatively
  straightforward because the executor has all statements on hand at the time of
  batch execution and will not prematurely testify to the success or failure of
  any statement before its execution
- [+] Fits a nice mental model/easy to reason about
- [+] Does not need to be opt-in, although we probably still want it to be, at
  least at first
- [+] More generally applicable
- [+] Could support parallelized reads
- [-] May not be possible to use with ORMs (or may at least require custom
  handling)
- [-] Inflexible with regard to allowing users to decide which statements get
  executed in parallel

### Coalesced Batches

An alternate approach to supporting concurrent `client.Batch`es executing within
a `client.Txn` is to gain concurrency support at a layer above KV, in SQL logic.
To do this, we would build up a single `client.Batch` between multiple
statements and then run this batch when all statements are ready.

Doing so would not be terribly difficult, but would be fairly intrusive into
code surrounding the `sql.tableWriter` interface. The `sql.Executor` would need
to inject some new object (`BatchCoalescer`?) that wraps a single
`client.Batch`, packages multiple client requests from multiple goroutines into
the batch, sends the batch only when all goroutines were ready, and then
delegates the results out to the appropriate goroutines when the batch response
arrives. During this implementation, it would almost certainly make sense to
pull out a new interface in the `internal/client` package that incorporates the
creation/execution of client requests (Get, Put, etc.), since these methods are
already shared across `client.DB`, `client.Txn`, and `client.Batch`. Our new
batch delegation object could then also implement this interface to allow for
conditional indirection in the SQL layer.

This approach gets more complicated when considering statements that issue
multiple KV batches. These cases are not rare, and arise for all statements that
deal with foreign keys. This is also the case for UPSERT statements. The optimal
solution here is not clear, but may reduce to the serial execution of these
parts of the statement evaluation, or the decision to simply not parallelize any
statement that requires more than one `client.Batch` execution at all.


# Unresolved questions

### Distributed SQL Interaction

The dependency detection algorithm proposed here applies just as well to
distributed SQL as it does to standard SQL. Likewise, the question of
programmatically interfacing with this new feature applies to both SQL engines
as well. However, it remains to be explored if distributed SQL will need any
special handling for parallel statement execution.

### Parallel Foreign Key Validation

There is a
[TODO](https://github.com/cockroachdb/cockroach/blob/b3e11b238327c2625c519503691361850c2bb261/pkg/sql/fk.go#L294)
in foreign key validation code to batch the checks for multiple rows. While not
directly related to this RFC, infrastructure developed here could be directly
applicable to solving that issue.
