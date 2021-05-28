# Life of a SQL Query

Original author: Andrei Matei (updated May 2021)

## Introduction

This document aims to explain the execution of an SQL query against
CockroachDB, explaining the code paths through the various layers of
the system (network protocol, SQL session management, parsing,
execution planning, syntax tree transformations, query running,
interface with the KV code, routing of KV requests, request
processing, Raft, on-disk storage engine). The idea is to provide a
high-level unifying view of the structure of the various components;
no one will be explored in particular depth but pointers to other
documentation will be provided where such documentation exists. Code
pointers will abound.

This document will generally not discuss design decisions; it will
rather focus on tracing through the actual (current) code.

The intended audience for this post is folks curious about a dive
through the architecture of a modern, albeit young, database presented
differently than in a [design
doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md). It
will hopefully also be helpful for open source contributors and new
Cockroach Labs engineers.

## Limitations

This document does not cover some important aspects of query execution,
in particular major developments that have occurred after the document
was initially authored; including but not limited to:

- how transaction and SQL session timestamps are assigned
- vectorized SQL execution
- distributed SQL processing
- concurrent statement execution inside a SQL transaction
- 1PC optimizations for UPDATE and INSERT
- key structure and encoding
- column families
- composite encoding for collated strings and DECIMAL values in PK and indexes
- closed timestamps
- follower reads

## Postgres Wire Protocol

A SQL query arrives at the server through the Postgres wire protocol
(CockroachDB speaks the Postgres protocol for compatibility with
existing client drivers and applications). The `pgwire` package
implements protocol-related functionality; once a client connection is
authenticated, it is represented by a
[`pgwire.conn`](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L55)
struct (it wraps a [`net.Conn`](https://golang.org/pkg/net/#Conn)
interface - Go's
sockets).
[`conn.serveImpl`](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L211)
implements the "read query - parse it - push it to `connExecutor` for
execution" loop. The protocol is message-oriented: for the lifetime of the
connection, we
[read a message](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L328)
usually representing one or more SQL statements, parse the queries, pass them
to the `connExecutor` for executing all the statements in the batch and, once
that's done and the results have been produced,
[serialize and buffer them](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L1221)
and then possibly
[send the buffered results to the client](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L1451).

Notice that the results are not streamed to the client and, moreover, a whole
batch of statements might be executed before any results are sent back.

## connExecutor

The
[`connExecutor`](https://github.com/cockroachdb/cockroach/blob/83f7cc42c7706a06b892596be4872dea78bdff7e/pkg/sql/conn_executor.go#L936)
is responsible for parsing and executing statements pushed into its
[statement buffer](https://github.com/cockroachdb/cockroach/blob/83f7cc42c7706a06b892596be4872dea78bdff7e/pkg/sql/conn_executor.go#L967)
by a given client connection as well as pushing the results back to the
`pgwire.conn`. The main entry point is
[`connExecutor.execCmd`](https://github.com/cockroachdb/cockroach/blob/83f7cc42c7706a06b892596be4872dea78bdff7e/pkg/sql/conn_executor.go#L1473),
which reads the current
[command](https://github.com/cockroachdb/cockroach/blob/ea2deb7aba31c264d0a2797e0da7a0d09516b22d/pkg/sql/conn_io.go#L116)
from the statement buffer and executes a query if necessary (not all commands
require query execution). The execution of the queries is done in the context
of a
[`sessiondata.SessionData`](https://github.com/cockroachdb/cockroach/blob/6c8ed5d5b90fd997286fe83ba255afeeed94e01f/pkg/sql/sessiondata/session_data.go#L26)
object which accumulates information about the state of the connection
(e.g. the database that has been selected, the various variables that
can be set) as well as of
[transaction state](https://github.com/cockroachdb/cockroach/blob/83f7cc42c7706a06b892596be4872dea78bdff7e/pkg/sql/conn_executor.go#L975).
The `connExecutor` also manipulates a
[`planner`](https://github.com/cockroachdb/cockroach/blob/5ae7430a1bc5497229386d124a62e75e2805b8ae/pkg/sql/planner.go#L132)
struct which provides the functionality around actually planning and
executing a query.

`connExecutor` implements a state-machine by receiving batches of statements
from `pgwire`, executing them one by one, updating its transaction state (did a
new transaction just begin or an old transaction just end? Did we encounter an
error which forces us to abort the current transaction?) and notifying
[ClientComm](https://github.com/cockroachdb/cockroach/blob/ea2deb7aba31c264d0a2797e0da7a0d09516b22d/pkg/sql/conn_io.go#L575)
about any results or errors.

### Parsing

Parsing is
[performed](https://github.com/cockroachdb/cockroach/blob/89621764d4c2d438d1781238f10e9ef27ef2c392/pkg/sql/pgwire/conn.go#L735),
by `pgwire.conn` and uses a LALR parser generated by `go-yacc` from a
[Yacc-like grammar file](https://github.com/cockroachdb/cockroach/blob/4ae1e3c4605759131cf87155b9e48cba4f272a22/pkg/sql/parser/sql.y),
originally copied from Postgres and stripped down, and then gradually
grown organically with ever-more SQL support. The process of parsing
transforms a `string` into an array of ASTs (Abstract Syntax Trees),
one for each statement. The AST nodes are structs defined in the
`sql/sem/tree` package, generally of two types - statements and
expressions. Expressions implement a common interface useful for
applying tree transformations. These ASTs will later be transformed by
the cost-based optimizer into an execution plan.

### Statement Execution

`connExecutor.execCmd` reads the current command from the `stmtBuf` and 
executes it. If the session had an open transaction after execution of the 
previous command, we continue executing statements until a `COMMIT/ROLLBACK`. 
This "consuming of statements" is done by the call to
[`connExecutor.execStmt()`](https://github.com/cockroachdb/cockroach/blob/a8a6d8542593e18e70d9becd2e6a1d4f26297e95/pkg/sql/conn_executor_exec.go#L68).

There is an impedance mismatch that has to be explained here, around
the interfacing of the SQL `Executor/session` code, which is
stream-oriented (with statements being executed one at a time possibly
within the scope of SQL transactions) and CockroachDB's Key/Value (KV)
interface, which is request oriented with transactions explicitly
attached to every request. The most interesting interface for the KV
layer of the database is the
[`Txn.exec()`](https://github.com/cockroachdb/cockroach/blob/0eba39ad3a309bcb37709b8e5eed556865dae944/pkg/kv/txn.go#L821)
method. `Txn` lives in the `/pkg/kv` package, which contains 
the KV client interface. `Txn` represents a KV transaction;
there's generally one associated with the SQL session, reused between
client ping-pongs.

The `Txn.exec` interface executes a given closure function in the context of a
distributed transaction. The transaction is automatically aborted if retryable 
returns any error aside from recoverable internal errors, in which case the 
closure is retried. Retries are 
[sometimes necessary](https://www.cockroachlabs.com/docs/stable/transactions.html#transaction-retries)
in CockroachDB (usually because of data contention). In case no error occurred 
the transaction is automatically committed.

To hint at the complications: a single SQL
statement executed outside of a SQL transaction (i.e. an "implicit
transaction") can be safely retried. However, a SQL transaction
spanning multiple client requests will have different statements
executed in different callbacks passed to `Txn.exec()`; as such, it is
not sufficient to retry one of these callbacks - we have to retry all
the statements in the transaction, and generally some of these
statements might be conditional on the client's logic and thus cannot
be retried verbatim (i.e. different results for a `SELECT` might
trigger different subsequent statements). In this case, we bubble up a
retryable error to the client; more details about this can be read in
our [transaction
documentation](https://www.cockroachlabs.com/docs/stable/transactions.html#client-side-intervention).

`connExecutor` serves as a coordinator between different components during a
SQL statement execution. It [builds a logical plan](https://github.com/cockroachdb/cockroach/blob/a8a6d8542593e18e70d9becd2e6a1d4f26297e95/pkg/sql/conn_executor_exec.go#L829)
for a statement, and then converts it to a physical plan and 
[passes it to a SQL engine for execution](https://github.com/cockroachdb/cockroach/blob/a8a6d8542593e18e70d9becd2e6a1d4f26297e95/pkg/sql/conn_executor_exec.go#L923). 

### Building execution plans

Now that we have figured out what (KV) transaction we're running inside
of, we are concerned with executing SQL statements one at a
time. [`execStmt()`](https://github.com/cockroachdb/cockroach/blob/a8a6d8542593e18e70d9becd2e6a1d4f26297e95/pkg/sql/conn_executor_exec.go#L68)
has a few layers below it dealing with the various states a SQL transaction can 
be in
([open](https://github.com/cockroachdb/cockroach/blob/a8a6d8542593e18e70d9becd2e6a1d4f26297e95/pkg/sql/conn_executor_exec.go#L243)
/
[aborted](https://github.com/cockroachdb/cockroach/blob/a8a6d8542593e18e70d9becd2e6a1d4f26297e95/pkg/sql/conn_executor_exec.go#L1196)
/ [waiting for a user retry](https://github.com/cockroachdb/cockroach/blob/a8a6d8542593e18e70d9becd2e6a1d4f26297e95/pkg/sql/conn_executor_exec.go#L1250),
etc.). `execStmt()` [creates an "execution plan"](https://github.com/cockroachdb/cockroach/blob/a8a6d8542593e18e70d9becd2e6a1d4f26297e95/pkg/sql/conn_executor_exec.go#L947)
for a statement and runs it.

An execution plan in CockroachDB is a tree of
[`planNode`](https://github.com/cockroachdb/cockroach/blob/1d2fadff2299166292cd5274b54bf7fad5df53c3/pkg/sql/plan.go#L73)
nodes, similar in spirit to the AST but, this time, containing
semantic information and also runtime state. This tree is built by
[`planner.makeOptimizerPlan()`](https://github.com/cockroachdb/cockroach/blob/df67aa9707fbf0193ec8b3ca4062240c360fc808/pkg/sql/plan_opt.go#L188),
which builds the `planNode` tree from a parsed statement after having 
performed all the semantic analysis and various transformations. 
The nodes in this tree are actually "executable"
(they have `startExec()` and `Next()` methods), and each one will consume
data produced by its children (e.g. a `joinNode` has
[`left and right`](https://github.com/cockroachdb/cockroach/blob/a97cdd6288c8a59f43aa0c4063310923ec04050b/pkg/sql/join.go#L23-L24)
children whose data it consumes).

SQL query planning and optimizations are described in detail in the 
documentation for the [`opt` package](https://github.com/cockroachdb/cockroach/blob/c097a16427f65e9070991f062716d222ea5903fe/pkg/sql/opt/doc.go). 

The plan `optbuilder` [looks at the type of the
statement](https://github.com/cockroachdb/cockroach/blob/54c2bb7bce8219620e4b486ebcec1a0717a69c7d/pkg/sql/opt/optbuilder/builder.go#L258)
and, for each statement type, invokes a specific method that builds a memo 
group (memo is a data structure for efficiently storing a forest of query 
plans). For example, a memo group for a `SELECT` statement is produced by 
[`optbuilder.buildSelectClause()`](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L924). 
Notice how different aspects of a `SELECT` statement are handled:
a memo group for a ScanOp expression is created
(`optbuilder.buildSelectClause()`->...->
[`optbuilder.buildScan()`](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L114))
to scan a table, a `WHERE` clause is turned into an [`memo.FiltersExpr`](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/select.go#L1047),
an `ORDER BY` clause is [ordering physical property](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/orderby.go#L65),
etc. In the end, a
[`memo.RelExpr`](https://github.com/cockroachdb/cockroach/blob/2e5cce372e402faa2155c6f6e85410a9c7bdd238/pkg/sql/opt/optbuilder/with.go#L183-L185)
is produced, which contains all the plans from the above.

Finally, the execution plan is simplified, [optimized](https://github.com/cockroachdb/cockroach/blob/970a932bebef1ecb5b900ed011705d6044148106/pkg/sql/opt/xform/optimizer.go#L199), and
a physical plan is built by [`Builder.Build()`](https://github.com/cockroachdb/cockroach/blob/49a5d88f4810b89ce564c29c13137f0bf89fd4c7/pkg/sql/opt/exec/execbuilder/builder.go#L134).

For example, for the Scan operation above a [`scanNode` will be created](https://github.com/cockroachdb/cockroach/blob/7bc9b4f4d2542069efcb83b871a139ae660e8743/pkg/sql/opt_exec_factory.go#L79-L81).
This method belongs to the `Factory` interface generated by [`execFactoryGen.genExecFactory()`](https://github.com/cockroachdb/cockroach/blob/cf83f2670eb0d93a820f7e65075b334861e7f6ea/pkg/sql/opt/optgen/cmd/optgen/exec_factory_gen.go#L46-L47), and
implemented by [`execFactory`](https://github.com/cockroachdb/cockroach/blob/7bc9b4f4d2542069efcb83b871a139ae660e8743/pkg/sql/opt_exec_factory.go#L46) and
[`distSQLSpecExecFactory`](https://github.com/cockroachdb/cockroach/blob/7bc9b4f4d2542069efcb83b871a139ae660e8743/pkg/sql/distsql_spec_exec_factory.go#L32-L33).

To make this notion of the execution plan more concrete, consider one
actually "rendered" by the `EXPLAIN` statement:

```sql
root@:26257> CREATE TABLE customers(
    name STRING PRIMARY KEY,
    address STRING,
    state STRING,
    index SI (state)
);

root@:26257> INSERT INTO customers VALUES
    ('Google', '1600 Amphitheatre Parkway', 'CA'),
    ('Apple', '1 Infinite Loop', 'CA'),
    ('IBM', '1 New Orchard Road ', 'NY');

root@:26257> EXPLAIN(VERBOSE) SELECT * FROM customers WHERE address LIKE '%Infinite%' ORDER BY state;
                                         info
---------------------------------------------------------------------------------------
  distribution: full
  vectorized: true

  • sort
  │ columns: (name, address, state)
  │ ordering: +state
  │ estimated row count: 1
  │ order: +state
  │
  └── • filter
      │ columns: (name, address, state)
      │ estimated row count: 1
      │ filter: address LIKE '%Infinite%'
      │
      └── • scan
            columns: (name, address, state)
            estimated row count: 3 (100% of the table; stats collected 3 seconds ago)
            table: customers@primary
            spans: FULL SCAN
```

You can see data being produced by a `scanNode`, being filtered by a
`filterNode`, and then sorted by a `sortNode`.

With the parameter to display the query plan generated by the 
[cost-based optimizer](https://www.cockroachlabs.com/docs/v21.1/cost-based-optimizer) 
turned on, the `EXPLAIN` output becomes:

```
root@:26257> EXPLAIN (OPT,VERBOSE) SELECT * FROM customers WHERE address LIKE '%Infinite%' ORDER BY state;
                                          info
----------------------------------------------------------------------------------------
  sort
   ├── columns: name:1 address:2 state:3
   ├── stats: [rows=1, distinct(2)=1, null(2)=0]
   ├── cost: 18.68
   ├── key: (1)
   ├── fd: (1)-->(2,3)
   ├── ordering: +3
   ├── prune: (1,3)
   ├── interesting orderings: (+1) (+3,+1)
   └── select
        ├── columns: name:1 address:2 state:3
        ├── stats: [rows=1, distinct(2)=1, null(2)=0]
        ├── cost: 18.62
        ├── key: (1)
        ├── fd: (1)-->(2,3)
        ├── prune: (1,3)
        ├── interesting orderings: (+1) (+3,+1)
        ├── scan customers
        │    ├── columns: name:1 address:2 state:3
        │    ├── stats: [rows=3, distinct(1)=3, null(1)=0, distinct(2)=3, null(2)=0]
        │    │   histogram(1)=  0     1     0     1      0    1
        │    │                <--- 'Apple' --- 'Google' --- 'IBM'
        │    │   histogram(2)=  0          1          1               1
        │    │                <--- '1 Infinite Loop' --- '1600 Amphitheatre Parkway'
        │    ├── cost: 18.57
        │    ├── key: (1)
        │    ├── fd: (1)-->(2,3)
        │    ├── prune: (1-3)
        │    └── interesting orderings: (+1) (+3,+1)
        └── filters
             └── address:2 LIKE '%Infinite%' [outer=(2), constraints=(/2: (/NULL - ])]
```

#### Expressions

A subset of ASTs are
[`tree.Expr`](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/sem/tree/expr.go#L26-L27),
representing various "expressions" - parts of statements that can
occur in many various places - in a `WHERE` clause, in a `LIMIT`
clause, in an `ORDER BY` clause, as the projections of a `SELECT`
statement, etc. Expression nodes implement a common interface so that
a [visitor pattern](https://en.wikipedia.org/wiki/Visitor_pattern) can
be applied to them for different transformations and
analysis. Regardless of where they appear in the query, all
expressions need some common processing (e.g. names appearing in them
need to be resolved to columns from data sources). These tasks are
performed by
[`planner.analyzeExpr`](https://github.com/cockroachdb/cockroach/blob/a83c960a0547720a3179e05eb54ea5b67d107d10/pkg/sql/analyze.go#L1596). Each
`planNode` is responsible for calling `analyzeExpr` on the expressions
it contains, usually at node creation time (again, we hope to unify
our execution planning more in the future).

`planner.analyzeExpr` performs the following tasks:

1. Resolving names (the `colA` in `select 3 * colA from MyTable` needs
   to be replaced by an index within the rows produced by the underlying
   data source (usually a `scanNode`))
2. Normalization (e.g. `a = 1 + 1` -> `a = 2`, ` a not between b and c` -> `(a < b) or (a > c)`)
3. Type checking (see [the typing
   RFC](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20160203_typing.md)
   for an in-depth discussion of Cockroach's typing system).

   1. Constant folding (e.g. `1 + 2` becomes `3`): we perform exact
      arithmetic using [the same library used by the Go
      compiler](https://golang.org/pkg/go/constant/) and classify all the
      constants into two categories: [numeric -
      `NumVal`](https://github.com/cockroachdb/cockroach/blob/7fa9ae4ffc705cf9819a896091f0aada60f74463/pkg/sql/sem/tree/constant.go#L115)
      or [string-like -
      `StrVal`](https://github.com/cockroachdb/cockroach/blob/7fa9ae4ffc705cf9819a896091f0aada60f74463/pkg/sql/sem/tree/constant.go#L411). These
      representations of the constants are smart enough to figure out the
      set of types that can represent the value
      (e.g. [`NumVal.AvailableTypes`](https://github.com/cockroachdb/cockroach/blob/7fa9ae4ffc705cf9819a896091f0aada60f74463/pkg/sql/sem/tree/constant.go#L273-L274)
      - `5` can be represented as `int`, `decimal` or `float`, but `5.4` can
      only be represented as `decimal` or `float`) This will come in useful
      in the next step.

   2. Type inference and propagation: this analysis phase assigns a
      result type to an expression, and in the process types all the
      sub-expressions. Typed expressions are represented by the
      [`TypedExpr`](https://github.com/cockroachdb/cockroach/blob/31dcbfc964f22ede5b7f827f4e6c0091ccfb132f/pkg/sql/sem/tree/expr.go#L50)
      interface, and they are finally able to evaluate themselves to a
      result value through the `Eval` method. The typing algorithm is
      presented in detail in the typing RFC: the general idea is that
      it's a recursive algorithm operating on sub-expressions; each level
      of the recursion may take a hint about the desired outcome, and
      each expression node takes that hint into consideration while
      weighting what options it has. In the absence of a hint, there's
      also a set of "natural typing" rules. For example, a `NumVal`
      described above
      [checks](https://github.com/cockroachdb/cockroach/blob/7fa9ae4ffc705cf9819a896091f0aada60f74463/pkg/sql/sem/tree/constant.go#L66)
      whether the hint is compatible with its list of possible
      types. This process also deals with [`overload
      resolution`](https://github.com/cockroachdb/cockroach/blob/7fa9ae4ffc705cf9819a896091f0aada60f74463/pkg/sql/sem/tree/type_check.go#L305)
      for function calls and operators.
4. Replacing sub-query syntax nodes by a `sql.subquery` execution plan
   node.

A note about sub-queries: consider a query like `select * from
Employees where DepartmentID in (select DepartmentID from Departments
where NumEmployees > 100)`. The query on the `Departments` table is
called a sub-query. Subqueries are recognized and replaced with an
execution node by
[`subqueryVisitor`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/subquery.go#L294). The
subqueries are then run and replaced by their results through the
[`subqueryPlanVisitor`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/subquery.go#L194). This
is usually done by various top-level nodes when they start execution
(e.g. `renderNode.Start()`).

### Notable `planNodes`

**NB: This section has not been updated to the current code base. Many
of the broad concepts still apply, but the execution engine now looks
rather different with vectorized execution, cost-based optimization,
and distributed SQL processing.**

As hinted throughout, execution plan nodes are responsible for
executing parts of a query. Each one consumes data from lower-level
nodes, performs some logic, and feeds data into a higher-level one.

After being constructed, their main methods are
[`startExec`](https://github.com/cockroachdb/cockroach/blob/1d2fadff2299166292cd5274b54bf7fad5df53c3/pkg/sql/plan.go#L74),
which initiates the processing, and
[`Next`](https://github.com/cockroachdb/cockroach/blob/1d2fadff2299166292cd5274b54bf7fad5df53c3/pkg/sql/plan.go#L84),
which is called repeatedly to produce the next row.

To tie this to the [SQL Executor](#sql-executor) section above,
`executor.execLocal()`,
the method responsible for executing one statement, calls
`plan.Next()` repeatedly and accumulates the results.

Consider some `planNode`s involved in running a `SELECT`
statement, using the table defined above and

```sql
SELECT * FROM customers WHERE state LIKE 'C%' AND STRPOS(address, 'Infinite') != 0 ORDER BY name;
```

as a slightly contrived example. This is supposed to return customers
from states starting with "C" and whose address contains the string
"Infinite". To get excited, let's see the query plan for this
statement:

```sql
root@:26257> EXPLAIN SELECT * FROM customers WHERE state LIKE 'C%' and STRPOS(address, 'Infinite') != 0 ORDER BY name;
                                           info
-------------------------------------------------------------------------------------------
  distribution: full
  vectorized: true

  • sort
  │ estimated row count: 1
  │ order: +name
  │
  └── • filter
      │ estimated row count: 1
      │ filter: strpos(address, 'Infinite') != 0
      │
      └── • index join
          │ estimated row count: 2
          │ table: customers@primary
          │
          └── • scan
                estimated row count: 2 (67% of the table; stats collected 23 minutes ago)
                table: customers@si
                spans: [/'C' - /'D')
```

So the plan produced for this query, from top (highest-level) to
bottom, looks like:

```
sortNode -> filterNode -> indexJoinNode (primary) -> scanNode (si index)
```

Before we inspect the nodes in turn, one thing deserves explanation:
how did the `indexJoinNode` (which indicates that the query is going
to use the "si" index) come to be? The fact that this query uses an
index is not apparent in the syntactical structure of the `SELECT`
statement, and so this plan is not simply a product of the mechanical
tree building hinted to above. Indeed, there's a step that we haven't
mentioned before: [plan
expansion](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/expand_plan.go#L28). Among
other things, this step performs "index selection" (more information
about the algorithms currently used for index selection can be found
in [Radu's blog
post](https://www.cockroachlabs.com/blog/index-selection-cockroachdb-2/)). We're
looking for indexes that can be scanned to efficiently retrieve only
rows that match (part of) the filter. In our case, the "si" index
(state index) can be scanned to efficiently retrieve only the
rows that are candidates for satisfying the `state LIKE 'C%'`
expression (in an ecstasy to agony moment, we see that our index
selection / expression normalization code [is smart
enough](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/analyze.go#L1436)
to infer that `state LIKE 'C%'` implies `state >= 'C' AND state <
'D'`, but is not smart enough to infer that the two expressions are in
fact equivalent and thus the filter can be elided altogether). We
won't go into plan expansion or index selection here, but the index
selection process happens [in the expansion of the
`SelectNode`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/expand_plan.go#L283)
and, as a byproduct, produces `indexJoinNode`s configured with the
index spans to be scanned.

Now let's see how these `planNode`s run:

1. [`sortNode`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sort.go#L31):
   The `sortNode` sorts the rows produced by its child and corresponds to
   the `ORDER BY` SQL clause. The
   [constructor](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sort.go#L60)
   has a bunch of logic related to the quirky rules for name resolution
   from SQL92/99. Another interesting fact is that, if we're sorting by a
   non-trivial expression (e.g. `SELECT a, b ... ORDER BY a + b`), we
   need the `a + b` values (for every row) to be produced by a
   lower-level node. This is achieved through a pattern that's also
   present in other node: the lower node capable of evaluating
   expressions and rendering their results is the `renderNode`; the
   `sortNode` constructor checks if the expressions it needs are already
   rendered by that node and, if they are not, asks for them to be
   produced through the
   [`renderNode.addOrMergeRenders()`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sort.go#L206)
   method.  The actual sorting is performed in the `sortNode.Next()`
   method. The first time it is called, [it consumes all the data
   produced by the child
   node](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sort.go#L359)
   and accumulates it into `n.sortStrategy` (an interface hiding multiple
   sorting algorithms). When the last row is consumed,
   `n.sortStrategy.Finish()` is called, at which time the sorting
   algorithm finishes its processing. Subsequent calls to
   `sortNode.Next()` simply iterate through the results of sorting
   algorithm.

2. [`indexJoinNode`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/index_join.go#L30):
   The `indexJoinNode` implements joining of results from an index with
   the rows of a table. It is used when an index can be used for a query,
   but it doesn't contain all the necessary columns; columns not
   available in the index need to be retrieved from the Primary Key (PK)
   key-values. The `indexJoinNode` sits on top of two scan nodes - one
   configured to scan the index, and one that is constantly reconfigured
   to do "point lookups" by PK. In the case of our query, we can see that
   the "SI" index is used to read a compact set of rows that match the
   "state" filter but, since it doesn't contain the "address" columns,
   the PK also needs to be used. Each index KV pair contains the primary
   key of the row, so there is enough information to do PK
   lookups. [`indexJoinNode.Next`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/index_join.go#L273)
   keeps reading rows from the index and, for each one, adds a spans to
   be read by the PK. Once enough such spans have been batched, they are
   all [read from the
   PK](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/index_join.go#L257). As
   described in the section on [SQL rows to KV
   pairs](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#data-mapping-between-the-sql-model-and-kv))
   from the design doc, each SQL row is represented as a single KV pair
   in the indexes, but as multiple consecutive rows in the PK
   (represented by a "key span").

   An interesting detail has to do with
   how filters are handled: note that the `state LIKE 'C%'` condition is
   evaluated by the index scan, and the `strpos(address, 'Infinite') !=
   0` condition is evaluated by the PK scan. This is nice because it
   means that we will be filtering as much as we can on the index side and
   we will be doing fewer expensive PK lookups. The code that figures out
   which conjunction is to be evaluated where is in `splitFilter()`,
   called by the [`indexJoinNode`
   constructor](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/index_join.go#L180).

3. [`scanNode`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/scan.go#L33):
   The `scanNode` generally constitutes the source of a `renderNode` or `filterNode`;
   it is responsible for scanning over the key/value pairs for a table or
   index and reconstructing them into rows. This node is starting to
   smell like rubber meeting a road, because we are getting closer to the
   actual data - the monolithic, distributed KV map. You'll see that the
   [`Next()`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/scan.go#L214)
   method is not particularly climactic, since it delegates the work to a
   `rowFetcher`, described below. There's one interesting thing that the
   `scanNode` does: it [runs a filter
   expression](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/scan.go#L233),
   just like the `filterNode`. That is because we are trying to push down
   parts of the `WHERE` clause as far as possible. This is generally a
   work in progress, see `filter_opt.go`. The idea is
   that a query like
```sql
/* Select the orders placed by each customer in the first year of membership. */
SELECT * FROM Orders o inner join Customers c ON o.CustomerID = c.ID WHERE Orders.amount > 10 AND Customers.State = 'NY' AND age(c.JoinDate, o.Date) < INTERVAL '1 year'
```
   is going to be compiled into two `scanNode`s, one for `Customers`,
   one for `Orders`. Each one of them can do the part of filtering
   that refers exclusively to their respective tables, and then the
   higher-level `joinNode` only needs to evaluate expressions that
   need data from both (i.e. `age(c.JoinDate, o.Date) < INTERVAL '1
   year'`).

   Let's continue downwards, looking at the structures that the
   `scanNode` uses for actually reading data.

   1. [`rowFetcher`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sqlbase/rowfetcher.go#L35):
      The `rowFetcher` is responsible for iterating through key-value
      pairs, figuring out where a SQL table or index row ends (remember
      that a SQL row is potentially encoded in multiple KV entries), and
      decoding all the keys and values in SQL column values, dealing with
      differences between the primary index and other indexes and with
      the [layout of a
      table](https://www.cockroachlabs.com/docs/stable/column-families.html). For
      details on the mapping between SQL rows and KV pairs, see the
      [corresponding section from the Design
      Doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#data-mapping-between-the-sql-model-and-kv) and the [encoding tech note](encoding.md).

      The `rowFetcher` also [performs
      decoding](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sqlbase/table.go#L953)
      from on-disk byte arrays to the representation of data that we do
      most processing on: implementation of the
      [`parser.Datum`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/parser/datum.go#L57)
      interface. For details on what the on-disk format is for different
      data types, browse around the [`util/encoding
      directory`](https://github.com/cockroachdb/cockroach/tree/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/util/encoding).

      For actually reading a KV pair from the database, the `rowFetcher` delegates to the `kvBatchFetcher`.

   2. [`kvBatchFetcher`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sqlbase/kvfetcher.go#L84):
      The `kvBatchFetcher` finally reads data from the KV database. It
      understands nothing of SQL concepts, such as tables, rows or
      columns. When it is created, it is configured with a number of "key
      spans" that it needs to read (these might be, for example, a single
      span for reading a whole table, or a couple of spans for reading
      parts of the PK or of an index).

      To actually read data from the KV database, the `kvBatchFetcher` uses the
      KV layer's "client" interface, namely
      [`client.Batch`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/internal/client/batch.go#L30). This
      is where the "SQL layer" interfaces with the "KV layer" - the
      `kvBatchFetcher` will
      [build](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sqlbase/kvfetcher.go#L197)
      such `Batch`es of requests, [send them for
      execution](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sqlbase/kvfetcher.go#L203)
      in the context of the KV transaction (remember the `Transaction`
      mentioned in the [Statement Execution
      section](#StatementExecution)), [read the
      results](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/sql/sqlbase/kvfetcher.go#L220)
      and return them to the hierarchy of `planNodes`. The requests being
      sent to the KV layer, in the case of this read-only query, are
      [`ScanRequest`s](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/roachpb/api.proto#L204).

The rest of this document will walk through the "execution" of KV
requests, such as the ones sent by the `kvBatchFetcher`.

## KV: key/value storage service

The KV layer of CockroachDB is a transactional, distributed key/value
storage service. A full description of its architecture is outside of the
scope of this article, see the
[architecture overview](https://www.cockroachlabs.com/docs/stable/architecture/overview.html). 
Briefly, the KV service maintains a single ordered key/value map
split into multiple contiguous ranges. Each range corresponds to a separate
[Raft](https://raft.github.io) consensus cluster, where each Raft node is called
a replica. A single CockroachDB node contains many replicas belonging to many
ranges, using a common underlying store. Cross-range transactions are achieved
through a [distributed 2-phase commit protocol](https://www.cockroachlabs.com/docs/v21.1/architecture/transaction-layer.html)
coordinated by the KV client.

The KV service is request-oriented, with a
[Protocol Buffers](https://developers.google.com/protocol-buffers)-based
[gRPC](https://grpc.io) API defined in [`api.proto`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto).
In practice, the KV client always sends
[`BatchRequest`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L2023),
a generic request containing a collection of other requests. All
requests have a
[`Header`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L1841)
which contains routing information (e.g. which replica a request is
destined for) and [transaction
information](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/roachpb/api.proto#L818)
(which transaction's context to execute the request in). The
corresponding response is, unsurprisingly,
[`BatchResponse`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L2035).

### The KV client interface

Clients send KV requests via the internal
[`kv.DB`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/db.go#L250-L251)
client interface, typically by obtaining a
[`kv.Txn`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L46)
transaction handle from
[`DB.NewTxn()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/db.go#L755)
and calling methods on it such as
[`Get()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L399)
and [`Put()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L452).
Recall the SQL engine using this `kv.Txn` to execute statements in the
context of a transaction.

Since we often want to send multiple operations in the same request
(for performance), they are grouped in a
[`kv.Batch`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/batch.go#L33-L34)
which can be obtained via [`Txn.NewBatch()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L388).
Calling e.g. [`Batch.Get()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/batch.go#L357)
and [`Batch.Put()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/batch.go#L399)
adds those operations to the batch, and
[`Txn.Run()`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L631)
sends the batch to the server as a `BatchRequest`, populating
[`Batch.Results`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/batch.go#L48)
with the results. Convenience methods such as `Txn.Get()` simply
create a batch internally with a single operation and run it.

If you trace what happens inside `Txn.Run()` you eventually get to
[`txn.db.sendUsingSender(ctx, batchRequest, txn.mu.sender)`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/txn.go#L982)
which will [call `sender.Send(ctx, batchRequest)`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/kv/db.go#L817) on the
passed sender. At this point, the request starts percolating through
a hierarchy of [`Sender`s](https://github.com/cockroachdb/cockroach/blob/cfb4b95d94e717e985423d5313c58187fbde5da4/pkg/kv/sender.go#L54) -
objects that perform various peripheral tasks and ultimately route
the request to a replica for execution.

`Sender`s have a single method, `Send()`, which ultimately passes
the request to the next sender. Let's go down this "sending" rabbit
hole:

`TxnCoordSender` → `DistSender` → `Node` → `Stores` → `Store` → `Replica`

The first two run on the same node that received the SQL query and is doing the
SQL processing (the "gateway node"), the others run on the nodes responsible for
the data that is being accessed (the "range node").

### TxnCoordSender

The top-most sender is the
[`TxnCoordSender`](https://github.com/cockroachdb/cockroach/blob/cfb4b95d94e717e985423d5313c58187fbde5da4/pkg/kv/kvclient/kvcoord/txn_coord_sender.go#LL95),
which is responsible for managing transaction state (see the
[Transaction Management section of the design doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)).
All operations for a transaction go though the same `TxnCoordSender` instance
(with some exceptions for distributed SQL processing), and if it crashes then
the transaction is aborted.

It is subdivided into an [interceptor stack](https://github.com/cockroachdb/cockroach/blob/cfb4b95d94e717e985423d5313c58187fbde5da4/pkg/kv/kvclient/kvcoord/txn_coord_sender.go#L140-L150)
consisting of [`lockedSender`s](https://github.com/cockroachdb/cockroach/blob/f176a39446e5235c9985a07f2550583dc106c26c/pkg/kv/kvclient/kvcoord/txn_lock_gatekeeper.go#L23-L24) - 
basically senders that share the `TxnCoordSender.mu` mutex and make up
a single [critical section](https://en.wikipedia.org/wiki/Critical_section).
These interceptors are, from top to bottom:

* [`txnHeartbeater`](https://github.com/cockroachdb/cockroach/blob/cc83f59eed96e4ecc829b40d3fa53b85e55fcfb0/pkg/kv/kvclient/kvcoord/txn_interceptor_heartbeater.go#L55):
creates and periodically heartbeats the transaction record to mark the
transaction as alive, and detects external aborts of the transaction.

* [`txnSeqNumAllocator`](https://github.com/cockroachdb/cockroach/blob/5e7f8bd7a511f58204a1419452fd4d439f9ccec8/pkg/kv/kvclient/kvcoord/txn_interceptor_seq_num_allocator.go#L60):
assigns sequence numbers to the operations within a transaction, for
ordering and idempotency.

* [`txnPipeliner`](https://github.com/cockroachdb/cockroach/blob/1bfd8cceb2b7f06970a26e800eacee372a203523/pkg/kv/kvclient/kvcoord/txn_interceptor_pipeliner.go#L179):
[pipelines writes](https://www.cockroachlabs.com/blog/transaction-pipelining/)
by asynchronously submitting them to consensus and keeping track of in-flight
writes, coordinating them with dependant transaction operations.

* [`txnSpanRefresher`](https://github.com/cockroachdb/cockroach/blob/ee23325c42c4464aa24e863d9cadf1a2e244a842/pkg/kv/kvclient/kvcoord/txn_interceptor_span_refresher.go#L104):
keeps track of transactional reads, and in the case of serialization errors
checks if its past reads are still valid at a higher transaction timestamp.
If they are, the transaction can continue at the higher timestamp without
propagating the error to the client and retrying the entire transaction.

* [`txnCommitter`](https://github.com/cockroachdb/cockroach/blob/ee23325c42c4464aa24e863d9cadf1a2e244a842/pkg/kv/kvclient/kvcoord/txn_interceptor_committer.go#L112):
manages commits and rollbacks and, in particular, implements the
[parallel commit protocol](https://www.cockroachlabs.com/blog/parallel-commits/).

* [`txnMetricRecorder`](https://github.com/cockroachdb/cockroach/blob/5e7f8bd7a511f58204a1419452fd4d439f9ccec8/pkg/kv/kvclient/kvcoord/txn_interceptor_metric_recorder.go#L27):
records various transaction request metrics.

* [`txnLockGatekeeper`](https://github.com/cockroachdb/cockroach/blob/f176a39446e5235c9985a07f2550583dc106c26c/pkg/kv/kvclient/kvcoord/txn_lock_gatekeeper.go#L41):
unlocks the `TxnCoordSender.mu` while requests are in flight, and enforces
a synchronous client protocol for relevant requests.

After traversing the `TxnCoordSender` stack, the request is passed on
to the `DistSender`.

### DistSender

The [`DistSender`](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L235-L236)
is truly a workhorse: it handles the communication between the gateway
node and the (possibly many) range nodes, putting the "distributed" in
"distributed database". It receives `BatchRequest`s, looks at the
requests inside the batch, figures out what range each command needs
to go to, finds the nodes/replicas responsible for that range, routes
the requests there and then collects and reassembles the results.

Let's go through the code a bit:

1. The request is subdivided into ranges: `DistSender.Send()` calls
   [`DistSender.divideAndSendBatchToRanges()`](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L755)
   which
   [iterates](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1110)
   over the constituent ranges of the request by using a
   [`RangeIterator`](https://github.com/cockroachdb/cockroach/blob/ac572af5be4e35dbdd58a95e32e5d1f6c24bc24c/pkg/kv/kvclient/kvcoord/range_iter.go#L28).
   Recall that a single request, such as a `ScanRequest`, can refer to a key
   span that might straddle many ranges.
   
   A lot of things hide behind this innocent-looking iteration: the cluster's
   range metadata needs to be accessed in order to find the mapping of keys to
   ranges (info on this metadata can be found in the [Range Metadata
   section](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#range-metadata)
   of the design doc). Range metadata is stored as regular data in the
   cluster, in a two-level index mapping range end keys to descriptors
   about the replicas of the respective range (the ranges storing this
   index are called "meta-ranges"). The `RangeIterator` logically
   iterates over these descriptors, in range key order.
   
   Brace yourselves: for moving from one range to the next, the iterator [calls
   back into the `DistSender`](https://github.com/cockroachdb/cockroach/blob/ac572af5be4e35dbdd58a95e32e5d1f6c24bc24c/pkg/kv/kvclient/kvcoord/range_iter.go#L193),
   which knows how to find the descriptor of the range responsible for
   one particular key. The `DistSender`
   [delegates](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L550)
   range descriptor lookup to the
   [`RangeCache`](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L75)
   (a LRU tree cache indexed by range end key). This cache
   desynchronizes with reality as ranges in a cluster split or move
   around; when an entry is discovered to be stale, we'll see below that
   the `DistSender` removes it from the cache.

   In the happy case, the cache has information about a descriptor covering the
   key we're interested in and it [returns it](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L620).
   In the unhappy case, it needs to [look up](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L649)
   the range descriptor in the range database `RangeCache.db`. This is an
   [interface](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L57)
   which [hides the `DistSender` itself](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L349),
   which will recursively [lookup the range's meta-key](https://github.com/cockroachdb/cockroach/blob/9b449c55c9af9ba4975fea51b1d798254955008f/pkg/kv/range_lookup.go#L197) by [calling into `DistSender.Send()`](https://github.com/cockroachdb/cockroach/blob/9b449c55c9af9ba4975fea51b1d798254955008f/pkg/kv/range_lookup.go#L312), which calls into the range cache, and so on.
   
   This recursion cannot go on forever. The descriptor of a regular range is in
   a meta2-range (a 2nd level index range), and the descriptors for meta2-ranges
   are present in the [(one and only) meta1-range](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L791),
   which is [provided](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/server/server.go#L404) by [`Gossip.GetFirstRangeDescriptor()`](https://github.com/cockroachdb/cockroach/blob/8c5253bbccea8966fe20e3cb8cc6111712f6f509/pkg/gossip/gossip.go#L1596).
   
2. Each sub-request (partial batch) is sent to its range. This is done
   through the call to
   [`DistSender.sendPartialBatchAsync()`](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1259)
   which
   [truncates](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1416)
   all the requests in the batch to the current range and then it
   [sends](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1478)
   the truncated batch to a range. All these partial batches are sent
   concurrently.

   `sendPartialBatch()` is the level at which an error stemming from stale
   `RangeCache` information is handled: the range descriptor that's detected to be stale is
   [evicted](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1523)
   from the cache and the partial batch is retried.

3. Sending a partial batch to a single range implies selecting the
   right replica of that range and performing an RPC to it. By default,
   each range is replicated three ways, but only one of the three
   replicas is the "lease holder" - the temporarily designed owner of
   that range, in charge of coordinating all reads and writes to it (see
   the [Range Leases
   section](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#range-leases)
   in the design doc). The leaseholder is also [cached in `RangeCache`](https://github.com/cockroachdb/cockroach/blob/d5f73c6a05c99f7c6b40b80dea80e8ed8ab15cac/pkg/kv/kvclient/rangecache/range_cache.go#L299), and the information can get stale.

   The `DistSender` method dealing with this is
   [`sendToReplicas`](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/kv/dist_sender.go#L451). It
   will use the cache to send the request to the lease holder, but it's
   also prepared to try the other replicas, in [order of
   "proximity"](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1764). The
   replica that the cache says is the leaseholder is simply [moved to the
   front](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1772)
   of the list of replicas to be tried and then an [RPC is
   sent](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1860)
   to all of them, in order, until one succeeds. If the leaseholder
   fails, we assume it is stale and [evict it from the cache](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1936).

4. Actually sending the RPCs is hidden behind the
   [`Transport` interface](https://github.com/cockroachdb/cockroach/blob/4c76e19157ced57ba082ea0ab8cf5b317bdb5e1f/pkg/kv/kvclient/kvcoord/transport.go#L51).
   Concretely, [`grpcTransport.SendNext()`](https://github.com/cockroachdb/cockroach/blob/4c76e19157ced57ba082ea0ab8cf5b317bdb5e1f/pkg/kv/kvclient/kvcoord/transport.go#L165)
   makes gRPC calls to the nodes containing the destination replicas, via the
   [`Internal` service](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L2224).

5. The (async) responses from the different replicas are
   [combined](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1199)
   into a single `BatchResponse`, which is ultimately returned from
   `DistSender.Send()`.

We've now gone through the relevant things that happen on the gateway node.
Next, we'll have a look at what happens on the "remote" side - on each of the
ranges.

### RPC server - Node

We've seen how the `DistSender` splits `BatchRequest` into partial
batches, each containing commands local to a single replica, and how
these commands are sent to the lease holders of their ranges through
RPCs. We're now moving to the "server" end of these RPCs.

The struct that implements the [`Internal` RPC service](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L2224)
is [`Node`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/server/node.go#L156).
It doesn't do anything of great relevance itself, but
[delegates](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/server/node.go#L878)
the request to
[`Stores`](https://github.com/cockroachdb/cockroach/blob/e8f925edd18d8800671a1a606f32d3ed9e299b3f/pkg/kv/kvserver/stores.go#L39)
which represents a collection of "stores" (on-disk databases,
typically one per physical disk -- see the [Architecture
section](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#architecture)
of the design doc). `Stores` implements the `Sender` interface,
just like the gateway layers that we've seen before, resuming the
pattern of sending requests down through a stack of `Sender`s via
the `Send()` method.

### Store

`Stores.Send()`
[identifies](https://github.com/cockroachdb/cockroach/blob/e8f925edd18d8800671a1a606f32d3ed9e299b3f/pkg/kv/kvserver/stores.go#L186)
which particular store contains the destination replica (based on
[request routing info](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L1852)
filled in by the `DistSender`), and
[routes](https://github.com/cockroachdb/cockroach/blob/e8f925edd18d8800671a1a606f32d3ed9e299b3f/pkg/kv/kvserver/stores.go#L191)
the request there.

A [`Store`](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvserver/store.go#L388)
typically represents one physical disk device. For our purposes, it mostly
[delegates](https://github.com/cockroachdb/cockroach/blob/cdd48a011e849edd404a29b5a8bd52793f019c7d/pkg/kv/kvserver/store_send.go#L195)
the request to a `Replica`, but one other interesting thing it does is,
in case requests from the current transaction have already been processed
on this node, it [updates the upper
bound](https://github.com/cockroachdb/cockroach/blob/cdd48a011e849edd404a29b5a8bd52793f019c7d/pkg/kv/kvserver/store_send.go#L156)
on the uncertainty interval to be used by the current request (see the
["Choosing a Timestamp" section of the design
doc](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md))
for details on uncertainty intervals). The uncertainty interval
dictates which timestamps for values are ambiguous because of clock
skew between nodes (the values for which don't know if they were
written before or after the serialization point of the current
txn). This code realizes that, if a request from the current txn has
been processed on this node before, no value written after that node's
timestamp at the time of that other request processing is ambiguous.

### Replica - executing reads, proposing writes via Raft

A
[`Replica`](https://github.com/cockroachdb/cockroach/blob/1041afb50f9bd2f7b3ccf704efb9cc2eac619b86/pkg/kv/kvserver/replica.go#L184)
represents one copy of a range, which in turn is a contiguous keyspace
managed by one instance of the Raft consensus algorithm. The system
tries to keep ranges around 512MB, by default. The `Replica` is the
final `Sender` in our hierarchy. The role of all the other `Sender`s
was, mostly, to route requests to the `Replica` currently acting as
the lease holder for the range (a _primus inter pares_ `Replica` that
takes on a bunch of coordination responsibilities we'll explore
below). A replica deals with read requests differently than write
requests: reads are evaluated directly, whereas writes will enter
another big chapter in their life and go through the Raft consensus
protocol.

The difference between the read and write request paths can be seen
immediately: `Replica.Send()` quickly [branches
off](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L100-L107)
based on the request type. We'll talk about the read and write paths
in turn.

Before that, however, both request types are wrapped in
[`Replica.executeBatchWithConcurrencyRetries()`](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L325)
which handles concurrency control for the batch: in a big
[retry loop](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L346) it will attempt to
[acquire latches](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L357)
(internal locks) via `concurrency.Manager.SequenceReq()` and
[execute the batch](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L375).
Any concurrency-related errors will be handled here, if possible, before
retrying the batch -- e.g. [write intent resolution](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L400-L404),
[transaction conflicts](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L405-L409),
[lease errors](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L418-L427),
and so on.

#### Concurrency control

Our request will most likely not arrive at the replica alone, but rather be
joined by a variety of other requests sent by many different nodes, clients, and
transactions. The
[`concurrency.Manager`](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_control.go#L148)
is responsible for deciding who goes when and in what order. Broadly, it will
allow readers to run concurrently while writers run alone, in FIFO order and
taking into account e.g. key spans, MVCC timestamps, and transaction isolation.

First, the replica [figures out](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L330)
which latches and locks need to be taken out for the requests. "Latches" are
single-request internal locks taken out for the duration of request evaluation
and replication, while "locks" are cross-request transactional locks (e.g. due
to an uncommitted write), supporting both read-only and read/write access modes.
To do this, it [looks up](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L330)
the command for each request, and [calls](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L829)
its `DeclareKeys` method with the request header. This method is provided during
[command registration](https://github.com/cockroachdb/cockroach/blob/b7fb524c899c38e3e92c80d6b88f45afc9df7ca2/pkg/kv/kvserver/batcheval/command.go#L87)
(see e.g. the [`Put` command](https://github.com/cockroachdb/cockroach/blob/702d8bb2d2432c35aee90a543f7b2facc7c9c65e/pkg/kv/kvserver/batcheval/cmd_put.go#L23-L39)),
and will often delegate to either [`DefaultDeclareKey`](https://github.com/cockroachdb/cockroach/blob/a7472e3c109224d9dae3f739fe1d99fb12c22327/pkg/kv/kvserver/batcheval/declare.go#L24)
(taking out latches) or
[`DefaultDeclareIsolatedKeys`](https://github.com/cockroachdb/cockroach/blob/a7472e3c109224d9dae3f739fe1d99fb12c22327/pkg/kv/kvserver/batcheval/declare.go#L39)
(taking out locks and latches) for the keys
[listed](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L50-L57)
in the request header.

Once the necessary latches and locks are known, the request will pass these to
the concurrency manager and
[wait for its turn](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L357)
by calling `Manager.SequenceReq()`. This will first attempt to
[acquire latches](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L206)
in the [latch manager](https://github.com/cockroachdb/cockroach/blob/fd30130e281d712a52c7cf65988278a354e39250/pkg/kv/kvserver/spanlatch/manager.go#L58), which may require it to
[wait](https://github.com/cockroachdb/cockroach/blob/fd30130e281d712a52c7cf65988278a354e39250/pkg/kv/kvserver/spanlatch/manager.go#L207)
for them to be released by other requests first. Locks are handled similarly,
by attempting to [acquire locks](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L226)
in the [lock table](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_control.go#L488) 
and [waiting](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L230)
for any conflicting transactions to complete -- this will also attempt to [push](https://github.com/cockroachdb/cockroach/blob/8132781d23003c0c6fa597d944fd94f665229093/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L430) the lock holder's timestamp forward, which may allow our transaction to run "before it", or even abort it depending on transaction priorities.

Once the request has acquired its latches and locks, it is ready to
[execute](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L375).
At this point, the request no longer has to worry about concurrent requests, as
it is fully isolated. There is one notable exception, however: write intents
(new values) written by concurrent transactions, which have not yet been fully
migrated into the concurrency manager, and must be handled as they are encountered.
We will get back to write intents later.

#### Lease acquisition and redirection

The first thing both `Replica.executeReadBatch()` and `Replica.executeWriteBatch()`
does is to call
[`Replica.checkExecutionCanProceed()`](https://github.com/cockroachdb/cockroach/blob/1041afb50f9bd2f7b3ccf704efb9cc2eac619b86/pkg/kv/kvserver/replica.go#L1258)
to perform a few pre-flight checks. Most importantly, it checks that the request
got to the right place, i.e. that the replica is the current lease holder.
Remember that a lot of the routing was done based on caches or outright guesses.

The leaseholder check is performed by [`Replica.leaseGoodToGoRLocked()`](https://github.com/cockroachdb/cockroach/blob/1041afb50f9bd2f7b3ccf704efb9cc2eac619b86/pkg/kv/kvserver/replica.go#L1318), a rabbit hole in its own right. Let's just say that, in case the
current replica is not the lease holder, it returns either
[`InvalidLeaseError`](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L979),
which will [cause](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L425)
the replica to try [acquiring the lease](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L576),
or [`NotLeaseholderError`](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L1023-L1024),
which is [returned to](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1977) the `DistSender` and [redirects](https://github.com/cockroachdb/cockroach/blob/904807640f6a28aa82c5e63f0b62de2dbf27b3a0/pkg/kv/kvclient/kvcoord/dist_sender.go#L1988)
the request to the real leaseholder.

Requesting a lease is done through the
[`pendingLeaseRequest`](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L122)
helper struct, which coalesces multiple requests for the same lease
and eventually constructs a
[`RequestLeaseRequest`](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L269)
and [sends it for
execution](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L423)
directly to the replica (as we've seen in other cases, bypassing all
the senders to avoid recursing infinitely). In case a lease is
requested, `redirectOnOrAcquireLease` will
[wait](https://github.com/cockroachdb/cockroach/blob/1f31803e9d158cae636b35856fb01aed9187ed93/pkg/kv/kvserver/replica_range_lease.go#L1182)
for that request to complete and check if it was successful. If it was,
the batch will be retried.

#### Read request path

Once the lease situation has been settled, `Replica.executeReadOnlyBatch()`
is ready to actually start reading. It [grabs](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_read.go#L57)
a storage engine handle, and [executes](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_read.go#L83)
the batch on it, eventually moving control to
[`Replica.evaluateBatch()`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L146)
which [calls `Replica.evaluateCommand()`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L271-L272).
for each request in the batch. `evaluateCommand()`
[looks up](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L497)
the command for the request's method in a [command registry](https://github.com/cockroachdb/cockroach/blob/b7fb524c899c38e3e92c80d6b88f45afc9df7ca2/pkg/kv/kvserver/batcheval/command.go#L69-L70),
and [passes execution](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L506-L510)
to it.

One typical read request is a [`ScanRequest`](https://github.com/cockroachdb/cockroach/blob/4e4f31a0ed1a8ea985b9ab6f72e29266b259900e/pkg/roachpb/api.proto#L384),
which is evaluated by
[`batcheval.Scan`](https://github.com/cockroachdb/cockroach/blob/261fc356765e61215478ba697d4fe252b9466c8f/pkg/kv/kvserver/batcheval/cmd_scan.go#L31).
The code is rather brief, and mostly
[calls](https://github.com/cockroachdb/cockroach/blob/261fc356765e61215478ba697d4fe252b9466c8f/pkg/kv/kvserver/batcheval/cmd_scan.go#L54-L55)
a corresponding function for the underlying storage engine.

#### Engine

We're getting to the bottom of the CockroachDB stack. The
[`Engine`](https://github.com/cockroachdb/cockroach/blob/83185789481f23bd6c4938a3588203f4cd4eef89/pkg/storage/engine.go#L640)
is an interface abstracting away different on-disk stores. From
version 21.1 onwards, we only support [Pebble](https://github.com/cockroachdb/pebble),
an embedded key/value database we developed ourselves based on
[RocksDB](https://rocksdb.org)/[LevelDB](https://github.com/google/leveldb).
RocksDB was previously used as the storage engine, but was replaced in pursuit of
[performance, stability, and tighter integration](https://www.cockroachlabs.com/blog/pebble-rocksdb-kv-store/).
Even though the storage engine is a crucial part of servicing a request, we
won't go into any details on Pebble's internals here, in the interest of
brevity.

For reads, we typically make use of the
[`storage.Reader`](https://github.com/cockroachdb/cockroach/blob/83185789481f23bd6c4938a3588203f4cd4eef89/pkg/storage/engine.go#L349)
interface, a subset of `Engine`. The `Scan` command from the previous
section passes this `Reader` to [`storage.MVCCScanToBytes()`](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2585),
which is a helper function that fetches a list of key/value pairs in a
given key span from the storage engine.

The CockroachDB storage layer uses [Multi-Version Concurrency Control (MVCC)](https://en.wikipedia.org/wiki/Multiversion_concurrency_control),
storing multiple versions of Protobuf-encoded [`Value`s](https://github.com/cockroachdb/cockroach/blob/0254e86051c310d38e5668cf82ba128ff4c1ab28/pkg/roachpb/data.proto#L84)
for a given key. Each version is identified by the value's
[`Timestamp`](https://github.com/cockroachdb/cockroach/blob/0254e86051c310d38e5668cf82ba128ff4c1ab28/pkg/roachpb/data.proto#L90),
and the latest version visible to a given transaction is given by its
[read timestamp](https://github.com/cockroachdb/cockroach/blob/0254e86051c310d38e5668cf82ba128ff4c1ab28/pkg/roachpb/data.proto#L388).
As a special case, new values (called write intents) are written
without a timestamp, and once the transaction commits these will
have to be resolved by rewriting them with the transaction's final
[write timestamp](https://github.com/cockroachdb/cockroach/blob/175e2daacc94cb47350be4c668e4236664515bbd/pkg/storage/enginepb/mvcc3.proto#L91) --
more on this later.

The workhorse of MVCC reads is the
[`storage.MVCCIterator`](https://github.com/cockroachdb/cockroach/blob/83185789481f23bd6c4938a3588203f4cd4eef89/pkg/storage/engine.go#L92),
which iterates over MVCC key/value pairs. `MVCCScanToBytes`
[makes use of this](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2592-L2593)
with the help of [`pebbleMVCCScanner`](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2385)
to [scan](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/pebble_mvcc_scanner.go#L215) over the key/value versions
in the storage engine, [collect](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/pebble_mvcc_scanner.go#L329)
the appropriate version of each key, and
[return them](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2403)
to the caller.

One notable exception to this is if the scan
[encounters conflicting write intents](https://github.com/cockroachdb/cockroach/blob/129b2dcaecfce350d4ed3a4b1f6b4b654b0bd11e/pkg/storage/mvcc.go#L2400-L2402)
from a different transaction. In this case, it returns a `WriteIntentError`
containing the encountered intents, which will need to be
[handled](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L400-L404)
by `executeBatchWithConcurrencyRetries()` further up the stack.

#### Intent resolution

In case the request runs into write intents (i.e. uncommitted values),
it has to deal with these to handle read-write and write-write
conflicts between transactions.

As we've touched on, MVCC storage operations will return `WriteIntentError`
if they encounter conflicting intents, and these errors are
[handled](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L400-L404)
in `Replica.executeBatchWithConcurrencyRetries` by
[delegating](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L518)
to [`concurrency.Manager.HandleWriterIntentError`](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L309).
This will first [add the intents to the lock table](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L325), 
since it clearly did not know about them yet or the request wouldn't have failed
(recall that write intents have not yet been fully migrated into the concurrency
manager's lock table), and then [wait](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/concurrency/concurrency_manager.go#L346) 
for the conflicting transaction to complete. As part of this waiting, it will try to
[push](https://github.com/cockroachdb/cockroach/blob/8132781d23003c0c6fa597d944fd94f665229093/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L430)
the lock holder using the
[`IntentResolver`](https://github.com/cockroachdb/cockroach/blob/3575dea3e6b084a63a1432672705f949572bc211/pkg/kv/kvserver/intentresolver/intent_resolver.go#L132)
to resolve the conflicting intents.

Resolving means figuring out if the transaction to which the intent belongs is
still pending (it might already be committed or aborted, in which case the
intent is "resolved"), or possibly "pushing" the transaction in question
(forcing it to restart at a higher timestamp, such that it doesn't conflict with
the current txn). If the conflicting txn is no longer pending or if it was
pushed, then the intents can be properly resolved (i.e. either replaced by a
committed value, or simply discarded).

The first part - figuring out the txn status or pushing it - is done in
[`IntentResolver.MaybePushTransaction`](https://github.com/cockroachdb/cockroach/blob/3575dea3e6b084a63a1432672705f949572bc211/pkg/kv/kvserver/intentresolver/intent_resolver.go#L379):
we can see that a series of `PushTxnRequest`s are batched and sent to the
cluster (meaning the hierarchy of `Sender`s on the current node will be used,
top to bottom, to route the requests to the various transaction records - see
the ["Transaction execution flow" section](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md)
of the design doc). In case the transaction we're trying to push is still
pending, the decision about whether or not the push is successful is done [deep
in the processing of the
`PushTxnRequest`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/batcheval/cmd_push_txn.go#L254)
based on the relative priorities of the pusher/pushee txns.

The second part -- replacing the intents that can now be resolved -- is
done through a call to
[`IntentResolve.ResolveIntent`](https://github.com/cockroachdb/cockroach/blob/8132781d23003c0c6fa597d944fd94f665229093/pkg/kv/kvserver/concurrency/lock_table_waiter.go#L480),
which will either convert the intent to a normal committed value or remove it.

Back where we left off in `Replica.executeBatchWithConcurrencyRetries`, the
request will now be [retried](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_send.go#L346).

#### Write request path

Write requests are conceptually more interesting than reads because
they're not simply serviced by one node/replica. Instead, they go
through the Raft consensus algorithm, which maintains an ordered
commit log, and are then applied by all of a range's replicas (see the
[Raft
section](https://github.com/cockroachdb/cockroach/blob/master/docs/design.md#raft---consistency-of-range-replicas)
of the design doc for more details). The replica that initiates this
process is, just like in the read case, the lease holder. Execution on
this lease holder is thus broken into two stages - before ("upstream
of" in code) Raft and below ("downstream of") Raft. The upstream stage
will eventually block for the corresponding Raft command to be applied
*locally* (after the command has been applied locally, future reads
are guaranteed to see its effects).

For what follows we'll introduce some terminology. We've already seen
that a replica (and the KV subsystem in general) receives
*requests*. In what follows, these requests will be _evaluated_, which
transforms them to Raft _commands_. The commands in turn are
_proposed_ to the Raft consensus group and, after the Raft group
accepts the proposals and commits them, control comes back to the
replicas (all of the replicas of a range this time, not just the
lease holder), which _apply_ them.

Execution of write requests shares much of the preprocessing logic
with reads, such as concurrency control, lease acquisition or
redirection, and retries. However, it has one additional step
that is closely linked to the read path: it [consults](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L119)
the [timestamp cache](https://github.com/cockroachdb/cockroach/blob/e6e428f8029e4e90f391cb746e2339ec6fe7a581/pkg/kv/kvserver/tscache/cache.go#L53),
and [moves the transaction's timestamp forward](https://github.com/cockroachdb/cockroach/blob/e6e428f8029e4e90f391cb746e2339ec6fe7a581/pkg/kv/kvserver/replica_tscache.go#L316)
if necessary. This structure is a bounded in-memory cache from key range to the
latest timestamp at which it was read, and serves to protect against violations
of snapshot isolation -- i.e. a write of a key at a lower timestamp than a
previous read must not succeed (see the Read-Write Conflicts – Read Timestamp
Cache section in [Matt's blog
post](https://www.cockroachlabs.com/blog/serializable-lockless-distributed-isolation-cockroachdb/)).
This cache is updated during the
[read path epilogue](https://github.com/cockroachdb/cockroach/blob/31847acd14ed27a340dfc620a544c3e33cbd7c9a/pkg/kv/kvserver/replica_read.go#L124)
and corresponding [write path proposal epilogue](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L144).

At this point, we [evaluate the request and propose resulting Raft commands](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L149).
We'll describe the process below (it will be fun) but, before we do, let's see
what the current method will do afterwards. The call returns a channel that
we'll [wait on](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L207).
This is the decoupling point that we've anticipated above - the point where we
cede control to the Raft machinery. The `Replica` doing the proposals accepts
its role as merely one of many replicas and waits for the consensus group to
make progress in lock-step. The channel will receive a result when the (local)
replica has applied the respective commands, which can happen only after the
commands have been committed to the shared Raft log (a global operation).

#### Evaluation of requests and application of Raft commands

As promised, let's see what happens inside `Replica.evalAndPropose()`. The
first thing is the process of evaluation, i.e. turning a KV *request*
into a Raft *command*. This is done through the call to
[`requestToProposal()`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L89),
which quickly calls
[`evaluateProposal()`](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L917),
which in turn calls
[`evaluateWriteBatch`](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L784).
This last method simulates the execution of the request if you will, and records
all the would-be changes to the `Engine` into a "batch" (these batches are how
Pebble models transactions). This batch will be
[serialized](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L834)
into a Raft command. If we were to commit this batch now, the changes
would be live, but just on this one replica, which would be a
potential data consistency violation. Instead, we [abort
it](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L790).
It will be resurrected when the command "comes out of Raft", as we'll see.

The simulation part takes place inside the
[`evaluateWriteBatchWrapper()`](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L693)
method. This takes in the `roachpb.BatchRequest` (the KV request we've
been dealing with all along), [allocates an
`engine.Batch`](https://github.com/cockroachdb/cockroach/blob/08322ffa1b3060a2d4cb7e3ff3ee8a0b370cf1e6/pkg/kv/kvserver/replica_write.go#L702)
and delegates to
[`evaluateBatch()`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L146). This
fellow finally
[iterates](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L235)
over the individual requests in the batch and, for each one, calls
[`evaluateCommand`](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L4823). We've
seen `evaluateCommand` before, on the read path: it looks up the
corresponding command for each request's method and
[calls it](https://github.com/cockroachdb/cockroach/blob/759fdac56ec59c75aa872e8e00d0a614bcc593a3/pkg/kv/kvserver/replica_evaluate.go#L507).
One such method would be
[`batcheval.Put`](https://github.com/cockroachdb/cockroach/blob/702d8bb2d2432c35aee90a543f7b2facc7c9c65e/pkg/kv/kvserver/batcheval/cmd_put.go#L42-L44),
which writes a value for a key. Inside it we'll see a [call to the
engine](https://github.com/cockroachdb/cockroach/blob/702d8bb2d2432c35aee90a543f7b2facc7c9c65e/pkg/kv/kvserver/batcheval/cmd_put.go#L57)
to perform this write (but remember, it's all performed inside a
Pebble transaction, the `storage.Batch`).

This was all for the purposes of recording the engine changes that
need to be proposed to Raft. Let's unwind the stack to
`evalAndPropose` (the method that started this section), and see what
happens with the result of
[`requestToProposal`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L89). For
one, we grab [`proposalCh`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L107),
the channel which will be returned to the caller to wait for the proposal's
local application.

it gets [inserted
into](https://github.com/cockroachdb/cockroach/blob/33c18ad1bcdb37ed6ed428b7527148977a8c566a/pkg/storage/replica.go#L2292)
the "pending proposals map" - a structure that will make the
connection between a command being *applied* and `tryAddWriteCmd`
which will be blocked on a channel waiting for the local
application. More importantly, it gets passed to
[`Replica.propose`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L209)
which inserts it into the
[proposal buffer](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L365)
where it eventually ends up being [proposed to `raftGroup`](https://github.com/cockroachdb/cockroach/blob/8de3463817826d431bf0c6433ad3eb81bf06c9b3/pkg/kv/kvserver/replica_proposal_buf.go#L692).
This `raftGroup` is a handle to a consensus group, implemented by the [Etcd
Raft library](https://github.com/coreos/etcd/tree/master/raft), a
black box to which we submit proposals to have them serialized through majority
voting into a coherent distributed log. This library is responsible for passing
the commands in order to all the replicas for
*application*.

This concludes the discussion of the part specific to the lease holder replica:
how commands are proposed to Raft and how the lease holder is waiting for them
to be applied before returning a reply to the (KV) client. What's missing is the
discussion on how exactly they are applied.

#### Raft command application

We've seen how commands are "put into" Raft. But how do they "come
out"? The Etcd Raft library implements a distributed state machine
whose description is beyond the present scope. Suffice to say that we
have a
[`raftProcessor`](https://github.com/cockroachdb/cockroach/blob/8c5253bbccea8966fe20e3cb8cc6111712f6f509/pkg/kv/kvserver/scheduler.go#L135)
interface that state transitions from this library call to. Our older
friend the `Store` implements this interface and the important method
is
[`Store.processReady()`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/store_raft.go#L506).
This will eventually call back into a specific `Replica` (the replica of a
range that's being modified by each command), namely it will call
[`handleRaftReadyRaftMuLocked`](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L495). This
will [apply committed entries](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_raft.go#L839)
by taking in the serialized `storage.Batch` and [applying it to the
state machine](https://github.com/cockroachdb/cockroach/blob/6e4c3ae05d92f4104b7b9fbb33f9d1cecf055fd3/pkg/kv/kvserver/apply/task.go#L286)
by [committing the batch to Pebble](https://github.com/cockroachdb/cockroach/blob/6b9168de33628257f3f1fdb17af1f8205ee21d32/pkg/kv/kvserver/replica_application_state_machine.go#L894).
The command has now been applied (on one particular replica, but keep in mind
that the process described in this section happens on *every* replica).

We've glossed over something important: after applying the command, on the
replica where the client is waiting for the response (i.e. typically the lease
holder), we need to signal the channel on which the client is waiting (which, as
we saw in the previous section, is blocked in `executeWriteBatch`). This happens
[at the end](https://github.com/cockroachdb/cockroach/blob/6e4c3ae05d92f4104b7b9fbb33f9d1cecf055fd3/pkg/kv/kvserver/apply/task.go#L297)
of command application, where [`AckOutcomeAndFinish`](https://github.com/cockroachdb/cockroach/blob/6e4c3ae05d92f4104b7b9fbb33f9d1cecf055fd3/pkg/kv/kvserver/replica_application_cmd.go#L166)
is called on every command -- if the command was local, it will eventually
[signal](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L145)
a proposal result by [sending it](https://github.com/cockroachdb/cockroach/blob/e4bb550d9abae97ab09c099592f32f88ad895177/pkg/kv/kvserver/replica_proposal.go#L161)
through the channel that the proposer is waiting on.

We've now come full circle - the proposer will now be unblocked and receive a
response on the channel, and it can unwind the stack letting its client know
that the request is complete. This reply can travel through the hierarchy of
`Sender`s, back from the lease holder node to the SQL gateway node, to a SQL
tree of `planNode`s, to the SQL Executor, and, through the `pgwire`
implementation, to the SQL client.
