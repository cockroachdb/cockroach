# The SQL layer in CockroachDB

Last update: September 2018

This document provides an architectural overview of the SQL layer in
CockroachDB.  The SQL layer is responsible for providing the "SQL API"
that enables access to a CockroachDB cluster by client applications.

Original author: knz

Table of contents:

- [Prologue](#prologue)
- [Overview](#overview)
- [Detailed model - Query Processing phases](#detailed-model---query-processing-phases)
- [Detailed model - Statements vs. Components](#detailed-model---statements-vs-components)
- [Detailed model - Components and Data Flow](#detailed-model---components-and-data-flow)
- Component details:
  - [pgwire](#pgwire)
  - [SQL front-end](#sql-front-end)
  - [SQL middle-end](#sql-middle-end)
  - [SQL back-end](#sql-back-end)
  - [Executor](#executor)
  - [Auxiliaries](#the-two-big-auxiliaries)
  - [Other components](#fringe-interfaces-and-components)

## Prologue

This document *complements* the prior document "Life of a SQL query"
by Andrei. Andrei's document is structured as an itinerary, where the
reader follows the same path as a SQL query and its response data
through the architecture of CockroachDB. This architecture document is
a top-down perspective of all the components involved side-by-side,
which names and describes the relationships between them. In short,
[Life of a SQL query](life_of_a_query.md) answers the question "what
happens and how" and this document answers the question "what are the
parts involved".

## Disclaimer / How to read this document

**tl;dr: there is an architecture, but it is not yet visible in the source code.**

In most state-of-the-art software projects, there exists a relatively
good correspondence between the main conceptual items of the overall
architecture (and its diagrams, say) and the source code.

For example, if the architecture calls out a thing called e.g. "query
runner", which takes as input a logical query plan (a data structure)
and outputs result rows (another data structure), you'd usually expect
a thing in the source code called "query runner" that looks like a
class whose instances would carry the execution's internal state
providing some methods that take a logical plan as input, and returning
result rows as results.

In CockroachDB's source code, this way of thinking does not apply:
instead, **CockroachDB's architecture is an emergent property of its
source code**.

"Emergent" means that while it is possible to understand the
architecture by reading the source code, the idea of an architecture
can only emerge in the mind of the reader after an intense and
powerful mental exercise of abstraction. Without this active effort,
the code just looks like a plate of spaghetti until months of grit and
iterative navigation and tinkering stimulates the reader's
subconscious mind to run through the abstraction exercise on its own,
and to slowly and incrementally reveal the architecture, while the
reader's experience builds up.

There are multiple things that can be said about this state of affairs:

- *this situation sounds much worse than it really is.* While the code
  is initially difficult to map to an overarching architecture, every
  person who has touched the code has made their best effort at
  maintaining good separation of responsibilities between different
  components. The fact that this document is able to reconstruct a
  relatively sane architectural model from the source code *despite
  the lack of explicit overarching architectural guidelines so far* is
  a testament to the quality of said source code and the work of all
  past and current contributors.

- nevertheless, *it exerts a high resistance against the onboarding of
  new team members*, and *it constitutes an obstacle to the formation
  of truly decoupled teams*. Let me explain.

  While our "starter projects" ensure that new team members get
  quickly up to speed with our engineering *process*, they are rather
  powerless at creating any high-level understanding whatsoever of how
  CockroachDB's SQL layer really works.  My observations so far
  suggest that onboarding a contributor to CockroachDB's SQL code such
  that they can contribute non-trivial changes to any *part* of the
  SQL layer requires four to six months of incrementally complex
  assignments over *all* of the SQL layer.

  The reason for this is that (until this document was written) the
  internal components of the SQL layer were not conceptually isolated,
  so one had to work with all of them to truly understand their
  boundaries. By the time any good understanding of any single
  component could develop, the team member would have needed to look
  at and comprehend every other component. And therefore teams could
  not maintain strong conceptual isolation between areas of the source
  code, for any trainee would be working across boundaries all the
  time.

- finally, *this situation is changing, and will change further.* As
  the number of more experienced engineers grows, more of us are
  starting to consciously realize that this situation is untenable and
  that we must start to actively address complexity growth and the
  lack of internal boundaries. Me authoring this document serves as
  witness to this change of winds. Moreover, some feature work
  (e.g. concurrent execution of SQL statements) is already motivating
  some good refactorings by Nathan, and more are coming on the
  horizon. Ideally, this entire "disclaimer" section in this
  architecture document would eventually disappear.

There is probably space for a document that would outline how we
*wish* CockroachDB's SQL architecture to look like; this is left as an
exercise for a next iteration, and we will focus here on recognizing
what is there without judgement.

In short, the rest of this document is a **model**, not a specification.

Also, several sections have a note "Whom to ask for details". This
reflects the current advertised expertise of several team members, so
as to serve as a possible point of entry for questions by newcomers,
but **does not intend to denote "ownership"**: so far I know, we don't
practice "ownership" in this part of the code base.

## Overview

The flow of data in the SQL layer during query processing can be summarized as follows:

![Very high-level, somewhat inaccurate model of the interactions in the SQL layer](sql/sql-hl.png)

There are overall five main component groups:

- **pgwire**: the protocol translator between clients and the executor;

- the SQL **front-end**, responsible for parsing,
  desugaring, free simplifications and semantic analysis; this
  comprises the two blocks "Parser" and "Expression analysis" in the
  overview diagram.

- the SQL **middle-end**, responsible for logical planning and
  optimization.

- the SQL **back-end**, which comprises "physical planning" and "query
  execution".

- the **executor**, which coordinates between the previous four things,
  the session data, the state SQL transaction and the interactions
  with the state of the transaction in the KV layer.

Note that these components are a fictional model: for efficiency and
engineering reasons, the the front-end and middle-end are grouped
together in the code; meanwhile the back-end is here considered as a
single component but is effectively developed and maintained as
multiple separate sub-components.

Besides these components on the "main" data path of a common SQL
query, there are additional auxiliary components that can also participate:

- the **lease manager** for access to SQL object descriptors;
- the **schema change manager** to perform schema changes asynchronously;
- the **memory monitors** for memory accounting.

Although they are auxiliary to the main components above, only the
memory monitor is relatively simple -- a large architectural
discussion would be necessary to fully comprehend the complexity of
SQL leases and schema changes.

The [detailed model section
below](#detailed-model-components-and-data-flow) describes these
components further and where they are located in the source code.

## Detailed model - Query processing phases

It is common for SQL engines to separate processing of a query into two
phases: **preparation** and **execution**. This is especially valuable because
the work of preparation can be performed just once for multiple
executions.

In CockroachDB this separation exists, and the preparation phase is
itself split into sub-phases: **logical preparation** and **physical
preparation**.

This can be represented as follows:

![Phases of SQL query execution](sql/sql-phases.png)

This diagram reveals the following:

- There are 3 main “groups” of statements:
  1. “Read-only” queries which only use SELECT, TABLE and VALUES.
  2. DDL statements (CREATE, ALTER etc) which incur schema changes.
  3. The rest (non-DDL), including SHOW, SET and SQL mutations.

- The **logical preparation** phase contains two sub-phases:
  - **Semantic analysis and validation**, which is common to all SQL statements;
  - **Plan optimization**, which uses different optimization implementations (or none)
    depending on the statement group.

- The **physical preparation** is performed differently depending on
  the statement group.

- **Query execution** is also performed differently depending on the statement group,
  but with some shared components across statement groups.

## Detailed model - Statements vs. Components

The previous section revealed that different statements pass through
different stages in the SQL layer. This can be further illustrated in
the following diagram:

![Current component use by statement type](sql/sql-current-layers.png)

This diagram reveals the following:

- There are actually 6 statement groups currently:
  1. The “read-only” queries introduced above,
  2. DDL statements introduced above,
  3. SHOW/SET and other non-mutation SQL statements,
  4. SQL mutation statements,
  5. Bulk I/O statements in CCL code that influence the schema: IMPORT/EXPORT, BACKUP/RESTORE,
  6. Other CCL statements.

- There are 2 separate, independent and partly redundant
  implementations of semantic analysis and validation. The CCL code
  uses its own. (This is bad and ought to be changed, see below.)

- There are 3 separate, somewhat independent but redundant
  implementations of logical planning and optimizations.

  - the SQL cost-based planner and optimizer is the new “main” component.
  - the heuristic planner and optimizer was the code used before the cost-based optimizer
	was implemented, and is still used for every statement not yet
	supported by the optimizer. This code is being phased out as
	its features are being taken over by the cost-based optimizer.
  - the CCL planning code exists in a separate package and tries
	hard (and badly) to create logical plans as an add-on package.
	It interfaces with the heuristic planner via some glue code that
	was organically grown over time without any consideration for
	maintainability. So it's bad on its own and also heavily
	relies on another component (the heuristic planner) which is
	already obsolete. (This is bad; this code needs to disappear.)

- There are 2 somewhat independent but redundant execution engines for
  SQL query plans: **distributed** and **local**.

  These two are currently being merged, although CCL statements
  have no way to integrate with distributed execution currently and
  still heavily rely on local execution. (This is bad; this needs to
  change.)

- The remaining components are used adequately by the statement types
  that require them and not more.

This proliferation of components is a historical artifact of the
CockroachDB implementation strategy in 2017, and is not to remain in
the long term. The desired situation looks more like the following:

![Desired component use by statement type](sql/sql-desired-layers.png)

That is, use the same planning and execution code for all the
statement types.

## Detailed model - Components and Data Flow

Here is a more detailed version of the summary of data flow
interactions between components, introduced at the beginning:

![Architecture model of CockroachDB's SQL layer](sql/sql-overview.png)

(Right-click then "open image in new window" to zoom in and keep the
diagram open while you read the rest of this document.)

### Boundary interfaces

There are two main interfaces between the SQL layer and its "outside world":

- the **network SQL interface**, for clients connections that want to speak SQL (via pgwire);
- the **transactional KV store**, CockroachDB's next high-level abstraction layer;

I call these "main" interfaces because they are fundamentally
necessary to provide any kind of SQL functionality. Also they are
rather conceptually *narrow*: the network SQL interface is more or
less "SQL in, rows out" and the KV interface is more or less "KV ops
out, data/acks in".

In addition, there exist also a few interfaces that are a bit less visible and
emerge as a side-effect of how the current source code is organized:

- the **distSQL flows** to/from "processors" running locally and on other nodes.
  - these establish their own network streams (on top of gRPC) to/from other nodes.
  - the interface protocol is more complex: there are sub-protocols to set up
    and tear down flows; managing errors, and transferring data between processors.
  - distSQL processors do not access most of the rest of the SQL code;
    the only interactions are limited to expression evaluation (a
    conceptually very small part of the local runner) and accessing
    the KV client interface.

- **the distSQL physical planner also talks directly to the distributed storage layer**
  to get *locality information* about which nodes are leaseholders for which ranges.
  
- **the internal SQL interface**, by which other
  components of CockroachDB can use the SQL interface to access lower
  layers without having to open a pgwire connection.
  The users of the internal interface include:
  - within the SQL layer itself, the **lease manager** and the
    **schema change manager**, which are outlined below,
  - the admin RPC endpoints, used by CLI tools and the admin web UI.
  - outside of the SQL layer: metrics collector (db stats), database
    event log collector (db/table creation/deletion etc), etc

- **the memory monitor interface**; this is currently technically in
  the SQL layer but it aims to regulate memory allocations across
  client connections and the admin RPC, so it has global state
  independent of SQL and I count it as somewhat of a fringe component.

- **the event logger**: this is is where the SQL layer saves details
  about important events like when a DB or table was created, etc.

## pgwire

(This is perhaps the architectural component that is the most
recognizable as an isolated thing in the source code.)

**Roles:**

- primary: serve as a **protocol translator** between network clients
  that speak pgwire and the internal API of the SQL layer.
- secondary: **authenticate** incoming client connections.

**How:**

Overall architecture: event loop, one per connection (in separate
goroutines, `v3Conn.serve()`). Get data from network, call into
executor, put data into network when executor call returns, rinse,
repeat.

**Interfaces:**

- The network side (`v3Conn.conn` implementing `net.Conn`): gets bytes
  of pgwire protocol in from the network, sends bytes of pgwire
  protocol out to the network.

- memory monitor (`Server.connMonitor`): pre-reserves chunks of memory
  from the global SQL pool (`Server.sqlMemoryPool`), that can be
  reused for smallish SQL sessions without having to grab the global
  mutex.

- Executor: pgwire queues input SQL queries and COPY data packets to
  the "conn executor" in the `sql` package. For each input SQL query
  pgwire also prepares a "result collector" that goes into the
  queue. The executor monitors this queue, executes the incoming
  queries and delivers the results via the result collectors.  pgwire
  then translates the results to response packets towards the
  client.

Code lives in `sql/pgwire`.

**Whom to ask for details:** mattj, jordan, alfonso, nathan.

## SQL front-end

1. the "Parser" (really: lexer + parser), in charge of **syntactic
   analysis**.
2. **scalar expression semantic analysis**, including name resolution,
   constant folding, type checking and simplification.
3. **statement semantic analysis**,
   including e.g. existence tests on the target names of schema change
   statements.

Reminder: "semantic analysis" as a general term is the phase in
programming language transformers where the compiler determines if the
input *makes sense*.  The output of semantic analysis is thus
conceptually a yes/no answer to the question "does this make sense"
and the input program, optionally with some annotations.

### Syntactic analysis

**Role:** transform SQL strings into syntax trees.

**Interface:**

- SQL string in, AST (Abstract Syntax Tree) out.
- mainly `Parser.Parse()` in `sql/parser/parse.go`.

**How:**

The code is a bit spread out but quite close to what every textbook
suggests.

- `Parser.Parse()` really:
  - creates a LL(2) lexer (`Scanner` in `scan.go`)
  - invokes a go-yacc-generated LALR parser using said scanner (`sql.go`
    generated from `sql.y`)
    - go-yacc generates LALR(1) logic, but SQL really needs LALR(2)
      because of ambiguities with AS/NOT/WITH; to work around this,
      the LL(2) scanner creates LALR(1) pseudo-tokens marked with
      `_LA` based on its 2nd lookahead.
  - expects either an error or a `Statement` list from the parser,
    and returns that to its caller.

- the list of tokens recognized by the lexer is automatically
  derived from the yacc grammar (cf. `sql/parser/Makefile`)

- many AST nodes!!!
  - until now we have wanted to be able to pretty-print the AST back
    to its original SQL form or as close as possible
    - no good reason from a product perspective, it was just
      useful in tests early on so we keep trying out of tradition
    - so the parser
      doesn't desugar most things (there can be `ParenExpr` or
      `ParenSelect` nodes in the parsed AST...)
    - except it does actually desugars some things like
      `TRIM(TRAILING ...)` to `RTRIM(...)`.
  - too many nodes, really, a hell to maintain. "IR project" ongoing
    to auto-generate all this code.

- AST nodes have a slot for a type annotation, filled
  in the middle-end (below) by the type checker.

**Whom to ask for details**: pretty much anyone.

### Scalar expression semantic analysis

**Role:** check AST expressions are valid, do some preliminary
optimizations on them, provide them with types.

**Interface:**

- `Expr` AST in, `TypedExpr` AST out (actually: typed+simplified
  expression)
- via `analyzeExpr()` (`sql/analyze.go`)

**How:**

1. name resolution (in various places): replaces column names by
   `parser.IndexedVar` instances, replaces function names by
   `parser.FuncDef` references.
2. `parser.TypeCheck()`/`parser.TypeCheckAndRequire()`:
   - performs constant folding;
   - performs type inference;
   - performs type checking;
   - memoizes comparator functions on `ComparisonExpr` nodes;
   - annotates expressions and placeholders with their types.
3. `parser.NormalizeExpr()`: desugar and simplify expressions:
   - for example, `(a+1) < 3` is transformed to `a < 2`
   - for example, `-(a - b)` is transformed to `(b - a)`
   - for example, `a between c and d` is transformed to `a >= c and a <= d`
   - the name "normalize" is a bit of a misnomer, since
     there is no real normalization going on. The historical
     motivation for the name was the transform that tries hard
	 to pull everything but variable names to the right of comparisons.

The implementation of these sub-tasks is *nearly* purely
functional. The only wart is that `TypeCheck` spills the type of SQL
placeholders (`$1`, `$2` etc) onto the semantic context object passed
through the recursion in a way that is order-sensitive.

Note: it's possible to inspect the expressions without desugaring and
simplification using `EXPLAIN(EXPRS, TYPES)`.

**Whom to ask for details:** the SQL team(s).

### Statement semantic analysis

**Role:** check that SQL statements are valid.

**Interface:**

- There are no interfaces here, unfortunately. The code for statement
  semantic analysis is currently interleaved with the code to construct the
  logical query plan.
- This does use (call into) expression semantic analysis as described above.

**How:**

- *check the existence of databases or tables* for statements that
  assume their existence
  - **this interacts with the lease manager to access descriptors**
- *check permissions* for statements that require specific privileges.
- *perform expression semantic analysis* for every expression used by
  the statement.
- *check the validity of requested schema change operations* for DDL
  statements.

**Code:** in the `opt` package, also currently some code in the `sql`
package.

**Whom to ask for details:** the SQL team(s).

## SQL middle-end

Two things are involved here:

- **logical planner**: transforms the annotated AST into a
  logical plan.
- **logical plan optimizer**: makes the logical plan better.

### Logical planner

**Role:** turn the AST into a logical plan.

**Interface:** see `opt/optbuilder`.

**How:**

- in-order depth-first recursive traversal of the AST;
- invokes semantics checks on the way;
- constructs a tree of “relational expression nodes”. This tree is also called
  “the memo” because of the data structure it uses internally.
- the resulting tree *is* the logical plan.

**Whom to ask for details:** the SQL team(s).

### Logical plan optimizer

**Role:** make queries run faster.

**Interface:** see `opt`.

**Whom to ask for details:** the optimizer team.

## SQL back-end

### Physical planner and distributed execution

**Role:** plan the distribution of query execution (= decide which
computation goes to which node) and then actually run the query.

See the distSQL RFC and "Life of a SQL query" for details.

**Code:** `pkg/sql/distsql{plan,run}`

**Whom to ask for details:** the SQL execution team.

### Distributed processors

**Role:** perform individual relational operations in a currently
executing distributed plan.

**Whom to ask for details:** the SQL execution team.

## Executor

**Roles:**

- coordinate between the other components
- maintain the state of the SQL transaction
- maintain the correspondence between the SQL txn state and
  the KV txn state
- perform automatic retries of implicit transactions, or
  transactions entirely contained in a SQL string received from pgwire
- track metrics

**Interfaces:**

- from pgwire: `ExecuteStatements()`, `Prepare()`,
  `session.PreparedStatements.New()`/`Delete()`,
  `CopyData()`/`CopyDone()`/`CopyEnd()`;

- for the internal SQL interface: `QueryRow()`, `queryRows()`,
  `query()`, `exec()`;

- into the other components within the SQL layer: see the interfaces
  in the previous sections of this document;

- towards the memory monitor: to account for result set accumulated in
  memory between transaction boundaries;

**How:**

- maintains its state in the `Session` object;
- there's a monster spaghetti code state machine in `executor.go`;
- there's a monster "god class" called `planner`;
- it's a mess, and yet it works!

**Whom to ask for details:** andrei, nathan

## The two big auxiliaries

### SQL table lease manager

This thing is responsible for leasing cached descriptors to the rest of SQL.

**Interface:**

- the lease manager presents an interface to "get cached descriptors"
  for the rest of the SQL code.

**Why:**

- we don't want to retrieve the schema descriptors using KV in every
  transaction, as this would be slow, so we cache them.
- since we cache descriptors, we need a cache consistency protocol with
  other nodes to ensure that descriptors are not cached forever and
  that cached copies are not so stale as to be invalid when there are
  schema changes. The lease manager abstracts this protocol from
  the rest of the SQL code.

**How:**

It's quite complicated.

However the *state* of the lease manager is itself stored in a SQL
table `system.leases`, and thus internally the lease manager must be
able to issue SQL queries to access that table. For this, it uses the
internal SQL interface.  It's really like "SQL calling into itself".
The reason why we don't get "turtles all the way down" is that the
descriptor for `system.leases` is not itself cached.

Note that **the lease manager uses the same KV `txn` object as the
ongoing SQL session**, to ensure that newly leased descriptors are
atomically consistent with the rest of the statements in the same
transaction.

**Code:** `sql/lease.go`.

**Whom to ask for details:** vivek, dt, andrei.

### Schema change manager

This is is responsible for performing changes to the SQL schema.

**Interface:**

- "intentions" to change the schema are defined as **mutation records**
  on the various descriptors,
- once mutation records are created, client components can write the
  descriptors back to the KV store, however they also must inform
  the schema change manager that a schema change must start via
  `notifySchemaChange`.

**Why:**

Adding a column to a very large table or removing a column can be very long.
Instead of performing these operations atomically within the transaction
where they were issued, **CockroachDB runs schema changes asynchronously**.

Then asynchronously the schema change manager will process whatever
needs to be done, such as backfilling a column or populating an index,
using a sequence of separate KV transactions.

**How:**

It's quite complicated.

Unlike the lease manager, the current state of ongoing schema changes
is not stored in a SQL table (it's stored directly in the
descriptors); however the schema change manager is (soon) to maintain
an informational "job table" to provide insight to users about the
progress of schema changes, and that is a SQL table.

So like the lease manager, the schema change manager uses the internal
SQL interface, and we have another instance here of "SQL calling into
itself".  The reason why we don't get "turtles all the way down" is
that the schema change manager never issues SQL that performs
schema changes, and thus never issues requests to itself.

Also the schema change manager internally talks to the lease manager:
leases have to stay consistent with completed schema changes!

**Code:** `sql/schema_changer.go`.

**Whom to ask for details:** vivek, dt.

## Fringe interfaces and components

### Memory monitors

Memory monitors have a relatively simple role: remember how much
memory has been allocated so far and ensure that the sum of
allocations does not exceed some preset maximum.

To ensure this:

- monitors get initialized with the maximum value ("budget") they will support;
- other things register their allocations to their monitor using an "account";
- registrations can fail with an error "not enough budget";
- all allocations can be de-registered at once by calling `Close` on an account.

In addition a monitor can be "subservient" to another monitor, with
its allocations counted against both its own budget and the budget of
the monitor one level up.

**Code:** `util/mon`; more details in a comment at the start of
`util/mon/bytes_usage.go`.

**Whom to ask for details:** the SQL execution team
