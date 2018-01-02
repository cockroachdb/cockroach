=======================================
 Evolving the SQL layer in CockroachDB
=======================================

:Authors: Raphael 'kena' Poss
:Status: draft
:Start Date: 2017-10-03
:RFC PR:

Summary
=======

This document outlines a path towards improving the handling of SQL
in CockroachDB.

With input from Peter Mattis, Jordan Lewis, and others.

.. sectnum::
.. contents::
   :depth: 2

Motivation
==========

The goal of this document is to *map out*, at a relatively high level,
the general directions of work that we wish to invest into
CockroachDB's SQL middle-end (mainly logical planning, semantic
analysis insofar that it supports planning) and back-end (query
execution).

The emphasis is on "mapping out": the motivation for this document is
to reveal the dependencies between work items as well as *hidden
dependencies* - significant investments that do not clearly correspond
to user-facing features.

Differences with roadmapping: so far roadmapping in CockroachDB has
been 1) flat - i.e. it doesn't track dependencies 2) oriented on
business value: roadmap items are mainly identified based on a clear
"end value" and thus can be small or large. In contrast, this
document highlights the dependencies and delineates work items
foremost so that all work items have comparable amounts of work to
them.

How to read this document
=========================

Context: `Guiding principles for the SQL middle-end and back-end in CockroachDB`__.

Take heed of the notion of *orthogonality* introduced in that tech note.

.. __: ../tech-notes/sql-principles.md

Methodology
***********

This is achieved by identifying technical *work items*: packages of
work to achieve well-defined goals by means of piecemeal evolutions of
the source code and documentation.

The main criteria to delineate a work item are as follows:

-  *effort*: the work requires more than a week and less than a month of
   dedicated work by two engineers (at least one author and one
   reviewer / work partner).
-  *goals*: the results of the work enable either:

   -  a (set of) new abstractiun(s) that accelerates how engineers think
      about a problem domain;
   -  a (set of) data structure(s) that enable a class of multiple related
      algorithms;
   -  a common reusable programming pattern;
   -  a non-trivial, preferably reusable algorithm;
   -  a shared service with an API.

The following aspects are out of scope for this particular document:

-  business motivations for the work items;
-  "who does what" (scheduling of human resources);
-  "what and when" (the precise calendar scheduling of the work);
-  most front-end matters: SQL syntax, built-in functions, operators;
-  most schema matters: structure of descriptors, schema access and
   updates;
-  most SQL/KV interface matters: how to encode values, how to access
   KV.

Characteristics
***************

Each work item is characterized by:

-  a title;
-  its goals;
-  its *dependencies*: other work items that must be completed
   beyond the MVP phase before the new work item can start;
-  its *accelerators*: other work items that, if completed
   before the new work item, will significantly speed up development
   and strengthen the results obtained for the goals.
-  optionally, its *sidecars*: side-benefits of the work items
   that can motivate the work beyond reaching the stated goals:

   -  functional changes visible to users ("features");
   -  non-functional behavior changes (mainly: performance);
   -  orthogonality improvements (changes the way engineers think about the
      engine).

Visual language
***************

Visually, we map out work items as boxes, with the dependencies and
accelerator relationships drawn as directed edges between
them. Dependencies will be drawn using a solid arrow, and accelerators
as dotted arrows.

Work items are colored transparently by default; a functional sidecar
is indicated by a green background, a non-functional behavior sidecar
by a blue background, and an orthogonality sidecar with a pink
background.

We add one star (★) next to the title if the preliminary
design phase has started already (e.g. RFC work); two stars (★★) if
the implementation has started, three stars (★★★) if the work is
sufficiently advanced so as to enable other work items that have the
item as dependency.

We add one direct hit icon (🎯) next to the title if:

-  for performance, an improvement over 10% over a key metric is
   expected or a constant/linear improvement is expected on a
   performance model / asymptotic complexity
-  for user-facing features, a blog post on the feature will likely
   become popular
-  for orthogonality projects, at least 2 people will notice an
   improvement in their work

We add two direct hit icons (🎯🎯) next to the title if:

-  for performance, an improvement over 100% over a key metric is
   expected or a quadratic/exponential improvement is expected on a
   performance model / asymptotic complexity
-  for user-facing features, the work enables a strong marketing /
   sales story
-  for orthogonality projects, most of the relevant engineering team
   will notice an improvement in their work

multiplicative effect is expected on user productivity

Request to the reader
*********************

Do's:

-  Double-check that the stated goals match the criteria stated above.
-  Double-check that the dependency and accelerator relationships make
   sense.
-  Suggest improvements.
-  Feel encouraged and empowered to create your own plan(s) of action
   using the same visual language but a different set of work items.
-  Signal your interest to work in a given general area, even if you
   disagree with particular work item definitions.
-  Suggest new work items or areas of work.

Don't:

-  Never consider this document as "final".
-  Do not attempt to match work items presented here 1-to-1 to issues
   on GitHub, items on the roadmap, high-level product features or
   marketing pillars.

Work items
==========

Plan overview
*************

.. raw:: html
   :file: items.embed.svg

Areas of work
*************

Here are the major areas of work that form clusters of related work
items in the definitions below:

-  *semantic extensions* to the SQL engine, which enables running SQL
   queries which were not supported previously.
-  *middle-end refactorings* / extensions; this supports
   e.g. further work on logical plan optimizations.
-  *back-end refactorings* / extensions; this supports
   e.g. new algorithms to execute queries.
-  *query compilation*: improving row throughput.
-  *perf. opts. for logical plan selection*: improving latency to last
   result.
-  *perf. opts. for logical planning*: improving latency to first
   result.

Some work items may conceptually belong to two or more clusters; for
the sake of readability, these are grouped visually in the cluster
where they most naturally belong.

.. contents::
   :local:

Work items for semantic extensions
**********************************

.. contents::
   :local:

Common table expressions (CTEs)
-------------------------------

.. workitem:: ctes
   :label: 🎯Common table expressions (CTEs)
   :attr: orthofunc
   :deps: scopes

Goals:

-  support the WITH syntax (not recursive)

Apply relational operator
-------------------------

.. workitem:: apply
   :label: Apply relational operator
   :attr: ortho
   :deps: scopes leafdata restart

Goals:

-  new function/operator "apply" which given a logical plan and a row of
   values,
   binds the free variables in the plan to the values and runs the plan.
-  provide infrastructure to run apply wherever present in a plan (ie.
   when not optimized away - removing apply is then done in particular
   query
   patterns during a later optimization).

General-purpose correlated subqueries (CSQs)
--------------------------------------------

.. workitem:: corr
   :label: 🎯🎯General-case correlated subqueries
   :attr: func
   :deps: apply

Goals:

-  encapsulate sub-plans with free variables with an apply operator.
-  add the corresponding logic tests

Sidecars:

-  this makes CockroachDB able to run arbitrary sub-queries.

Semi-joins
----------

.. workitem:: sjoin
   :label: Semi-joins
   :attr: ortho

Semi-join(A, B, P, X): a relational operator which, for every row in A
for which a row in B matches according to join predicate P, runs
plan/operator X with the matching rows in A and B as arguments.

Goals:

-  introduce the semi-join logical operator
-  implement a logical execution path to run this operator
   for testing (for testing; the semijoins later dissipate into
   regular joins by rewriting)

----

Query compilation
*****************

Query compilation is a class of optimizations that improves the
overall row throughput during the execution of a single query, by
paying the cost of an extra compilation step before query
execution can start.

The essence of compilation is to pre-allocate ("statically") resources
needed during execution before the execution starts. For example,
native code generation pre-allocates stack entries and machine
registers for variables. Another example is compilation of a task
schedule ("static scheduling"), where task segments are identified
upfront and a branch to the (pre-decided) next task segment is
inserted in the code at the end of each segment, bypassing the need
for any scheduling logic in a runtime system.

.. contents::
   :local:

Expression compilation
----------------------

.. workitem:: exprcomp
   :label: 🎯Expression compilation★
   :attr: orthoperf
   :deps: leafdata
   :accels: irast

Goals:

-  eliminate allocations of ``Datum``\ s during SQL expression
   evaluation
   (by preallocating intermediate temporary values and reusing them
   between evaluations).
-  reduce/eliminate the need for tree traversals for evaluation (by
   pre-scheduling the operations to perform).

Sidecars:

-  faster throughput overall
-  lower memory footprint throughout CockroachDB, by reducing
   the amount of dead Datums waiting for GC by the Go runtime

Plan compilation for local execution
------------------------------------

.. workitem:: plancomp
   :label: 🎯Plan compilation
   :attr: orthoperf
   :deps: leafdata ctxfreeplans
   :accels: exprcomp planser irplan

Goals:

-  eliminate allocations of ``[]Datum`` (by preallocating
   intermediate row buffers and reusing them during plan execution)
   (note: already done in distsql)
-  reduce/eliminate the need for dynamic dispatch in local execution
   by pre-allocating processors and linking them via shared buffers
   (note: already done in distsql)

Sidecars:

-  faster throughput overall;
-  lower memory footprint throughout CockroachDB, by reducing
   the amount of dead Datums waiting for GC by the Go runtime;

Batched row processing
----------------------

.. workitem:: batchrows
   :label: 🎯🎯Batched row processing
   :attr: orthoperf
   :accels: plancomp

Goals:

-  make every plan processor work on batches ("segments") of multiple
   rows at a time.
-  change the interface between components to support this.
-  stretch goal: if implemented after lookup joins, extend lookup joins
   to concentrate key prefixes across batches.

Sidecars:

-  faster throughput overall

Column storage for row batches
------------------------------

.. workitem:: cstore
   :label: 🎯Column storage for row batches
   :attr: perf
   :deps: batchrows

Goals:

-  store row batches by columns (i.e. all values in a column of a batch
   are contiguous in memory)
-  validate (measure) that doing so ensure that filters/renders etc
   maximize locality of access across rows

Sidecars:

-  faster throughput overall

Vectorized operations on row batches
------------------------------------

.. workitem:: vector
   :label: 🎯Vectorized operations on row batches
   :attr: perf
   :deps: cstore irast
   :accels: irplan

Goals:

-  make common filtering/projection steps use vector operations
-  validate (measure) this yields throughput improvements

Sidecars:

-  faster throughput overall

----

Middle-end refactorings
***********************

.. contents::
   :local:

SQL leaf data
-------------

.. workitem:: leafdata
   :label: SQL leaf data★
   :attr: orthoperf

Goals:

-  migrate leaf data into a value data structure passed as argument
   throughout semantic checks and evaluation
-  make IR trees immutable
-  reduce/eliminate allocations caused by tree rewrites

Sidecars:

-  more performance
-  simpler to reason about the code

Use IR codegen for ASTs
-----------------------

.. workitem:: irast
   :label: 🎯Use IR codegen for ASTs★★
   :attr: orthoperf
   :deps: leafdata

Goals:

-  define AST nodes using a higher-level, condensed definition language
-  reduce manually maintained boilerplate in Go sources
-  reduce allocation costs of parsing & other tree algorithms

Sidecars:

-  accelerates further engineering of tree algorithms
-  lower memory usage
-  probably more performance due to more efficient data storage

Use IR codegen for logical plans
--------------------------------

.. workitem:: irplan
   :label: 🎯Use IR codegen for plans
   :attr: orthoperf
   :accels: irast

Goals:

-  define logical plan nodes using a higher-level, condensed definition
   language
-  reduce manually maintained boilerplate in Go sources
-  reduce allocation costs of logical planning & other tree algorithms

Make plans stateless / context-free
-----------------------------------

.. workitem:: ctxfreeplans
   :label: Make plans stateless / context-free
   :attr: ortho

Goals:

-  remove execution-level data structures from logical plan nodes (e.g.
   make
   them live in semi-persistent state tables attached to the planner)
-  make logical plan nodes immutable after logical planning
-  make logical plan nodes reusable across different queries that use
   the same SQL text

Sidecars:

-  makes it easier to reason about the code

Logical plan specification/serialization language
-------------------------------------------------

.. workitem:: planser
   :label: Logical plan spec/ser language
   :attr: ortho
   :deps: ctxfreeplans
   :accels: irplan

Goals:

-  define and implement a mini-language to specify logical plans
   exactly, so as to
   enable bypassing of the SQL analysis / logical plans.
-  enable printing out an existing plan in this language.

Sidecars:

-  facilitates further testing and debugging of logical planner

Interface-based schema API
--------------------------

.. workitem:: scapi
   :label: Interface-based schema API
   :attr: ortho

Goals:

-  encapsulate all schema accessors behind an interface API

Introduce scoping and free vars in name resolution
--------------------------------------------------

.. workitem:: scopes
   :label: Introduce scoping and free vars in name resolution★★
   :attr: ortho
   :deps: scapi

Goals:

-  introduce name resolution environments with a data structure that
   separates
   the definition of table/col names to their uses.
-  change the ``getDataSource`` logic to use a name resolution
   environment.
-  modify all logical plan constructors to use name environments to look
   up table names
   and columns.
-  collect free column variables in the environment.
-  make ``getDataSource`` owner of the error detection for "unknown
   column names"
   (derived from "are there still free variables after name resolution")

----

Back-end refactorings
*********************

.. contents::
   :local:

Make data sources accept filters/projections
--------------------------------------------

.. workitem:: smartds
   :label: Make data sources accept filters/projections
   :attr: ortho

Goals:

-  create a general-purpose "data source" interface that can be used
   in physical plans; have this interface support an API
   to feed additional constraints or projections to the data source
-  use this interface during predicate and projection push-down in
   the existing planning code.

Push down predicates to vtables
-------------------------------

.. workitem:: vtpreds
   :label: Push down predicates to vtables
   :attr: perf
   :deps: smartds

Goals:

-  implement the data source API to accept predicates on vtables
-  use predictates to filter out rows during initial vtable population

Push down projections to vtables
--------------------------------

.. workitem:: vtprojs
   :label: Push down projections to vtables
   :attr: perf
   :deps: smartds

Goals:

-  implement the data source API to accept projections on vtables
-  use projection info to filter out columns during initial vtable
   population

Push down predicates to KV
--------------------------

.. workitem:: kvpreds
   :label: 🎯Push down predicates to KV
   :attr: perf
   :deps: smartds

Goals:

-  implement the data source API to accept predicates on KV scans
-  use predictates to filter out rows during KV scans

Push down projections to KV
---------------------------

.. workitem:: kvprojs
   :label: Push down projections to KV
   :attr: perf
   :deps: smartds

Goals:

-  implement the data source API to accept predicates on KV scans
-  use projection info to eliminate some KV lookups

Make physical plans restartable
-------------------------------

.. workitem:: restart
   :label: Make physical plans restartable
   :attr: ortho

Goals:

-  add a "restart" API to arbitrary physical plans (note: scans already
   can do it) to reuse the existing resources (e.g. distsql
   flows/processors, compiled program if relevant, etc)
-  make it possible to change key parameters (in particular add
   constraints / change spans) in-between restarts

Partial replanning infrastructure
---------------------------------

.. workitem:: gends
   :label: Partial replanning infrastructure
   :attr: ortho
   :deps: restart smartds

Goals:

-  defer span computation to the "start"/"restart" phase
   of execution
-  extend the data source API to accept new
   filters and projections during the "start"/"restart" phase of
   execution

Incremental phy planning of distributed queries
-----------------------------------------------

.. workitem:: incphy
   :label: Incremental physical planning for dist queries
   :deps: gends

See RFC on `distributed phy planning for LIMIT
<../RFCS/20170602_distsql_limit.md>`__ - although the RFC was
written with LIMIT in mind, the overall optimization is useful in
general: it ensures that a cluster is not flooded with processors
unless there is demand for the additional throughput.

Goals:

-  add a run-time API to stimulate the creation of new processors while
   a plan is running;
-  modify the phy planner to only spawn processor "on demand" for more
   rows;
-  modify the synchronizers / mergers to spawn processors on demand when
   starved of rows
-  investigate when/whether it is advantageous to select the node(s)
   closest to the gateway initially.

Sidecars:

-  faster handling of queries with LIMIT
-  possibly lower memory usage throughout the cluster

Incremental joins
-----------------

.. workitem:: incrjoin
   :label: Incremental / segmented joins
   :deps: opthints restart

(Also sometimes called "nested loops", except it also works with
hashing.)

Goals:

-  implement a new join data strategy which fetches the rows in the left
   operand in batches, and for each batch computes the hash table, then
   restarts the right operand (entirely) to filter the result rows.
-  make this algorithm parameterized by the maximum memory usage
   allowed.

Lookup joins
------------

.. workitem:: lookupjoin
   :label: 🎯🎯Lookup-based joins
   :attr: perf
   :deps: incrjoin gends
   :accels: incphy

Goals:

-  implement a new join resolution algorithm that extends the extant
   "index join" algorithm to arbitrary plan operands: for
   every (group of) row(s) on the left, transform the
   values in the rows to a WHERE constraint, propagate
   to the right operand, recompute spans, and restart the
   right operand plan to get the next "batch" of rows to
   advance the join.

Constant-space query execution
------------------------------

.. workitem:: fixedmem
   :label: Constant-space query execution
   :attr: perf
   :deps: incrjoin

Goals:

-  ensure that any query plan can either accept a memory
   budget and guarantee query completion within that budget (spilling
   to disk / using segmented processing as necessary), or refuse
   that budget during planning.

Sidecars:

-  more concurrent queries can run simultaneously, overall better Q
   throughput

----

Performance otimizations in logical planning
********************************************

.. contents::
   :local:

Enable manual override of planner decisions
-------------------------------------------

.. workitem:: opthints
   :label: 🎯Enable manual override of planner decisions
   :attr: orthofunc

a.k.a. "query hints" except they would not be hints for now and more
like constraints.

Goals:

-  design and implement a general-purpose annotation syntax to force
   particular logical planning choices
-  ensure it works to select:

   -  which join algorithm to use
   -  which sort algorithm to use

Cache query plans
-----------------

.. workitem:: cacheplans
   :label: 🎯Cache query plans
   :attr: perf
   :deps: leafdata ctxfreeplans

Goals:

-  reuse query plans across EXECUTEs
-  ensure plans are properly discarded upon schema change events
-  either measure that caching is always beneficial, or introduce a
   threshold beyond which caching and cache lookups are performed.

----

Cost-based logical plan optimizations
*************************************

.. contents::
   :local:

Collecting table statistics
---------------------------

.. workitem:: tstats
   :label: Collecting table statistics

Goals:

-  provide an API which, given an index descriptor
   and a set of indexed column IDs, returns an estimate of the
   cardinality of that tuple, or "not known"
-  actually maintain and delivers actual cardinality estimates in most
   KV tables
-  if possible, also maintain/deliver for vtables

Suggestion: review data collected by pg, see
https://www.postgresql.org/docs/9.6/static/view-pg-stats.html

Costing function for logical plans
----------------------------------

.. workitem:: cstf
   :label: Costing function for logical plans
   :attr: perf
   :deps: tstats
   :accels: fixedmem

Goals:

-  define a preliminary costing model, and document the formula
   for the existing relational operators currently implemented in
   CockroachDB.
-  define and implement a function which takes a logical
   plan as input and delivers an estimate cost based on the preliminary
   costing model.
-  use this cost in the current index selection code (instead of the
   arbitrary constants currently used).
-  investigate and if possible implement a memoization of this cost.

Sidecars:

-  probably better index selection already with the existing code,
   and thus better query performance in some cases

Plan equivalency classes
------------------------

.. workitem:: pleq
   :label: Plan equivalency classes

Goals:

-  extend the data structure(s) used to represent plans to
   use equivalency classes in the references between nodes
-  make the equivalency class data structure carry plan properties
   (needed columns, column types, etc) instead of the nodes themselves
-  define/deliver an API to manipulate nodes and add new nodes
   in a given equivalency class

Plan rewriting infrastructure
-----------------------------

.. workitem:: prewrite
   :label: Plan rewriting infrastructure
   :deps: pleq ctxfreeplans
   :accels: irplan

Goals:

-  define a software pattern which makes it possible to write
   all rewrite rules in one location
-  define a "rule application engine" which can run the rules
   and applies them and memoizes the results
-  ensure that rule application is properly traced,
   and each rule is uniquely identified for the purpose of tracing
-  define a base (small!) set of simple rules for testing

Cost-based plan selection
-------------------------

.. workitem:: cbpsel
   :label: 🎯🎯Cost-based plan selection
   :attr: perf
   :deps: cstf prewrite
   :accels: lookupjoin incrjoin

Goals:

-  define and implement a plan enumeration strategy which
   performs pruning and exercises rule rewriting to
   converge on "good plans"
-  ensure the feature is gated behind a session variable
-  measure the perf gains/losses when using this algorithm
   for a set of reference queries

Sidecars:

-  more performance overall

Feedback-directed plan selection
--------------------------------

.. workitem:: fdpsel
   :label: 🎯Feeback-directed plan selection
   :attr: perf
   :deps: cbpsel qexstats
   :accels: nbench

Goals:

-  collect measured ex stats across all instances of the same query
   structure (either in memory on each node, and/or persist to KV
   periodically). Attach a "degree of certainty" to this information
   based on how often a query was seen and the stddev of the measured
   ex stats.
-  while costing, compare measured ex stats with cost predicted by
   costing function. If they diverge, use the one with higher degree of
   certainty.
-  measure benefits.
-  ensure feature is gated behind feature flag.

Sidecars:

-  possibly higher performance for some queries, when
   the underlying data changes faster than the background table
   statistics collection process.

Collect plan execution statistics
---------------------------------

.. workitem:: qexstats
   :label: 🎯Collect per-planop ex stats
   :accels: ctxfreeplans

Goals:

-  annotate stages of a logical query plan with a (plan internal) stage
   identifier
-  propagate the stage identifiers to physical plans and distributed
   processors
-  during query execution collect row count, row throughput, memory
   usage and (for
   joins/unions/distinct) observed cardinality for each stage of a query
   plan;
   associate this data to the stage IDs

Node benchmarking
-----------------

.. workitem:: nbench
   :label: Node benchmarking

Goals:

-  measure disk I/O max throughput upon node start-up, possibly detect
   hypervisor / VM type
-  measure CPU speed upon node start-up
-  keep this data in the node metadata in gossip so all other nodes can
   observe it

Node-specific costing
---------------------

.. workitem:: ncst
   :label: Node-specific costing
   :attr: perf
   :deps: nbench cstf

Goals:

-  modify/extend the costing function during planning to take into
   account the measured
   node perf values

Sidecars:

-  better planning when using new hardware with perf characteristics
   different
   from testing clusters used at Cockroach Labs
-  better planning when using heterogeneous nodes

Eliminate CSQs in common cases
------------------------------

.. workitem:: corropt
   :label: 🎯CSQ elimination in common cases
   :attr: perf
   :deps: corr sjoin prewrite
   :accels: irplan

Goals:

-  implement and apply rules to eliminate the apply operator in plans
   in common cases

----

User-facing features
********************

.. contents::
   :local:

Show rewrite alternatives in EXPLAIN
------------------------------------

.. workitem:: expalt
   :label: Show opt alternatives in EXPLAIN
   :attr: func
   :deps: pleq

Goals:

-  (optional) modify the current rewrite code to keep old plan nodes
   alongside
   the new nodes in their equivalency class
-  show all members of each equivalency class in EXPLAIN side-by-side,
   so that the user can view which plans were considered. Probably a new
   EXPLAIN option should gate this feature.
-  deprecate EXPLAIN(NOOPTIMIZE/NOEXPAND) in favor of this new feature.

Sidecar:

-  facilitates testing and troubleshooting of further plan optimizations


Reveal table stats to users
---------------------------

.. workitem:: tstatsui
   :label: 🎯Reveal table stats to users
   :attr: func
   :deps: tstats

Goals:

-  expose table statistics either as documented system table or via
   ``crdb_internal`` accessor
-  investigate and if possible provide ``pg_stats``
   https://www.postgresql.org/docs/9.6/static/view-pg-stats.html
-  implement an admin UI hidden debug page that reveals the statis
-  put the opportunity for more UI for table stats on the radar of the
   design department

Expose query stats to users
---------------------------

.. workitem:: explstats
   :label: 🎯Expose query stats to users
   :attr: func
   :deps: qexstats

Goals:

-  ensure the stats show up in SHOW TRACE.
-  define and implement infrastructure to communicate and aggregate
   these collected ex stats back to the gateway node.
-  make ``EXPLAIN ANALYZE`` show the data after the query has run, see
   https://www.postgresql.org/docs/9.1/static/sql-explain.html
-  log/aggregate this data in a new in-memory table
   ``node_query_statistics`` modeled
   after ``node_statement_statistics``.

Expose node quality to users
----------------------------

.. workitem:: nqual
   :label: 🎯🎯Expose node quality to users
   :attr: func
   :deps: nbench

Goals:

-  modify ``node ls`` and/or admin UI to highlight nodes with slow disks
   and/or slow CPU and/or low memory
-  define a notion of "node quality" based on these metrics and show it
   to users

Allow DBAs to override plans
----------------------------

.. workitem:: dbaoverride
   :label: 🎯🎯Allow DBAs to override plans
   :attr: funcperf
   :deps: explstats planser
   :accels: cacheplans

Goals:

-  define a table that matches query structure to (predefined) logical
   plans;
-  define a mechanism for operators to "save" the logical plan for a
   query
   into that table;
-  modify the planner to check this table for new queries; if a query
   matches
   use the predefined plan instead of the regular logical planning code.

----

Schema work
***********

The items in this section are not strictly performance-related but do
have an influence on the SQL middle-end and back-end.

.. contents::
   :local:

Changing the PK of a table
--------------------------

.. workitem:: pkchange
   :label: 🎯🎯Allow changing the PK of a table
   :attr: func

Goals:

- support ALTER TABLE ... PRIMARY KEY.

This must be a combination of adding a new index and updating all the
other secondary indexes. The strategy would be to duplicate all the
secondary indexes, spawn a backfill job, and until the backfill has
completed update both the old and new PK in inserts/updates/etc.


Change FK/CHECK defaults to become like postgres
------------------------------------------------

(compatibility item)

In pg when one adds a FK or CHECK constraint, the database immediately
"activates" it by default. Clients can assume that the FK/CHECK
constraints will be active immediately without further action.

In CockroachDb currently the default is to add them initially as "NOT
VALID". The separate VALIDATE statement enables them using an async
job.

.. workitem:: constraintdefs
   :label: Change FK/CHECK defaults to become like postgres
   :attr: func	  

(Work item prio TBD depending on how serious the issue is)

Add indexes automatically for FKs
---------------------------------

(compatibility + UX item)

In CockroachDB one cannot add a FK unless there is a matching index on
the source table already. This is surprising because:

- pg does not require this otherwise.
- in a SQL database the notion of relational integrity is a functional
  concern whereas indexing is a performance concern. Users usually
  expect them to be orthogonal. Which means that if an index
  is needed for a FK, then so be it, the user doesn't need to be
  faced explicitly with this requirement.

.. workitem:: autoidxfk
   :label: 🎯Add indexes automatically for FKs
   :attr: func

Goals:

- make FK adds (either in CREATE or ALTER) not require an index on the source table

Support DEFERRABLE / INITIALLY DEFERRED
---------------------------------------

DEFERRABLE is a policy for checking FK and CHECK constraints within a
transaction. When specified in the schema, the corresponding
constraint is not enforced *at the end of the statement* but instead
*at the end of the transaction*.

This is necessary e.g. when two tables have mutual FK relationships,
in which case the integrity for two INSERT statements for the matching
tuples can only be verified after the 2nd INSERT has completed.

.. workitem:: deferrable
   :label: 🎯Support DEFERRABLE / INITIALLY DEFERRED
   :attr: func

Modify dump to break cycles
---------------------------

.. workitem:: dumpcycles
   :label: 🎯🎯Modify dump to break cycles
   :attr: func
   :deps: deferrable

Goals:

- make dump use DEFERRABLE in the SQL output as appropriate to break cycles.
- ensure that users with cycles in their schema constraints can use non-CCL dump/restore
  successfully.

