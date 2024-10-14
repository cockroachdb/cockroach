// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemachanger

// The schemachanger package and its descendents contain the declarative schema
// changer. This machinery is the latest iteration of how CockroachDB performs
// schema changes, in contrast with the so-called legacy schema changer. This
// doc file provides a high-level overview of its architecture, the aim being to
// provide a useful introduction to the code and the design decisions behind
// it.
//
// # Similarities and differences with the legacy schema changer
//
// The main challenge that any schema changer in CockroachDB must solve is to
// perform these correctly while maintaining the "online" property: tables must
// remain accessible to concurrent queries while they undergo schema changes.
// Online schema changes are enabled by upholding the 2-version invariant, see
// docs/RFCS/20151014_online_schema_change.md for details. This necessarily
// implies that schema changes may require multiple transactions to complete.
// Upholding correctness requires maintaining:
//  - atomicity: when executing multiple DDL statements in one transaction,
//    either all of them succeed or all of them fail;
//  - isolation: any side effects of an ongoing schema change must
//    remain invisible to concurrent queries until the schema change completes
//    successfully.
// This implies that schema changes which fail must be rolled back somehow.
// Schema changes may legitimately fail, typically when adding a constraint on a
// nonempty table. However, we want these rollbacks to always succeed.
//
// In CockroachDB, both in the legacy and in the declarative schema changer, for
// schema changes involving more than one transaction. this sequence of
// transactions is driven by a job; of type SCHEMA_CHANGE or
// TYPEDESC_SCHEMA_CHANGE for the legacy schema changer, NEW_SCHEMA_CHANGE for
// the declarative schema changer. In both cases the schema change gets rolled
// back when the job's `Resume` method encounters an error, after which its
// `OnFailOrCancel` method takes over and eventually transitions the job to the
// `failed` terminal state. Both schema changers also have in common the fact
// that they store the current state and targeted end-state of an ongoing schema
// change inside the affected descriptor, but here the similarities end: for the
// legacy schema changer, this state is encoded in the descriptor protobuf's
// `mutations` slice; for the declarative schema changer, it's
// `declarative_schema_changer_state`.
//
// Due to its design the legacy schema changer is unable to deal with certain
// situations correctly. Consider for example:
//
//    CREATE TABLE t (a INT, b INT);
//    INSERT INTO t VALUES (1, 1), (2, 1);
//    BEGIN;
//    ALTER TABLE t DROP COLUMN a;
//    CREATE UNIQUE INDEX ON t(b);
//    COMMIT;
//
// The legacy schema changer will correctly raise a constraint violation error
// (code 23505) however will be unable to roll back the schema change
// correctly:
//
//    > SELECT * FROM t;
//      b |  a
//    ----+-------
//      1 | NULL
//      1 | NULL
//    (2 rows)
//
// The declarative schema changer, on the other hand, does so:
//
//    > SELECT * FROM t;
//      a | b
//    ----+----
//      1 | 1
//      2 | 1
//    (2 rows)
//
// # The element model
//
// The declarative schema changer models its persisted state in terms of
// elements, which can be thought of as "things" on which a schema change can be
// performed, typically leaves of a DDL statement's AST: e.g. a table's name is
// modeled as an element called TableName, a column's default value expression
// is modelled by ColumnDefaultExpression, and so forth.
//
// A schema change is defined by setting target statuses to a set of
//elements.
// These statuses are PUBLIC, ABSENT or TRANSIENT_ABSENT. This is best described
// by example; `ALTER TABLE foo RENAME COLUMN x TO y`, assuming `foo` has
// descriptor ID 123 and `x` has column ID 4, gets translated into:
//  - ColumnName{DescID: 123, ColumnID: 4, Name: x} targets ABSENT,
//  - ColumnName{DescID: 123, ColumnID: 4, Name: y} targets PUBLIC.
//
// When the schema change starts, those two elements are respectively in the
// PUBLIC and ABSENT statuses. Performing the schema change means transitioning
// these to their target status. This may include intermediate statuses, this
// isn't the case for ColumnName but a Column transitions through DELETE_ONLY,
// WRITE_ONLY, etc. because of the need to uphold the 2-version invariant.
// Elements and statuses are defined in `scpb`. The mapping of the descriptor
// model (i.e. what's in system.descriptor) to the element model (which is what
// the declarative schema changer understands) is defined and implemented in
// `scdecomp`. The mapping of DDL ASTs to elements and targets is defined and
// implemented in `scbuild/internal/scbuildstmt`.
//
// # Status transition operations
//
// Status changes are effected by performing operations, which can be grouped
// into three types, defined in `scop`:
//  - MutationType is the most common and consists of schema metadata operations
//    which mutate the contents of the system.descriptor, system.namespace,
//    system.zone, etc. tables. To follow on the previous example, transitioning
//    ColumnName from ABSENT to PUBLIC will involve executing the SetColumnName
//    operation, which will use the catalog API to mutate the table descriptor
//    by overwriting the `name` field in the corresponding column descriptor.
//  - BackfillType involves backfilling and merging indexes.
//  - ValidationType involves validating constraints.
//
// These operations are defined in `scop` and are implemented in `scexec/...`.
// The mapping of element status transitions to operations is defined in
// `scplan/internal/opgen`.
//
// # Planning a schema change
//
// As mentioned earlier, we need to uphold the 2-version invariant, which means
// that some elements cannot transition from ABSENT to PUBLIC or vice-versa in
// one transaction. For example, a CREATE INDEX statement will result in a
// SecondaryIndex element targeting PUBLIC. Its initial status will be ABSENT
// (the index doesn't exist yet, after all) but it will reach PUBLIC by
// transitioning through DELETE_ONLY and WRITE_ONLY among many other statuses in
// distinct transactions.
//
// We model these status transitions as a directed acyclical graph, in which
// each node is an (element, target status, current status) tuple and status
// transitions are edges between nodes sharing the same (element, target status)
// pairs. We denote these as op-edges because these edges are decorated with the
// sequence of operations to effect the status transition. Constraints on status
// transitions, such as the 2-version invariant, are modeled as dependency
// edges, or dep-edges, in the graph. These edges are decorated with constraints
// which need to be satisfied. In the case of the 2-version invariant, the
// constraint is `PreviousTransactionPrecedence`, which imposes that the status
// transitions must reach the origin node before the destination node and more
// importantly must do so in separate transactions.
//
// Planning a schema change therefore boils down to generating the corresponding
// DAG and identifying a graph partition which satisfies all dep-edge
// constraints. The graph object itself is defined in `scplan/internal/scgraph`,
// the nodes and the op-edges are generated using `scplan/internal/opgen`, the
// dep-edges between those nodes are generated using
// `scplan/internal/rules/...`, and the partitioning is implemented in
// `scplan/internal/scstage`. Unsurprisingly, the `scplan` package ties all of
// this together.
//
// # Rules
//
// Generating these dep-edges is non-trivial and, practically speaking, requires
// a vast quantity of complex code. This deserves further commentary.
//
// A graph will typically have a lot of dep-edges. Think for instance of a DROP
// DATABASE statement operating on a database with a good many tables in it,
// each table will easily have a few dozen elements and in such a DROP these
// will all have target ABSENT. There will be dep-edges to ensure that, for
// instance, a table name is not removed from system.namespace before the table
// descriptor's `state` field is set to `DROP`.
//
// These edges need to be generated programmatically, and rather than doing so
// in an imperative fashion, we have chosen to define them declaratively, in the
// form of rules. These rules are essentially predicates which can be applied to
// any pair of nodes to determine whether there should be a dep-edge between
// those to. See `scplan/internal/rules/current` for the latest set of rules.
//
// The advantage of this approach is that the rules can be expressed
// using a high level of abstraction, making them easier to reason about and
// therefore less likely to have bugs. The disadvantage is that we needed to
// build what's essentially a mini-embedded RDBMS to apply these predicates
// efficiently, from a performance perspective. This database is implemented in
// `rel`.
//
// This is the reason why the element model is relational in nature. Each
// element has a set of attributes which uniquely identify it (defined in
// `screl`) and serve as a key. Elements can be joined on on other elements
// containing suitable references. For example, a ColumnName element can be
// joined with its corresponding Column element on `(DescID, ColumnID)` and with
// its Table element on `DescID`.
//
// These declarative rule definitions are at the heart of what makes this schema
// changer implementation different from the previous one, which is why we call
// it the _declarative_ schema changer.
//
// # Remaining packages of note and testing
//
// The previous sections have tried to give an overview of the important parts
// of the declarative schema changer. There remains a few things to be said
// about how it integrates with the rest of CockroachDB and how it's tested.
//
// Due to its largely self-contained nature and the need to thoroughly test each
// of its components we have relied heavily on dependency injection to enforce a
// clear interface between the declarative schema changer internals, which
// reason in terms of elements, and the rest of the codebase, which reasons in
// terms of descriptors, assumes access to the sql.planner, etc.
//
// The actual dependencies are quarantined in the `scdeps` package while the
// testing dependencies are in `scdeps/sctestdeps`. Both implement the same
// interfaces.
//
// The plumbing with the job machinery is encapsulated in `scjob` while the
// plumbing with the backup restore logic is in `scbackup`; `scrun ` essentially
// glues everything together to provide entry points into the declarative schema
// changer from outside.
//
// The `schemachanger_test` package mainly contains integration tests which
// perform data-driven end-to-end testing of the declarative schema changer. See
// also `schemachangerccl_test` which is the same thing but quarantined in
// `pkg/ccl`, and `sctest/...` for common definitions. These integration tests
// operate on the following basis. After some initial setup, one or more DDL
// statements are provided. These are fed into the declarative schema changer
// and each test verifies its behavior with respect to a specific thing:
//  - TestEndToEndSideEffects verifies that the side effects are as
//    expected, it does so by injecting test dependencies in the whole
//    framework; this also checks EXPLAIN (DDL) diagrams;
//  - TestRollback tests schema changer behavior with respect to errors thrown
//    at any point during the schema change where one may legitimately be
//    expected;
//  - TestBackup tests behavior after restoring a backup performed at any point
//    during the ongoing schema change;
//  - TestPause tests behavior after pausing the schema change job;
//  - and so forth...
//
// Finally `corpus` is used to test declarative schema changer behavior in
// mixed-version clusters.
