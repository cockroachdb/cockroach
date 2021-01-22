- Feature Name: Declarative, state-based schema changer
- Status: draft
- Start Date: 2021-01-15
- Authors: Andrew Werner, Lucy Zhang
- RFC PR: [#59288](https://github.com/cockroachdb/cockroach/pull/59288)
- Cockroach Issue: [#54584](https://github.com/cockroachdb/cockroach/issues/54584)

# Summary

We introduce a new framework for online schema changes which decouples the planning of DDL statements from their execution: It allows schema changes to be specified declaratively in terms of schema element states, state transitions, and state dependencies, and it generates schema change plans for transactions involving DDL statements that are independent of the details of how state transitions are executed.

This framework remedies long-standing problems with the current implementation of schema changes in CRDB, which was built around a limited, hard-coded set of state transitions and has proved insufficient for the range of DDL statements and their combinations that we support today. Its flexibility with regards to both execution context and specifying rules for planning schema changes is a prerequisite for transitioning from today's async schema change jobs to fully transactional schema changes.

# Motivation

A schema change is a sequence of state changes on a set of descriptors—or, at a more granular level, on a set of schema elements such as columns and indexes. This state-based abstraction, central to schema changes in CRDB, was adapted from Google's [F1 schema change paper](https://static.googleusercontent.com/media/research.google.com/en//pubs/archive/41376.pdf). Today, those state changes take place over multiple transactions, most of which run in an asynchronous job after the user transaction has committed. During a schema change, pairs of consecutive descriptor versions, which may be simultaneously in use in the cluster, must provide mutually compatible schemas for concurrent transactions accessing table data. It's also possible to initiate arbitrary combinations of schema changes within a transaction (with some limitations), which imposes dependencies between schema elements: For instance, new constraints cannot be validated until the columns they refer to have been backfilled.

Schema changes in CRDB have always been guided by the state-based abstraction, but their current implementation has serious deficiencies, some of which make the implementation unsuitable for the impending introduction of [transactional schema changes](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20200909_transactional_schema_changes.md). The current schema changer has hard-coded states and transitions that best accomodate a single column or index change, and is fundamentally limited in its ability to support other schema changes and their combinations. This has become untenable as the number of supported DDL statements has grown.

Problems with the schema changer which threaten the correctness and stability of schema changes, both current and future, include:

- *Data loss and metadata corruption bugs.* If columns are dropped alongside columns or indexes being added, [column data can be permanently lost even if the schema change is reverted due to failure](https://github.com/cockroachdb/cockroach/issues/46541), due to all schema elements undergoing state changes in lockstep. Sometimes [reverting is impossible in the presence of constraints and leads to descriptor corruption](https://github.com/cockroachdb/cockroach/issues/47712). These bugs are intrinsic to the hard-coded sequence of states in the schema changer.
- *Difficulty of reasoning about imperative schema change logic.* There is no consistent and statement-agnostic abstraction for states and transitions beyond direct descriptor access, and no principled separation between "planning" and "execution" (i.e., descriptor changes) during statement execution. This makes some "states" only implicitly defined, and questions like "What are the differences in state between the current and previous descriptor versions?" or even "Which descriptors changed?" hard to answer.
- *Lack of extensibility.* Check and foreign key constraints, `ALTER PRIMARY KEY`, and `ALTER COLUMN TYPE` have all been added to the schema changer through introducing implicitly defined states and special cases outside the main column and index change track, and, in the latter two cases, by chaining two runs of the schema changer together.
- *Difficulty of testing.* The schema changer job resumer is monolithic, and the only way to pause at specific states is to inject global testing knob closures. This framework is difficult to use. We rely too heavily on logic tests, which only test a small fraction of possible states (and no concurrent transactions). As a result, schema changes are severely under-tested.

Furthermore, major changes to schema changes, both in infrastructure and across many statements' specific execution logic, are necessary to support transactional schema changes.

The main infrastructural change needed is to decouple the specification of schema change states and transitions from the context in which they are executed. Schema changes should continue to run when split across the user transaction and async job, as they are today, and the schema changer should conceptually unify those execution phases. In the future, when schema changes are transactional, they should also be able to run in a series of child transactions within the user transaction. A state transition corresponding to a new descriptor version can, and should, be specified independently of actually writing an updated descriptor to disk.

Some schema changes also require reimplementation for a transactional world. Most significantly, we'll need to [use the index backfiller to rewrite the primary index for all column changes](https://github.com/cockroachdb/cockroach/issues/47989), because an unchanged primary index must remain part of the table for concurrent transactions. The reimplementation will be much easier when states and rules can be declaratively specified and modified.

# Guide-level explanation

The new schema changer separates the planning from the execution of schema changes, somewhat analogously to the planning and execution of queries. Planning involves generating, from the DDL statements' AST nodes, an intermediate representation of all schema changes to be performed, and computing dependencies and generating declaratively specified operations ("ops") to be executed.

Ops can be descriptor updates, backfills, or constraint validation queries. Different specific implementations of the execution of these ops are used depending on context: For instance, we currently have a special "backfiller" which builds indexes on tables created earlier in the same transaction, and the new schema changer will allow us to unify its interface with that of the real index backfiller, plumbing in one or the other as appropriate. (This flexibility will also be useful in the future when implementing transactional schema changes.)

A single instance of a schema changer is responsible for all the schema changes initiated in a transaction, and is tied to the `connExecutor` state machine: Before the transaction commits, it will both incrementally accumulate state as part of planning and execute descriptor changes as necessary for each statement, though the details vary depending on whether the schema change is executing mostly in an async job or within the user transaction itself. In this RFC, most examples will focus on the former case, where most of the work of execution occurs after the transaction in a separate job.

## Terminology

Note: Throughout the RFC, we use "schema" as in "table schema" and "schema change", not as in "user-defined schema."

- Element: A component of a schema (e.g., column, index, constraint, enum value) that may be added, dropped, or modified as part of a schema change.
- Target: An element and a direction (add or drop), which together specify the goal of a schema change (e.g., adding a column).
- State: Associated with a target at a specific point in the schema change. Includes states corresponding to the existing `DescriptorMutation` states, like `delete-only` and `write-only`, as well as newly formalized ones like `backfilled`.
- Node: A target annotated with a state, representing the target at a specific point in the schema change.
- Graph: Directed graph of nodes. Represents changes to be run by a schema changer. The edges of a graph are either op edges or dependency edges.
    - Op edge: An edge connecting nodes for the same target, indicating the sequence of states the target goes through during the schema change. If column `a` is being added, there is an op edge from (`a`, `delete-only`) to (`a`, `write-only`).
    - Dependency edge: An edge connecting nodes for different targets, representing a logical dependency between certain states on each target. If check constraint `c` is being added on a new column `a`, there is a dep edge from (`c`, `validated`) to (`a`, `backfilled`).
- Op: A specification of an operation to be executed. Can be a descriptor update, backfill, or constraint validation query. Each op corresponds to an op edge, and vice versa, because an op is exactly a specification of how to transition from one node to another. For the (`a`, `delete-only`) to (`a`, `write-only`) example above, the op would be a descriptor update.
- Phase: Context in which the execution of an op occurs. For today's schema changes, the possible phases are:
    - statement execution
    - pre-commit (of the user transaction)
    - post-commit (of the user transaction; that is, in the async schema change job)
- Stage: A set of ops, together with their corresponding initial and final nodes, to be executed "together." All ops within a stage must have the same type. For instance, a state may be made up of descriptor updates, and the updates should be issued in the same transaction during execution. The result of planning (for a specific phase) is a partition of all the ops in a graph into a sequence of stages, such that the stages' ordering is compatible with the op edges and dependency edges of the graph. After executing the final stage, all targets will have reached their final states.

## Example

We'll consider the `ALTER TABLE` statement that follows:

```sql
CREATE TABLE t (a INT PRIMARY KEY);
ALTER TABLE t ADD COLUMN b INT DEFAULT 0;
```

### Planning: Building targets

We'll assume the column is added by [writing its data to an entirely new primary index and then swapping the old and new primary indexes](https://github.com/cockroachdb/cockroach/issues/47989), akin to the implementation of `ALTER PRIMARY KEY`. Therefore, three targets will be needed in all: One to add the column, one to add the new primary index (which behaves like a secondary index while still being added and in a non-public state), and one to drop the old primary index.

During planning for the `ALTER TABLE` statement, the targets are built from the AST node and an immutable (read-only) instance of the table descriptor. Each element in each target contains the metadata for the relevant column or index to be added or dropped, in the form of an embedded `sqlbase.ColumnDescriptor` or `sqlbase.IndexDescriptor`, along with other relevant metadata for the schema change.

The following is a simplified definition of the `Column` element. (Elements are defined as protobuf messages so they can be serialized in the schema change job record.)

```proto
message Column {
  uint32 table_id = 1;
  uint32 family_id = 2;
  string family_name = 3;
  cockroach.sql.sqlbase.ColumnDescriptor column = 4;
}
```

The index elements and targets are defined similarly. For the schema change above, the generated "add column" target is as follows:

```go
scpb.Node{
	Target: &scpb.Column{
		TableID:    52,
		FamilyID:   0,
		FamilyName: "primary",
		Column:     descpb.ColumnDescriptor{
			ID:       2,
			Name:     "b",
			Nullable: true,
			Type:     <type information>,
		},
	},
	State: scpb.Target_ADD,
}
```

### Planning: Constructing stages

The execution of the schema change is split across multiple phases: statement execution, pre-commit, and post-commit (i.e., in the schema change job). The goal of planning is to produce, for each execution phase, a sequence of stages to be executed in order. Each stage contains a set of ops to be executed. Roughly speaking, the states of the targets at the conclusion of a phase serve as the starting point for the following phase.

For our targets, the new column and new primary index begin in the `absent` state, and the old primary index begins in the `public` state. Recall that in the old schema changer, a column being added would move to `delete-only` during statement execution. As part of the reimplementation of `ADD COLUMN`, we will (somewhat arbitrarily) defer that step to the pre-commit phase in the user transaction. Therefore, in this example, the statement execution phase is a no-op; only in the pre-commit phase do the new column and new primary index move to the `delete-only` state, at which point the updated descriptor is committed, and the table is ready to undergo the remainder of the schema change in the async job. (This distinction between the statement execution and post-commit phases is unimportant for an implicit transaction, but becomes significant for transactions with multiple DDL statements, where the visibility of earlier schema changes matters.)

The post-commit phase involves the most complex planning. The following diagram (generated by `EXPLAIN (DDL, DEPS)`, a feature of the new schema changer), demonstrates the graph of nodes and their op edges and dependency edges:

Figure 1: [Output of `EXPLAIN (DDL, DEPS)`](https://cockroachdb.github.io/scplan/viz.html#H4sIAAAAAAAA/+xYUU/jOBB+bn4FyuMJlqal0HJtpNAUKbpeQUuX1QmdKjc21MI4OdvZXe7Efz8laROXmNSh6S5IPGZmPB6PZ775HIjvGAgXe3v/GQ0ezdMPn0RcIDbjVixWyFuJvEHAHJGBKQC7Q4Kbv8cy2r65Dajg+F80MHvmfmrT7wswJ2jPJ4DzgRlEAjHT7gtm9wVcSRcIwFj8G/fD+adhQKIH2j8U0O4fJoaJsb3uKfnIPNm+tGiTLUS3ICJi9CNk2QLr9PTUm0y7xW0xzIzaRS0FDyjT+wp9REi8e2YjWISKZuIxRHrB34IHTB4zW4+Kc0ki+wykyFvNov47hmKRWRwfSRaHyaZ2qWQtnomchpDhB8AUESU+vDyqTrtsB2MlMux9vgAhGpg0oMj8Oy23o5rK7TKN1qMQ/ahYdDhfo1efLmbIFzig/IVV3wDJ1zhXQ50bSF17UMunpe8xvlMtn0DHp1yMG9qIou+zZQ3N7pGijiKK/4k2tdQ3xDgOqHLXF6MMxAKxYkU8S9zSmIuAoWGV7JfWu8Kt9hX4Oo5rbb/OR/u90/ZTVDLVgu9f0XYKrKizjI1G48nI+AxEYUpm6PHNUtQ8dc6uRpNpak+7udwdjUfT0cyZuLOvn73paHYxGf+1NDspmEnKXq68/HI29oZLudVcKaz1Ta12rjhzhn+ce+PxyF0pW7myLCTLKtjJ2k6uXQ/qKFdcO2PPdab51lmWWs8C7uaK0phOCnaytpdr12I6PrDpydbww/0g/PQnuEcOhAimeOsi7jMcioC5iCCBLqiKWJUizbACEXUrEVHPLSOikw1EdKJHRKfvloieVyCi0wRC3OoQskSBA5v26ivA57V3Gc0JVlyhRt2V10gNx7ZaN35AAjYwGYIrtDuwafen9KND4VeGt+jLneanF+enU8yP1YwVVs0ZSubkq2HK0+dOw91xp9Wl1MedKvJmLe40on4AMb1bw0YFi5IqR0FbJpWeOFfS20IvPVpvi6tdvS2+6HDD69dwwy2b0mrHzbf9z4Kk+dJTJr1zDQiGQGTHqdZ55aUiM2HJUlFy2yanFSdn+x93SXKScM+Af3+LCak/Kdse1Uqn185AeIvZ9BNO34lP31XMpqN0aNWbFrmAX8VmPsaT/niqc+y8XRhPyGeNrz6XBWFYC4vaLUJ3XyKVMeu2jutLSHIWZ84RFW8vC70Uvuq+fRmn6kHxD8zaFaVWaPUe+28V04zGk9EwGjQmqNmfNMd1zX0uHgkamBDwxbLfaTKmm5uskimf/TZzP19cqsyMJ+P/AAAA//83ASrl/h0AAA==)

Targets (elements and their add/drop directions) are shown at the top. The op edges for each target are indicated by black arrows, and the dependency edges between targets by red ones. The dependency rules that apply to this example are as follows (stated informally):

- If an index being added contains a column being added, the index cannot become `write-only` until the column does.
- If an index being added contains a column being added, the index cannot become `public` until the column does.
- If a new primary index replaces an old one, the new one must become `public` simultaneously with the old one becoming `write-only`.

The dependency edges determine the partitioning of ops into stages. A stage is defined as follows, and is both a transition between sets of `Node`s and a specification of ops to execute to effect that transition:

```go
type Stage struct {
	Before, After []*scpb.Node
	Ops           scop.Ops
}
```

(Note that some targets may undergo no change in a stage, in which case their corresponding nodes in `Before` and `After` will be equal.)

Stages are generated in order starting at the initial nodes for each target, and each successive stage is generated by taking all possible ops that don't conflict with any dependency edges. The following diagram (generated by `EXPLAIN (DDL)`) demonstrates the generated stages. (The present implementation doesn't recognize different phases, but is more than adequate for the present example. Stage 0 corresponds to the single stage of descriptor changes needed in the pre-commit phase, and all following stages occur in the async job.)

Link to Figure 2: [Output of `EXPLAIN (DDL)`](https://cockroachdb.github.io/scplan/viz.html#H4sIAAAAAAAA/+xYUW/iOBB+hl9R5fHUbokDFPYgUkqoFB1Hqy3b1ak6IZO4xWrq5GJn93qn/e8nh4SY4oDTBB2V+sjMZPx58s3MFzz8GMFweXLyb7NB48Xqh+vHlKFoTnVultj7ib3hwwXyhxpl8BGdtLRfuY3orfvU3vpsjyfj2Xh+PZ38of2ZuvXMrUvdIHODzzdfLyfOKPU0G42fUiy6IQGjZ2DaW2CsqT3/9sV5fW5nC1ZBYLc0wAsJQJAB7KkC7OcAL63Rb1fOZDK2My9olUUFdAkqI0UFgCIqYOSo7qyJY1szAVS7NKiOBFQ7A9XNQW2kI+AiR/HK08sh7LpIMSIZ1TspIqNVhMjQixAZYAuRCg5DxvJuhqNdiKNTiEPgsXV5O57O9pZiAwGD0SNiNENw/xAQRvE/aKj1tdNVzGDA4MJHJ64PKR1qQcxQpJkDFpkD5mXWJYIeN/9C3XDxaRT48TMZnDPPHJwngUmwuZkp+bHOZLrCQ/tisbeOA9vHEPiM1v6FxB/7Ps+3jmFRjLbD2EuI1OA8wGfsv6xjHcKuBIuYMxCRt7b9P7DHluuIbluIOE8ONXdaNvBMxTKEEX6GkQRRksPJUXXArhOamalpntIlDNFQIwFBGR3bNRHoZoXWIR76uySNcP6MGuNsHCGX4YDQgqe+Qz9/xrodqbyBVWrHU8qpq2fk71QpJ1TJWaKNCPoxTzk0f0ISHsUE/xXva6nvKKI4INJTC1EGbImibUa8KlwaTFkQoVGZ6ithENIqv4KFSuJa26/z0X7vtP0kTCZK4/v/aDvJrKiTxql0ETXSSqAQQTqKYocIwnHD3pMr1ybP32wQ48zMU1q2rZ1S9uKjoeZBukReEkzaZ2aevyioc2bmh9lfrm+kUd0zk39XVe1R6gbhp9/hE7I8D3mroWQj6kY4ZEFkIx8xdE1k6mNnO45K6C/H3kWG6R79NVXTX7N3q7+uSuivWdI5dvnOSSh1wSml10yppNvfzCNHfQOMDrcBUglg17cBSm5/pQ0wJm7gYfK4QXXJLijRb3uF2q2gkNTKo7Qubg+lkL6qbLi7t2y4aq3X460H7rM5HzCWzXm9xV3Vv4VUBr1FvG8RrjDwd1OrWol0nReiuiotHE8Vbp9kOejlAb98V0oQLir0ntTFpYTer6dkyR0vofv0gH3/6OrD5RBoSYvAewvIe6vPXdX/qErqsxotyU3voI89yNYzpM46iSJaiJTM+WoVBXzugLasbICTEXTr68TX0+gmXvjYPboJBLjGBxc1TyDxhb7p4h8aSV0j1al9jlVLAL4QQK8+mtpREIabRK1nY36Q9lDCXuJV+4I8WlLzBW9IFzzg362GLnXx3W+A2luhho/aQ+5ug+9uQ7q7Da6ijY7Uxde6UeNaT+5pLSgi7Mgq1PzZ/C8AAP//g35tdeYgAAA=)

Op edges are again represented by arrows with solid black lines. Each set of op edges in a stage correspond exactly to the set of ops to be executed in that stage. Targets which do not undergo any change within a stage, because they are "waiting" for dependencies to be satisfied in a later stage, have arrows with dotted lines within that stage. Note that the generated stages are compatible with each dependency edge indicated in the output of `EXPLAIN (DDL, DEPS)` (Figure 1).

Ultimately, for this example, planning produces one stage for the pre-commit phase and six stages for the post-commit phase, including a backfill for the default value. We turn our attention to the execution of these stages and their ops.

### Execution

Ops are executed via the schema change executor, which is an abstraction over the various dependencies needed to perform descriptor updates, index backfills, and constraint and unique index validation. Since each op is a complete specification of some unit of work required, the executor serves as a thin translation layer.

Different executors can be initialized with different implementations of descriptor updates, backfills, and validation, the details being mostly independent of the schema change steps themselves. Today's schema changes requiring async jobs will, in general, have two executors: one tied to the user transaction's `connExecutor` state machine, and another in the job resumer. (As previously mentioned, the in-transaction executor will have a separate backfill implementation for tables created earlier in the transaction. Schema changes on such tables are also planned differently.) 

In the interim before transactional schema changes, a newly introduced job resumer will run the post-commit phase of the schema change. Its graph nodes (which deterministically determine the execution stages) will be stored in the job details. Backfill checkpointing will work similarly to the old schema changer job.

# Reference-level explanation

To cover:

- More detailed description of interactions between `connExecutor` state machine, target building, and statement execution for explicit transaction with multiple DDL statements; what state is stored in `extraTxnState`
- Declarative specification of dependency rules
- Reverting schema changes, running them in the the job `OnFailOrCancel` hook
- More precise definition of stages; description and proof of correctness for greedy algorithm that partitions ops into stages
- Locking vs. non-locking descriptor updates
  - E.g., foreign keys and backreferences
- New invariant that schema changes cannot be queued (in the mutations list) until previous ones finish
  - Polling and waiting enabled by child transactions
- Backward and forward compatibility with the old schema changer
- Testing

## Detailed design

## Drawbacks

## Rationale and Alternatives

## Unresolved questions
