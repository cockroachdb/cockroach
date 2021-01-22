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
- *Difficulty of testing.* The schema changer job resumer is monolithic, and the only way to pause at specific states is to inject global testing knob closures. This framework is difficult to use. We rely too heavily on logic tests, which only test a small fraction of possible states (and no concurrent transactions). Furthermore, much of the implementation lives in the `sql` package and is heavily entangled with it, so it's difficult to isolate components in testing. As a result, schema changes are severely under-tested.

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
- User transaction: The initial transaction in which DDL statements are planned and begin execution (used colloquially in the context of schema changes to distinguish this part of execution from the async schema change job).

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
Target{
	ElementProto: ElementProto{
		Column: &Column{
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
	},
	Direction: Target_ADD,
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

Stages are generated in order starting at the initial nodes for each target, and each successive stage is generated by taking all possible ops that don't conflict with any dependency edges. The following diagram, generated by `EXPLAIN (DDL)`, excludes the dependency edges but instead demonstrates the generated stages. (The present implementation doesn't recognize different phases, but is more than adequate for the present example. Stage 0 corresponds to the single stage of descriptor changes needed in the pre-commit phase, and all following stages occur in the async job.)

Link to Figure 2: [Output of `EXPLAIN (DDL)`](https://cockroachdb.github.io/scplan/viz.html#H4sIAAAAAAAA/+xYUW/iOBB+hl9R5fHUbokDFPYgUkqoFB1Hqy3b1ak6IZO4xWrq5GJn93qn/e8nh4SY4oDTBB2V+sjMZPx58s3MFzz8GMFweXLyb7NB48Xqh+vHlKFoTnVultj7ib3hwwXyhxpl8BGdtLRfuY3orfvU3vpsjyfj2Xh+PZ38of2ZuvXMrUvdIHODzzdfLyfOKPU0G42fUiy6IQGjZ2DaW2CsqT3/9sV5fW5nC1ZBYLc0wAsJQJAB7KkC7OcAL63Rb1fOZDK2My9olUUFdAkqI0UFgCIqYOSo7qyJY1szAVS7NKiOBFQ7A9XNQW2kI+AiR/HK08sh7LpIMSIZ1TspIqNVhMjQixAZYAuRCg5DxvJuhqNdiKNTiEPgsXV5O57O9pZiAwGD0SNiNENw/xAQRvE/aKj1tdNVzGDA4MJHJ64PKR1qQcxQpJkDFpkD5mXWJYIeN/9C3XDxaRT48TMZnDPPHJwngUmwuZkp+bHOZLrCQ/tisbeOA9vHEPiM1v6FxB/7Ps+3jmFRjLbD2EuI1OA8wGfsv6xjHcKuBIuYMxCRt7b9P7DHluuIbluIOE8ONXdaNvBMxTKEEX6GkQRRksPJUXXArhOamalpntIlDNFQIwFBGR3bNRHoZoXWIR76uySNcP6MGuNsHCGX4YDQgqe+Qz9/xrodqbyBVWrHU8qpq2fk71QpJ1TJWaKNCPoxTzk0f0ISHsUE/xXva6nvKKI4INJTC1EGbImibUa8KlwaTFkQoVGZ6ithENIqv4KFSuJa26/z0X7vtP0kTCZK4/v/aDvJrKiTxql0ETXSSqAQQTqKYocIwnHD3pMr1ybP32wQ48zMU1q2rZ1S9uKjoeZBukReEkzaZ2aevyioc2bmh9lfrm+kUd0zk39XVe1R6gbhp9/hE7I8D3mroWQj6kY4ZEFkIx8xdE1k6mNnO45K6C/H3kWG6R79NVXTX7N3q7+uSuivWdI5dvnOSSh1wSml10yppNvfzCNHfQOMDrcBUglg17cBSm5/pQ0wJm7gYfK4QXXJLijRb3uF2q2gkNTKo7Qubg+lkL6qbLi7t2y4aq3X460H7rM5HzCWzXm9xV3Vv4VUBr1FvG8RrjDwd1OrWol0nReiuiotHE8Vbp9kOejlAb98V0oQLir0ntTFpYTer6dkyR0vofv0gH3/6OrD5RBoSYvAewvIe6vPXdX/qErqsxotyU3voI89yNYzpM46iSJaiJTM+WoVBXzugLasbICTEXTr68TX0+gmXvjYPboJBLjGBxc1TyDxhb7p4h8aSV0j1al9jlVLAL4QQK8+mtpREIabRK1nY36Q9lDCXuJV+4I8WlLzBW9IFzzg362GLnXx3W+A2luhho/aQ+5ug+9uQ7q7Da6ijY7Uxde6UeNaT+5pLSgi7Mgq1PzZ/C8AAP//g35tdeYgAAA=)

Op edges are again represented by arrows with solid black lines. Each set of op edges in a stage correspond exactly to the set of ops to be executed in that stage. Targets which do not undergo any change within a stage, because they are "waiting" for dependencies to be satisfied in a later stage, have arrows with dotted lines within that stage. Note that the generated stages are compatible with each dependency edge indicated in the output of `EXPLAIN (DDL, DEPS)` (Figure 1).

Ultimately, for this example, planning produces one stage for the pre-commit phase and six stages for the post-commit phase, including a backfill for the default value. We turn our attention to the execution of these stages and their ops.

### Execution

Ops are executed via the schema change executor, which is an abstraction over the various dependencies needed to perform descriptor updates, index backfills, constraint and unique index validation, and job creation. (In the case of dropping indexes, as in the present example, making an index `absent` requires queuing a GC job.) Since each op is a complete specification of some unit of work required, the executor serves as a thin translation layer.

Different executors can be initialized with different implementations of descriptor updates, backfills, and validation, the details being mostly independent of the schema change steps themselves. Today's schema changes requiring async jobs will, in general, have two executors: one tied to the user transaction's `connExecutor` state machine, and another in the job resumer.

In the interim before transactional schema changes, a newly introduced job resumer will run the post-commit phase of the schema change. Its graph nodes (which deterministically determine the execution stages) will be stored in the job progress, which represents a significant conceptual change from the old schema change job: As with the earlier execution phases, the source of truth of the schema change's progress is no longer the descriptors themselves, but rather the nodes stored by the job. Backfill checkpointing will work similarly to the old schema changer job.

# Reference-level explanation

## Detailed design

### Schema changes in explicit transactions

This section outlines how the schema changer interacts with the `connExecutor` state machine and statement execution over the course of a transaction, in the most general case of an explicit transaction containing one or more DDL statements, possibly together with non-DDL statements/queries. This discussion is specific to the non-transactional, asynchronous schema changes of the present, but the concepts involved for the transactional schema changes of the future will not fundamentally differ.

The new schema changer has a single unified planning implementation and `planNode` for all DDL statements, with all specialization occurring in the builder itself. The present implementation, when new-style schema changes are enabled, intercepts all DDL statements during planning but falls back to the existing implementation if the statement is unsupported.

Figure 3 shows all three phases of a general schema change, with a focus on what occurs within the user transaction. We first examine the planning and execution of each DDL statement, followed by what happens when the transaction commits.

Figure 3:

![
@startuml
partition "User transaction" {
start;
-> AST node for DDL statement;
partition "Statement planning/execution" {
repeat :Build targets;
-> Initial nodes;
:Plan stages (statement execution phase);
split
-> Stages/ops;
:Execute ops;
detach;
split again
end split;
repeat while (Transaction commit?) is (No) -> Updated nodes
}
-> Nodes after all statement execution is complete;
partition "Transaction commit" {
:Plan stages (pre-commit phase);
split
-> Stages/ops;
:Execute ops;
detach;
split again
end split;
:Create job;
:Commit transaction;
}
}
-> Nodes after post-commit phase (marshaled in job record);
partition "Schema change job" {
:Plan stages (post-commit phase);
-> Stages/ops;
:Execute ops;
stop;
}
@enduml
](http://www.plantuml.com/plantuml/png/hP91Jm8n48Nl-ojUEB47mfrD51KF9eQO03zWR8VTccxRj2MQ6FwxRH5aiyQJ9-ZCz7llpNOP8lbuw7Nbqg-AoXfCTe4zeYSJIEJA19zLcMkgsGtkDbiOAnbxwx5QFIEr8lTiKliAiZbNuJGPeqmxvmyMXpFGisEAMDmVb9P8z9PZA09F9Y585v5KMRoa-rcavO3fHGmN6bn7WUkc2awhc0cRCZotBbz_B7ECSf8SINJVew2MbAdOI9HpSpRrtYdDc6v_LW1X-rx5snegcLZR6abcvsGo8qz6gsDMNkU_e7ra3z8QlzXDX4npcYCFDZQMotiQndUUPwVUFwPUFFYKcl5cN_EXuA-_VrCTn-cS3N5W1TEUVEX8iuGo6GRFmdfP3z-5wBWdY8vCMoJ78KVeklaxNuZMPPFB52gzvIy0)

The main additional state introduced is in `extraTxnState`, which stores a set of nodes representing the "current" targets and their states, accumulated from all the DDL statements issued in the transaction so far.

For each DDL statement issued, represented by the "Statement planning/execution" box in Figure 3, the stored `extraTxnState` nodes will, in general, undergo two changes: The first occurs during statement planning, when targets are added or modified by the builder to represent the schema change, and the second occurs during statement execution, when ops are actually executed to produce state changes and the nodes are updated accordingly.

(Note: Although constructing stages and ops is conceptually part of the planning of a schema change, in the present implementation it occurs during the `planNode`'s execution (i.e., in `startExec`) instead of during its planning. Since the stages and ops for a schema change are basically a pure function of the input nodes and the execution phase, it doesn't matter specifically when we construct them. For the rest of this document, we'll continue to refer to constructing stages as part of "planning" a schema change.)

The process of adding targets for an `ADD COLUMN` schema change was illustrated in the previous section. In general, an incoming DDL statement may also modify the existing set of targets in addition to adding new ones. For example, when an `ADD COLUMN` statement is issued following another earlier one in the same transaction, the preexisting targets to change the primary index must be updated to add the second new column to the new index's stored columns.

Stages are then generated from the updated targets produced by the builder, and the resulting ops are all executed within the transaction. For explicit transactions with multiple DDL statements, the distinction between the statement execution and pre-commit phases becomes relevant. As mentioned earlier, we defer some descriptor updates to the pre-commit phase to avoid writing new schema elements that may need further updates, enabled by the property that [newly added, non-public schema elements are not visible to reads and writes later in the transaction](https://github.com/cockroachdb/cockroach/blob/master/docs/RFCS/20200909_transactional_schema_changes.md#non-visible-ddls-43057). However, some schema changes do have immediately visible effects, such as `DROP COLUMN`, as well as all of today's schema changes that do not require descriptor mutations. For these schema changes, a non-empty list of stages will be generated for the statement execution phase.

Special handling is needed when the schema change operates on tables created earlier in the same transaction. Since other transactions cannot read or write to the table, there is usually no multi-version, asynchronous schema change required (with an exception being the treatment of foreign key backreferences—though note that this case is not treated correctly in the old schema changer), and it is usually possible to complete the schema change entirely within statement execution. We currently have an separate shortcut implementation for these "in-transaction" schema changes, but the new schema changer implementation will unify the in-transaction schema change path with the usual one: We'll simply generate a list of stages for the statement execution phase indicating that the schema change should be run to completion. The executor, accordingly, will use the special backfiller implementation that performs transactional KV writes to the index.

Note that targets only change as part of statement planning. After all DDL statements within the transaction have been executed, the targets become stable, and only their associated states (nodes) will change, resulting from op execution, from that point on.

Finally, when the `connExecutor` is about to commit the transaction, we construct stages and execute the ops for the pre-commit phase, with the input nodes being the nodes stored in `extraTxnState` as a result of all the DDL statements in the transaction. This is a "planning" and "execution" cycle for the schema changer, though it is not tied to the planning or execution of any particular statement. At this point, we also create the job, with the nodes at the conclusion of the pre-commit phase marshalled in the job record. After the transaction commits, the remainder of the schema change can proceed in the job.

### All about planning

#### Building targets

Schema elements include the following:

- Columns
- Indexes (primary and secondary)
- Unique constraints
- Check constraints
- Foreign key constraints
- Sequence dependencies

Recall that a target is simply an element together with a direction (either *add* or *drop*).

For any given DDL statement, the logic of building its targets will be similar to its existing execution logic (for `ALTER TABLE`, `CREATE INDEX`, etc.). The crucial difference is that instead of accumulating changes on a set of mutable descriptors which are directly read from and written to the store, we only interact with immutable descriptors while building, and instead accumulate changes to the targets themselves.

Our strategy is to reimplement all existing schema changes in parallel, only sharing code with the existing implementation if it requires minimal refactoring (which, in practice, means that there are still plenty of opportunities). This strategy presents a risk of introducing divergences between the two implementations, but our existing SQL tests (both specifically for DDL statements and otherwise) should reduce the risk.

A new problem presented by the target-based approach is that when adding new targets, the relevant preexisting schema elements on the descriptor are spread across both the descriptor and any preexisting targets from earlier DDL statements in the transaction. (We may safely assume there are no concurrent schema changes started in other transactions, as described in a later section.)

One solution is to take advantage of the duality between descriptor states and state changes, and decompose the descriptor itself into a set of completed targets: A table with a column is simply an table with an "empty" schema together with a column target in the public state. This amounts to creating an abstraction for descriptors purely in terms of their schema elements, and allows for unifying "completed" and in-progress schema elements in a much more principled way than what our current mutable descriptors and APIs provide. It also suggests a natural way to implement `CREATE` statements, by starting with an "empty" descriptor and accumulating targets generated from the statement.

#### Constructing graphs and stages

A graph, which is a directed graph of nodes, op edges, and dependency edges, is an intermediate representation used in constructing stages and their ops from a given set of input nodes. Constructing a graph is equivalent to this two-step process:

1. For each initial node, which consists of a target and an initial state, generate new nodes for all the successive states for the target and connect them with op edges.
2. For each pair of nodes (between different targets), determine whether they have a dependency, and if so, connect them with a dependency edge.

The rules which specify the successive states and op edges to generate for a given node, and the rules which specify which pairs of nodes have dependencies, are independent of any specific instance of a schema change; rather, they encode the semantics of classes of schema changes in general. In the current implementation, the rules are specified declaratively in a complicated Go data structure, in terms of predicates on nodes and pairs of nodes. Future iterations may use code generation.

A valid list of stages produced from a graph is an ordered partition of all its op edges (not its nodes!), such that all ops within the same stage have the same type (descriptor mutation, backfill, or constraint validation), and the order respects both the op edges and the dependency edges: The op edges for each target must be in order, and if node `x` depends on node `y`, then the order of ops must not cause `x` to be reached before `y`.

Stages are constructed successively and greedily: From the nodes produced as a result of the previous stage, we simply take all the op edges directed from those nodes which do not cause a dependency edge constraint to be violated. This works because once a constraint specified by a dependency edge is satisfied, it will be satisfied for all later states of the target being depended on.

#### Optimizations

In some cases, a target may be able to skip some intermediate states during a schema change while preserving correctness. Examples include:

- Columns being added with no default value or computed expression do not need to be backfilled, and can even move directly from the absent state to the write-only state, since no values are written to the column before it becomes public.
- Unique indexes being added which index the same set of columns as an existing unique index do not require validation. In particular, when replacing a primary index in the process of adding or dropping columns (not in the primary key), the new primary index does not need to be validated for uniqueness.

Both of these optimizations are equivalent to deleting some nodes from the graph, and redirecting inbound and outbound dependency edges to adjacent nodes as appropriate. More generally, an optimization can be conceived of as a transformation of the graph according to some rewrite rules. Optimization passes would occur after the basic graph is constructed, and the resulting optimized graph would still be subject to the same process of constructing stages.

In the second example, the optimization relies on public schema elements (the preexisting unique index) which are not necessarily involved in the schema change. This poses a problem if targets and nodes are limited to only the schema elements undergoing changes. One potential solution, previously discussed in the context of building targets, is to synthesize targets from all preexisting public schema elements. These targets would undergo no state changes within the schema change graph, and would effectively serve only as inputs to the optimizer.

### Reverting failed schema changes

Schema change jobs can fail during normal execution in ways that do not reflect a failure or fault in the database: Backfills can fail when evaluating expressions or writing unique indexes, and constraint validation can fail if the constraint is not valid. Canceling long-running schema change jobs (before a certain point) will also be supported. But schema changes can also run into transient failures, and we need to be defensive about bugs. In all these cases, we'll attempt to revert the schema change job.

Schema changes being reverted are planned by simply reversing the directions in each of the original targets, and then executed as normal. This will be done in `OnFailOrCancel`.

Reverting is not possible in all states of a schema change: When dropping some schema elements, such as columns, table data will become unrecoverable once the element reaches the delete-only state. When such a state is reached, the job payload will be marked as non-cancelable. If the job still fails after that point, which should be a rare occurrence since all backfills and validation will have been completed, reverting will not be attempted.

The existing schema changer has an allowlist of retryable errors which are expected to be transient. Our current plan is to forego such a list in the new implementation, and then add back allowed errors when deemed necessary. It may also separately be useful to be more explicit about expected and unexpected errors from the backfill and from constraint validation, though this will involve some work, especially on the backfill side.

### Limitations on concurrent schema changes

The new schema changer forbids multiple concurrent async schema changes which (roughly speaking) require descriptor mutations. New schema changes cannot even be "queued" in the mutations list, as with today's schema changes, until the list is empty. Queuing a mutation is equivalent to partially executing a schema change, since the elements have moved to a new non-public state, and we've decided to cut all instances of concurrent execution from the state space to make schema changes easier to reason about. This is compatible with our plans to lock descriptors in the context of transaction schema changes.

To preserve some compatibility with existing behavior, if a schema change is attempted when other schema changes on the same descriptor are running, we must poll and wait until the other schema changes are finished before committing the transaction. In general, within an explicit transaction, we'll need to poll using child transactions and then attempt to push the transaction timestamp. For implicit transactions with a single statement, it suffices to simply restart the transaction, which is similar to the process of waiting for old descriptor leases to drain.

An attempted formalization of mutual exclusion for schema changes is as follows: Within a single set of changes managed by a single schema changer, some modified descriptors are "locked" and undergo "locking" changes, while others undergo non-locking changes. Locking changes roughly correspond to schema changes requiring mutations, and are subject to mutual exclusion; non-locking changes, which are anticipated to be single-transaction changes such as adding foreign key backreferences, can occur even on locked tables. These concepts need more clarification for schema changes that do not require descriptor mutations.

### Migration plan and compatibility with old schema changer

We plan to gradually cut over to the new schema changer, enabling it for statements by default as they become supported. There will be at least one release in which new and old schema change jobs must coexist. (One reason is that although version gates provide guarantees about when jobs can start, schema change jobs from past versions can continue running past the version gate.)

Our current solution, which is relatively simple, is to have new-style schema changes wait for either new or old schema changes to finish before committing, and for old-style schema changes to simply return an error if there are ongoing new-style schema changes. There are certainly more user-friendly ways to deal with this.

Both old- and new-style schema changes can perform "non-locking" updates on tables currently undergoing the other kind of schema change. This seems fine in principle, but may deserve closer examination.

### Testing

Making it easier to write high-quality schema change tests, especially tests which cover more combinations of schema changes or more intermediate states compared to the status quo, was an explicit goal of the design.

Since targets and stages are declaratively specified and decoupled from execution, we can test the planning components separately using data-driven tests, with no need for a SQL server. The input to the tests consists of DDL statements (or lists of DDL statements grouped in the same "transaction"), and the output consists of either targets or stages.

We now also have a uniform interface, the executor, through which all side effects and state changes occur, which allows for the introduction of testing knobs which also have a uniform interface. We plan to put a better abstraction over the testing knobs in place for end-to-end tests which need to execute queries, etc., in specific states.

We also intend to reuse our existing tests, including the SQL logic tests and the randomized schema changer workload tests. The latter, in particular, is useful because it potentially allows for testing combinations of both old- and new-style schema changes running concurrently.

## Drawbacks

- With the ban on concurrent schema changes, we lose the ability to queue multiple schema changes and have the jobs continue running even after the connection is closed. This problem was first considered in the context of transactional schema changes, and there exists a [proposal for a solution in the long term](https://github.com/cockroachdb/cockroach/pull/61109). However, it remains a problem in the short term, especially if there's no UI to clarify the new behavior and the behavior is inconsistent between the new-style and old-style schema changes.
- In general, as long as new-style schema changes are on by default for some but not all schema changes, UI differences and possible minor differences in semantics will be a problem.
- There will likely be a period in which all new schema changes are implemented twice, and there is a risk of having the implementations diverge that needs to be managed.

## Rationale and alternatives

Previously we had considered the idea of hard-coding a different set of states in the schema changer simply to avoid the problem of permanently losing data after a rollback. This would not have solved the other problems listed at the beginning of this document, and was not seriously pursued once we got started with transactional schema change planning.

## Unresolved questions

- More clarity is needed on the semantics of locking vs. non-locking updates in the context of concurrent schema changes.
  - A related question is [how to deal with `DROP TABLE`](https://github.com/cockroachdb/cockroach/issues/61256), and its relative `TRUNCATE TABLE`, in the presence of ongoing schema changes. Part of the difficulty is that the state of the schema change, which would presumably need to be updated, is now stored in its job.
- Related to the new index-backfill-based column changes, and somewhat outside the scope of this RFC: [We need execution changes to support columns which are not included in the primary index](https://github.com/cockroachdb/cockroach/issues/59149). The exact changes need to be worked out.
