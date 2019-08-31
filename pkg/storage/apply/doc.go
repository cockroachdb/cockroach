// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

/*
Package apply provides abstractions and routines associated with the application
of committed raft entries to a replicated state machine.

State Machine Replication

Raft entry application is the process of taking entries that have been committed
to a raft group's "raft log" through raft consensus and using them to drive the
state machines of each member of the raft group (i.e. each replica). Committed
entries are decoded into commands in the same order that they are arranged in
the raft log (i.e. in order of increasing log index). This ordering of decoded
commands is then treated as the input to state transitions on each replica.

The key to this general approach, known as "state machine replication", is that
all state transitions are fully deterministic given only the current state of
the machine and the command to apply as input. This ensures that if each
instance is driven from the same consistent shared log (same entries, same
order), they will all stay in sync. In other words, if we ensure that all
replicas start as identical copies of each other and we ensure that all replicas
perform the same state transitions, in the same order, deterministically, then
through induction we know that all replicas will remain identical copies of each
other when compared at the same log index.

This poses a problem for replicas that fail for any reason to apply an entry. If
the failure wasn't deterministic across all replicas then they can't carry on
applying entries, as their state may have diverged from their peers. The only
reasonable recourse is to signal that the replica has become corrupted. This
demonstrates why it is necessary to separate deterministic command failures from
non-deterministic state transition failures. The former, which we call "command
rejection" is permissible as long as all replicas come to the same decision to
reject the command and handle the rejection in the same way (e.g. decide not to
make any state transition). The latter, on the other hand, it not permissible,
and is typically handled by crashing the node.

Performance Concerns

The state machine replication approach also poses complications that affect
performance.

A first challenge falls out from the requirement that all replicated commands be
sequentially applied on each replica to enforce determinism. This requirement
must hold even as the concurrency of the systems processing requests and driving
replication grows. If this concurrency imbalance becomes so great that the
sequential processing of updates to the replicated state machine can no longer
keep up with the concurrent processing feeding inputs into the replicated state
machine, replication itself becomes a throughput bottleneck for the system,
manifesting as replication lag. This problem, sometimes referred to as the
"parallelism gap", is fundamentally due to the loss of context on the
interaction between commands after replication and a resulting inability to
determine whether concurrent application of commands would be possible without
compromising determinism. Put another way, above the level of state machine
replication, it is easy to determine which commands conflict with one another,
and those that do not conflict can be run concurrently. However, below the level
of replication, it is unclear which commands conflict, so to ensure determinism
during state machine transitions, no concurrency is possible.

Although it makes no attempt to explicitly introduce concurrency into command
application, this package does attempt to improve replication throughput and
reduce this parallelism gap through the use of batching. A notion of command
triviality is exposed to clients of this package, and those commands that are
trivial are considered able to have their application batched with other
adjacent trivial commands. This batching, while still preserving a strict
ordering of commands, allows multiple commands to achieve some concurrency in
their interaction with the state machine. For instance, writes to a storage
engine from different commands are able to be batched together using this
interface. For more, see Batch.

A second challenge arising from the technique of state machine replication is
its interaction with client responses and acknowledgment. We saw before that a
command is guaranteed to eventually apply if its corresponding raft entry is
committed in the raft log - individual replicas have no other choice but to
apply it. However, depending on the replicated state, the fact that a command
will apply may not be sufficient to return a response to a client. In some
cases, the command may still be rejected (deterministically) and the client
should be alerted of that. In more extreme cases, the result of the command may
not even be known until it is applied to the state machine. In CockroachDB, this
was the case until the major rework that took place in 2016 called "proposer
evaluated KV" (see docs/RFCS/20160420_proposer_evaluated_kv.md). With the
completion of that change, client responses are determined before replication
begins. The only remaining work to be done after replication of a command
succeeds is to determine whether it will be rejected and replaced by an empty
command. To facilitate this acknowledgement as early as possible, this package
provides the ability to acknowledge a series of commands before applying them to
the state machine. Outcomes are determined before performing any durable work by
stepping commands through an in-memory "ephemeral" copy of the state machine.
For more, see Task.AckCommittedEntriesBeforeApplication.

A final challenge comes from the desire to properly prioritize the application
of commands across multiple state machines in systems like CockroachDB where
each machine hosts hundreds or thousands of replicas. This is a complicated
concern that must take into consideration the need for each replica's state
machine to stay up-to-date (is it a leaseholder? is it serving reads?), the need
to acknowledge clients in a timely manner (are clients waiting for command
application?), the desire to delay application to accumulate larger application
batches (will batching improve system throughput?), and a number of other
factors. This package has not begun to answer these questions, but it serves to
provide the abstractions necessary to perform such prioritization in the future.

Usage

The package exports a set of interfaces that users must provide implementations
for. Notably, users of the package must provide a StateMachine that encapsulates
the logic behind performing individual state transitions and a Decoder that is
capable of decoding raft entries and providing iteration over corresponding
Command objects.

These two structures can be used to create an application Task, which is capable
of applying raft entries to the StateMachine (see Task.ApplyCommittedEntries).
To do so, the Commands that were decoded using the Decoder (see Task.Decode) are
passed through a pipeline of stages. First, the Commands are checked for
rejection while being staged in an application Batch, which produces a set of
CheckedCommands. Next, the application Batch is committed to the StateMachine.
Following this, the in-memory side-effects of the CheckedCommands are applied to
the StateMachine, producing AppliedCommands. Finally, these AppliedCommands are
finalized and their clients are acknowledged.
*/
package apply
