// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scop

// A Phase represents the context in which an op is executed within a schema
// change. Different phases require different dependencies for the execution of
// the ops to be plumbed in.
//
// Today, we support the phases corresponding to async schema changes initiated
// and partially executed in the user transaction. This will change as we
// transition to transactional schema changes.
type Phase int

//go:generate stringer --type Phase

const (
	_ Phase = iota
	// StatementPhase refers to execution of ops occurring during statement
	// execution during the user transaction.
	StatementPhase
	// PreCommitPhase refers to execution of ops occurring during the user
	// transaction immediately before commit.
	PreCommitPhase
	// PostCommitPhase refers to execution of ops occurring after the user
	// transaction has committed (i.e., in the async schema change job).
	// Note: Planning rules cannot ever be in this phase, since all those operations
	// should be executed in pre-commit.
	PostCommitPhase
	// PostCommitNonRevertiblePhase is like PostCommitPhase but in which target
	// status changes are non-revertible.
	PostCommitNonRevertiblePhase

	// EarliestPhase references the earliest possible execution phase.
	EarliestPhase = StatementPhase

	// LatestPhase references the latest possible execution phase.
	LatestPhase = PostCommitNonRevertiblePhase
)
