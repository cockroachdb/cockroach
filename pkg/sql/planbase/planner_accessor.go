// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package planbase

import (
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// PlannerAccessor provides access to planner methods needed during plan execution.
// This interface breaks the circular dependency between planbase and the sql package.
type PlannerAccessor interface {
	// Txn returns the transaction for this execution.
	Txn() *kv.Txn

	// ExtendedEvalContext returns the extended evaluation context.
	ExtendedEvalContext() ExtendedEvalContextI

	// EvalContext returns the evaluation context.
	EvalContext() *eval.Context

	// SessionData returns the session data.
	SessionData() *sessiondata.SessionData
}
