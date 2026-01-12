// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package planbase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// RunParams is a struct containing all parameters passed to planNode.Next() and
// startPlan.
//
// IMPORTANT: Fields are now exported (capitalized) because RunParams is in a separate
// package from where it's used. Code in pkg/sql accesses these fields directly.
type RunParams struct {
	// Ctx is the context.Context for this method call.
	Ctx context.Context

	// ExtendedEvalCtx groups fields useful for this execution.
	// Used during local execution and distsql physical planning.
	ExtendedEvalCtx ExtendedEvalContextI

	// P is the planner associated with this execution. Only used during local
	// execution.
	P PlannerAccessor
}

// EvalContext gives convenient access to the runParam's EvalContext().
func (r *RunParams) EvalContext() *eval.Context {
	return r.ExtendedEvalCtx.EvalContext()
}

// SessionData gives convenient access to the runParam's SessionData.
func (r *RunParams) SessionData() *sessiondata.SessionData {
	return r.ExtendedEvalCtx.SessionData()
}

// ExecCfg gives convenient access to the runParam's ExecutorConfig.
// Note: This returns interface{} to avoid circular import - cast to *ExecutorConfig in pkg/sql
func (r *RunParams) ExecCfg() interface{} {
	return r.ExtendedEvalCtx.GetExecCfg()
}

// Ann is a shortcut for the Annotations from the eval context.
func (r *RunParams) Ann() *tree.Annotations {
	return r.EvalContext().Annotations
}
