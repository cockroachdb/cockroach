// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/redact"
)

// plannerJobExecContext is a wrapper to implement JobExecContext with a planner
// without allowing casting directly to a planner. Eventually it would be nice
// if we could implement the API entirely without a planner however the only
// implementation of extendedEvalContext is very tied to a planner.
type plannerJobExecContext struct {
	p *planner
}

// MakeJobExecContext makes a JobExecContext.
func MakeJobExecContext(
	ctx context.Context,
	opName redact.SafeString,
	user username.SQLUsername,
	memMetrics *MemoryMetrics,
	execCfg *ExecutorConfig,
) (JobExecContext, func()) {
	plannerInterface, close := NewInternalPlanner(
		opName,
		nil, /*txn*/
		user,
		memMetrics,
		execCfg,
		NewInternalSessionData(ctx, execCfg.Settings, opName),
	)
	p := plannerInterface.(*planner)
	return &plannerJobExecContext{p: p}, close
}

func (e *plannerJobExecContext) SemaCtx() *tree.SemaContext { return e.p.SemaCtx() }
func (e *plannerJobExecContext) ExtendedEvalContext() *extendedEvalContext {
	return e.p.ExtendedEvalContext()
}
func (e *plannerJobExecContext) SessionData() *sessiondata.SessionData {
	return e.p.SessionData()
}
func (e *plannerJobExecContext) SessionDataMutatorIterator() *sessionDataMutatorIterator {
	return e.p.SessionDataMutatorIterator()
}
func (e *plannerJobExecContext) ExecCfg() *ExecutorConfig        { return e.p.ExecCfg() }
func (e *plannerJobExecContext) DistSQLPlanner() *DistSQLPlanner { return e.p.DistSQLPlanner() }
func (e *plannerJobExecContext) LeaseMgr() *lease.Manager        { return e.p.LeaseMgr() }
func (e *plannerJobExecContext) User() username.SQLUsername      { return e.p.User() }
func (e *plannerJobExecContext) MigrationJobDeps() upgrade.JobDeps {
	return e.p.MigrationJobDeps()
}
func (e *plannerJobExecContext) SpanConfigReconciler() spanconfig.Reconciler {
	return e.p.SpanConfigReconciler()
}

func (e *plannerJobExecContext) SpanStatsConsumer() keyvisualizer.SpanStatsConsumer {
	return e.p.SpanStatsConsumer()
}

// JobExecContext provides the execution environment for a job. It is what is
// passed to the Resume/OnFailOrCancel methods of a jobs's
// Resumer to give that resumer access to things like ExecutorCfg, LeaseMgr,
// etc -- the kinds of things that would usually be on planner or similar during
// a non-job SQL statement's execution. Unlike a planner however, or planner-ish
// interfaces like PlanHookState, JobExecContext does not include a txn or the
// methods that defined in terms of "the" txn, such as privilege/name accessors.
// (though note that ExtendedEvalContext may transitively include methods that
// close over/expect a txn so use it with caution).
type JobExecContext interface {
	SemaCtx() *tree.SemaContext
	ExtendedEvalContext() *extendedEvalContext
	SessionData() *sessiondata.SessionData
	SessionDataMutatorIterator() *sessionDataMutatorIterator
	ExecCfg() *ExecutorConfig
	DistSQLPlanner() *DistSQLPlanner
	LeaseMgr() *lease.Manager
	User() username.SQLUsername
	MigrationJobDeps() upgrade.JobDeps
	SpanConfigReconciler() spanconfig.Reconciler
	SpanStatsConsumer() keyvisualizer.SpanStatsConsumer
}
