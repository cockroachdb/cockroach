// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// plannerJobExecContext is a wrapper to implement JobExecContext with a planner
// without allowing casting directly to a planner. Eventually it would be nice
// if we could implement the API entirely without a planner however the only
// implementation of extendedEvalContext is very tied to a planner.
type plannerJobExecContext struct {
	p *planner
}

// MakeJobExecContext makes a JobExecContext.
func MakeJobExecContext(opName string, user string, memMetrics *MemoryMetrics, execCfg *ExecutorConfig) (JobExecContext, func()) {
	p, close := newInternalPlanner(opName, nil /*txm*/, user, memMetrics, execCfg)
	return &plannerJobExecContext{p: p}, close
}

func (e *plannerJobExecContext) SemaCtx() *tree.SemaContext { return e.p.SemaCtx() }
func (e *plannerJobExecContext) ExtendedEvalContext() *extendedEvalContext {
	return e.p.ExtendedEvalContext()
}
func (e *plannerJobExecContext) SessionData() *sessiondata.SessionData {
	return e.p.SessionData()
}
func (e *plannerJobExecContext) ExecCfg() *ExecutorConfig        { return e.p.ExecCfg() }
func (e *plannerJobExecContext) DistSQLPlanner() *DistSQLPlanner { return e.p.DistSQLPlanner() }
func (e *plannerJobExecContext) LeaseMgr() *lease.Manager        { return e.p.LeaseMgr() }
func (e *plannerJobExecContext) User() string                    { return e.p.User() }

// JobExecContext provides the execution enviorment for a job. It is what is
// passed to the Resume/OnFailOrCancel/OnPauseRequested methods of a jobs's
// Resumer to give that resumer access to things like ExecutorCfg, LeaseMgr,
// etc -- the kinds of things that would usually be on planner or similar during
// a non-job SQL statement's execution.
type JobExecContext interface {
	SemaCtx() *tree.SemaContext
	ExtendedEvalContext() *extendedEvalContext
	SessionData() *sessiondata.SessionData
	ExecCfg() *ExecutorConfig
	DistSQLPlanner() *DistSQLPlanner
	LeaseMgr() *lease.Manager
	User() string
}
