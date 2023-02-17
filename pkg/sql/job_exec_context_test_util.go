// Copyright 2021 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// FakeJobExecContext is used for mocking the JobExecContext in tests.
type FakeJobExecContext struct {
	JobExecContext
	ExecutorConfig *ExecutorConfig
}

// ExecCfg implements the JobExecContext interface.
func (p *FakeJobExecContext) ExecCfg() *ExecutorConfig {
	return p.ExecutorConfig
}

// SemaCtx implements the JobExecContext interface.
func (p *FakeJobExecContext) SemaCtx() *tree.SemaContext {
	return nil
}

// ExtendedEvalContext implements the JobExecContext interface.
func (p *FakeJobExecContext) ExtendedEvalContext() *extendedEvalContext {
	panic("unimplemented")
}

// SessionData implements the JobExecContext interface.
func (p *FakeJobExecContext) SessionData() *sessiondata.SessionData {
	return nil
}

// SessionDataMutatorIterator implements the JobExecContext interface.
func (p *FakeJobExecContext) SessionDataMutatorIterator() *sessionDataMutatorIterator {
	panic("unimplemented")
}

// DistSQLPlanner implements the JobExecContext interface.
func (p *FakeJobExecContext) DistSQLPlanner() *DistSQLPlanner {
	if p.ExecutorConfig == nil {
		panic("unimplemented")
	}
	return p.ExecutorConfig.DistSQLPlanner
}

// LeaseMgr implements the JobExecContext interface.
func (p *FakeJobExecContext) LeaseMgr() *lease.Manager {
	panic("unimplemented")
}

// User implements the JobExecContext interface.
func (p *FakeJobExecContext) User() username.SQLUsername {
	panic("unimplemented")
}

// MigrationJobDeps implements the JobExecContext interface.
func (p *FakeJobExecContext) MigrationJobDeps() upgrade.JobDeps {
	panic("unimplemented")
}

// SpanConfigReconciler implements the JobExecContext interface.
func (p *FakeJobExecContext) SpanConfigReconciler() spanconfig.Reconciler {
	panic("unimplemented")
}
