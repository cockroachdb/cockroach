// Copyright 2016 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// planHookFn is a function that can intercept a statement being planned and
// provide an alternate implementation. It's primarily intended to allow
// implementation of certain sql statements to live outside of the sql package.
//
// To intercept a statement the function should return a non-nil function for
// `fn` as well as the appropriate sqlbase.ResultColumns describing the results
// it will return (if any). If the hook plan requires sub-plans to be planned
// and started by the usual machinery (e.g. to run a subquery), it must return
// then as well. `fn` will be called in a goroutine during the `Start` phase of
// plan execution.
type planHookFn func(
	context.Context, tree.Statement, PlanHookState,
) (fn PlanHookRowFn, header colinfo.ResultColumns, subplans []planNode, avoidBuffering bool, err error)

// PlanHookRowFn describes the row-production for hook-created plans. The
// channel argument is used to return results to the plan's runner. It's
// a blocking channel, so implementors should be careful to only use blocking
// sends on it when necessary. Any subplans returned by the hook when initially
// called are passed back, planned and started, for the RowFn's use.
//
//TODO(dt): should this take runParams like a normal planNode.Next?
type PlanHookRowFn func(context.Context, []planNode, chan<- tree.Datums) error

var planHooks []planHookFn

func (p *planner) RunParams(ctx context.Context) runParams {
	return runParams{ctx, p.ExtendedEvalContext(), p}
}

// PlanHookState exposes the subset of planner needed by plan hooks.
// We pass this as one interface, rather than individually passing each field or
// interface as we find we need them, to avoid churn in the planHookFn sig and
// the hooks that implement it.
//
// The PlanHookState is used by modules that are under the CCL. Since the OSS
// modules cannot depend on the CCL modules, the CCL modules need to inform the
// planner when they should be invoked (via plan hooks). The only way for the
// CCL statements to get access to a "planner" is through this PlanHookState
// that gets passed back due to this inversion of roles.
type PlanHookState interface {
	resolver.SchemaResolver
	RunParams(ctx context.Context) runParams
	SemaCtx() *tree.SemaContext
	ExtendedEvalContext() *extendedEvalContext
	SessionData() *sessiondata.SessionData
	ExecCfg() *ExecutorConfig
	DistSQLPlanner() *DistSQLPlanner
	LeaseMgr() *lease.Manager
	TypeAsString(ctx context.Context, e tree.Expr, op string) (func() (string, error), error)
	TypeAsStringArray(ctx context.Context, e tree.Exprs, op string) (func() ([]string, error), error)
	TypeAsStringOpts(
		ctx context.Context, opts tree.KVOptions, optsValidate map[string]KVStringOptValidate,
	) (func() (map[string]string, error), error)
	User() security.SQLUsername
	AuthorizationAccessor
	// The role create/drop call into OSS code to reuse plan nodes.
	// TODO(mberhault): it would be easier to just pass a planner to plan hooks.
	GetAllRoles(ctx context.Context) (map[security.SQLUsername]bool, error)
	BumpRoleMembershipTableVersion(ctx context.Context) error
	EvalAsOfTimestamp(ctx context.Context, asOf tree.AsOfClause) (hlc.Timestamp, error)
	ResolveMutableTableDescriptor(ctx context.Context, tn *tree.TableName, required bool, requiredType tree.RequiredTableKind) (prefix catalog.ResolvedObjectPrefix, table *tabledesc.Mutable, err error)
	ShowCreate(
		ctx context.Context, dbPrefix string, allDescs []descpb.Descriptor, desc catalog.TableDescriptor, displayOptions ShowCreateDisplayOptions,
	) (string, error)
	CreateSchemaNamespaceEntry(ctx context.Context, schemaNameKey roachpb.Key,
		schemaID descpb.ID) error
	MigrationJobDeps() migration.JobDeps
	BufferClientNotice(ctx context.Context, notice pgnotice.Notice)
}

// AddPlanHook adds a hook used to short-circuit creating a planNode from a
// tree.Statement. If the func returned by the hook is non-nil, it is used to
// construct a planNode that runs that func in a goroutine during Start.
//
// See PlanHookState comments for information about why plan hooks are needed.
func AddPlanHook(f planHookFn) {
	planHooks = append(planHooks, f)
}

// ClearPlanHooks is used by tests to clear out any mocked out plan hooks that
// were registered.
func ClearPlanHooks() {
	planHooks = nil
}

// hookFnNode is a planNode implemented in terms of a function. It begins the
// provided function during Start and serves the results it returns over the
// channel.
type hookFnNode struct {
	optColumnsSlot

	f        PlanHookRowFn
	header   colinfo.ResultColumns
	subplans []planNode

	run hookFnRun
}

// hookFnRun contains the run-time state of hookFnNode during local execution.
type hookFnRun struct {
	resultsCh chan tree.Datums
	errCh     chan error

	row tree.Datums
}

func (f *hookFnNode) startExec(params runParams) error {
	// TODO(dan): Make sure the resultCollector is set to flush after every row.
	f.run.resultsCh = make(chan tree.Datums)
	f.run.errCh = make(chan error)
	go func() {
		err := f.f(params.ctx, f.subplans, f.run.resultsCh)
		select {
		case <-params.ctx.Done():
		case f.run.errCh <- err:
		}
		close(f.run.errCh)
		close(f.run.resultsCh)
	}()
	return nil
}

func (f *hookFnNode) Next(params runParams) (bool, error) {
	select {
	case <-params.ctx.Done():
		return false, params.ctx.Err()
	case err := <-f.run.errCh:
		return false, err
	case f.run.row = <-f.run.resultsCh:
		return true, nil
	}
}

func (f *hookFnNode) Values() tree.Datums { return f.run.row }

func (f *hookFnNode) Close(ctx context.Context) {
	for _, sub := range f.subplans {
		sub.Close(ctx)
	}
}
