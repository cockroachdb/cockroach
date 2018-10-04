// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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
) (fn PlanHookRowFn, header sqlbase.ResultColumns, subplans []planNode, err error)

// PlanHookRowFn describes the row-production for hook-created plans. The
// channel argument is used to return results to the plan's runner. It's
// a blocking channel, so implementors should be careful to only use blocking
// sends on it when necessary. Any subplans returned by the hook when initially
// called are passed back, planned and started, for the the RowFn's use.
//
//TODO(dt): should this take runParams like a normal planNode.Next?
type PlanHookRowFn func(context.Context, []planNode, chan<- tree.Datums) error

var planHooks []planHookFn

// wrappedPlanHookFn is similar to planHookFn but returns an existing plan type.
// Additionally, it takes a context.
type wrappedPlanHookFn func(
	context.Context, tree.Statement, PlanHookState,
) (planNode, error)

var wrappedPlanHooks []wrappedPlanHookFn

func (p *planner) RunParams(ctx context.Context) runParams {
	return runParams{ctx, p.ExtendedEvalContext(), p}
}

// PlanHookState exposes the subset of planner needed by plan hooks.
// We pass this as one interface, rather than individually passing each field or
// interface as we find we need them, to avoid churn in the planHookFn sig and
// the hooks that implement it.
type PlanHookState interface {
	SchemaResolver
	RunParams(ctx context.Context) runParams
	ExtendedEvalContext() *extendedEvalContext
	SessionData() *sessiondata.SessionData
	ExecCfg() *ExecutorConfig
	DistSQLPlanner() *DistSQLPlanner
	LeaseMgr() *LeaseManager
	TypeAsString(e tree.Expr, op string) (func() (string, error), error)
	TypeAsStringArray(e tree.Exprs, op string) (func() ([]string, error), error)
	TypeAsStringOpts(
		opts tree.KVOptions, optsValidate map[string]KVStringOptValidate,
	) (func() (map[string]string, error), error)
	User() string
	AuthorizationAccessor
	// The role create/drop call into OSS code to reuse plan nodes.
	// TODO(mberhault): it would be easier to just pass a planner to plan hooks.
	CreateUserNode(
		ctx context.Context, nameE, passwordE tree.Expr, ifNotExists bool, isRole bool, opName string,
	) (*CreateUserNode, error)
	DropUserNode(
		ctx context.Context, namesE tree.Exprs, ifExists bool, isRole bool, opName string,
	) (*DropUserNode, error)
	GetAllUsersAndRoles(ctx context.Context) (map[string]bool, error)
	BumpRoleMembershipTableVersion(ctx context.Context) error
	Select(ctx context.Context, n *tree.Select, desiredTypes []types.T) (planNode, error)
	EvalAsOfTimestamp(asOf tree.AsOfClause, max hlc.Timestamp) (hlc.Timestamp, error)
	ResolveUncachedDatabaseByName(
		ctx context.Context, dbName string, required bool) (*UncachedDatabaseDescriptor, error)
}

// AddPlanHook adds a hook used to short-circuit creating a planNode from a
// tree.Statement. If the func returned by the hook is non-nil, it is used to
// construct a planNode that runs that func in a goroutine during Start.
func AddPlanHook(f planHookFn) {
	planHooks = append(planHooks, f)
}

// AddWrappedPlanHook adds a hook used to short-circuit creating a planNode from a
// tree.Statement. If the returned plan is non-nil, it is used directly by the planner.
func AddWrappedPlanHook(f wrappedPlanHookFn) {
	wrappedPlanHooks = append(wrappedPlanHooks, f)
}

// hookFnNode is a planNode implemented in terms of a function. It begins the
// provided function during Start and serves the results it returns over the
// channel.
type hookFnNode struct {
	f        PlanHookRowFn
	header   sqlbase.ResultColumns
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
