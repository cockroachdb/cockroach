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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// hookFnNode is a planNode implemented in terms of a function. It begins the
// provided function during Start and serves the results it returns over the
// channel.
type hookFnNode struct {
	f      func(context.Context, chan<- tree.Datums) error
	header sqlbase.ResultColumns

	run hookFnRun
}

// planHookFn is a function that can intercept a statement being planned and
// provide an alternate implementation. It's primarily intended to allow
// implementation of certain sql statements to live outside of the sql package.
//
// To intercept a statement the function should return a non-nil function for
// `fn` as well as the appropriate sqlbase.ResultColumns describing the results
// it will return (if any). `fn` will be called in a goroutine during the
// `Start` phase of plan execution.
//
// The channel argument to `fn` is used to return results with the client. It's
// a blocking channel, so implementors should be careful to only use blocking
// sends on it when necessary.
type planHookFn func(
	context.Context, tree.Statement, PlanHookState,
) (fn func(context.Context, chan<- tree.Datums) error, header sqlbase.ResultColumns, err error)

var planHooks []planHookFn

// wrappedPlanHookFn is similar to planHookFn but returns an existing plan type.
// Additionally, it takes a context.
type wrappedPlanHookFn func(
	context.Context, tree.Statement, PlanHookState,
) (planNode, error)

var wrappedPlanHooks []wrappedPlanHookFn

// PlanHookState exposes the subset of planner needed by plan hooks.
// We pass this as one interface, rather than individually passing each field or
// interface as we find we need them, to avoid churn in the planHookFn sig and
// the hooks that implement it.
type PlanHookState interface {
	SchemaResolver
	ExtendedEvalContext() *extendedEvalContext
	SessionData() *sessiondata.SessionData
	ExecCfg() *ExecutorConfig
	DistLoader() *DistLoader
	LeaseMgr() *LeaseManager
	TypeAsString(e tree.Expr, op string) (func() (string, error), error)
	TypeAsStringArray(e tree.Exprs, op string) (func() ([]string, error), error)
	TypeAsStringOpts(
		opts tree.KVOptions, valuelessOpts map[string]bool,
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
		f.run.errCh <- f.f(params.ctx, f.run.resultsCh)
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
func (*hookFnNode) Close(context.Context) {}
