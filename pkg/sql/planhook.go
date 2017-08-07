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
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

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
	parser.Statement, PlanHookState,
) (fn func(context.Context, chan<- parser.Datums) error, header sqlbase.ResultColumns, err error)

var planHooks []planHookFn

// PlanHookState exposes the subset of planner needed by plan hooks.
// We pass this as one interface, rather than individually passing each field or
// interface as we find we need them, to avoid churn in the planHookFn sig and
// the hooks that implement it.
type PlanHookState interface {
	EvalContext() parser.EvalContext
	ExecCfg() *ExecutorConfig
	DistLoader() *DistLoader
	TypeAsString(e parser.Expr, op string) (func() (string, error), error)
	TypeAsStringArray(e parser.Exprs, op string) (func() ([]string, error), error)
	TypeAsStringOpts(opts parser.KVOptions) (func() (map[string]string, error), error)
	User() string
	AuthorizationAccessor
}

// AddPlanHook adds a hook used to short-circuit creating a planNode from a
// parser.Statement. If the func returned by the hook is non-nil, it is used to
// construct a planNode that runs that func in a goroutine during Start.
func AddPlanHook(f planHookFn) {
	planHooks = append(planHooks, f)
}

// hookFnNode is a planNode implemented in terms of a function. It begins the
// provided function during Start and serves the results it returns over the
// channel.
type hookFnNode struct {
	f      func(context.Context, chan<- parser.Datums) error
	header sqlbase.ResultColumns

	resultsCh chan parser.Datums
	errCh     chan error

	row parser.Datums
}

func (*hookFnNode) Close(context.Context) {}

func (f *hookFnNode) Start(params runParams) error {
	// TODO(dan): Make sure the resultCollector is set to flush after every row.
	f.resultsCh = make(chan parser.Datums)
	f.errCh = make(chan error)
	go func() {
		f.errCh <- f.f(params.ctx, f.resultsCh)
		close(f.errCh)
		close(f.resultsCh)
	}()
	return nil
}

func (f *hookFnNode) Next(params runParams) (bool, error) {
	select {
	case <-params.ctx.Done():
		return false, params.ctx.Err()
	case err := <-f.errCh:
		return false, err
	case f.row = <-f.resultsCh:
		return true, nil
	}
}
func (f *hookFnNode) Values() parser.Datums { return f.row }
