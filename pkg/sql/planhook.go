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
//
// Author: Daniel Harrison (daniel.harrison@gmail.com)

package sql

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// planHookFn is a function that can intercept a statement being planned and
// provide an alternate implementation. It's primarily intended to allow
// implementation of certain sql statements to live outside of the sql package.
//
// To intercept a statement the function should return a non-nil function for
// `fn` as well as the appropriate sqlbase.ResultColumns describing the results it will
// return (if any). `fn` will be called during the `Start` phase of plan
// execution.
type planHookFn func(
	context.Context, parser.Statement, PlanHookState,
) (fn func() ([]parser.Datums, error), header sqlbase.ResultColumns, err error)

var planHooks []planHookFn

// PlanHookState exposes the subset of planner needed by plan hooks.
// We pass this as one interface, rather than individually passing each field or
// interface as we find we need them, to avoid churn in the planHookFn sig and
// the hooks that implement it.
type PlanHookState interface {
	ExecCfg() *ExecutorConfig
	LeaseMgr() *LeaseManager
	TypeAsString(e *parser.Expr) (func() string, error)
	TypeAsStringArray(e *parser.Exprs) (func() []string, error)
	User() string
	AuthorizationAccessor
}

// AddPlanHook adds a hook used to short-circuit creating a planNode from a
// parser.Statement. If the func returned by the hook is non-nil, it is used to
// construct a planNode that runs that func during Start.
func AddPlanHook(f planHookFn) {
	planHooks = append(planHooks, f)
}

// hookFnNode is a planNode implemented in terms of a function. It runs the
// provided function during Start and serves the results it returned.
type hookFnNode struct {
	f func() ([]parser.Datums, error)

	header sqlbase.ResultColumns

	res    []parser.Datums
	resIdx int
}

func (*hookFnNode) Ordering() orderingInfo  { return orderingInfo{} }
func (*hookFnNode) MarkDebug(_ explainMode) {}
func (*hookFnNode) Close(context.Context)   {}

func (*hookFnNode) Spans(context.Context) (_, _ roachpb.Spans, _ error) {
	panic("unimplemented")
}

func (f *hookFnNode) Start(context.Context) error {
	var err error
	f.res, err = f.f()
	f.resIdx = -1
	return err
}
func (f *hookFnNode) Columns() sqlbase.ResultColumns {
	return f.header
}
func (f *hookFnNode) Next(context.Context) (bool, error) {
	if f.res == nil {
		return false, nil
	}
	f.resIdx++
	return f.resIdx < len(f.res), nil
}
func (f *hookFnNode) Values() parser.Datums { return f.res[f.resIdx] }

func (*hookFnNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}
