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

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
)

type planHookFn func(
	context.Context, parser.Statement, *ExecutorConfig,
) (func() ([]parser.DTuple, error), ResultColumns, error)

var planHooks []planHookFn

// AddPlanHook adds a hook used to short-circuit creating a planNode from a
// parser.Statement. If the func returned by the hook is non-nil, it is used to
// construct a planNode that runs that func during Start.
func AddPlanHook(f planHookFn) {
	planHooks = append(planHooks, f)
}

// funcNode is a planNode implemented in terms of a planHookRunFn. It runs the
// provided function during Start.
type funcNode struct {
	f func() ([]parser.DTuple, error)

	header ResultColumns

	res    []parser.DTuple
	resIdx int
}

var _ planNode = &funcNode{}

func (*funcNode) Ordering() orderingInfo              { return orderingInfo{} }
func (*funcNode) ExplainTypes(_ func(string, string)) {}
func (*funcNode) SetLimitHint(_ int64, _ bool)        {}
func (*funcNode) MarkDebug(_ explainMode)             {}
func (*funcNode) expandPlan() error                   { return nil }
func (*funcNode) Close()                              {}
func (*funcNode) setNeededColumns(_ []bool)           {}

func (f *funcNode) Start() error {
	var err error
	f.res, err = f.f()
	f.resIdx = -1
	return err
}
func (f *funcNode) Columns() ResultColumns {
	return f.header
}
func (f *funcNode) Next() (bool, error) {
	if f.res == nil {
		return false, nil
	}
	f.resIdx++
	return f.resIdx < len(f.res), nil
}
func (f *funcNode) Values() parser.DTuple { return f.res[f.resIdx] }

func (*funcNode) ExplainPlan(_ bool) (name, description string, children []planNode) {
	return "func", "-", nil
}

func (*funcNode) DebugValues() debugValues {
	return debugValues{
		rowIdx: 0,
		key:    "",
		value:  parser.DNull.String(),
		output: debugValueRow,
	}
}
