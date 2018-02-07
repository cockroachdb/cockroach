// Copyright 2018 The Cockroach Authors.
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

package build

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
)

func (b *Builder) buildRelational(ev xform.ExprView) (exec.Node, error) {
	switch ev.Operator() {
	case opt.ScanOp:
		return b.buildScan(ev)

	case opt.SelectOp:
		return b.buildSelect(ev)

	default:
		panic(fmt.Sprintf("unsupported relational op %s", ev.Operator()))
	}
}

func (b *Builder) buildScan(ev xform.ExprView) (exec.Node, error) {
	tblIndex := ev.Private().(opt.TableIndex)
	tbl := ev.Metadata().Table(tblIndex)
	return b.factory.ConstructScan(tbl)
}

func (b *Builder) buildSelect(ev xform.ExprView) (exec.Node, error) {
	input, err := b.buildRelational(ev.Child(0))
	if err != nil {
		return nil, err
	}
	filter := b.buildScalar(ev.Child(1))
	return b.factory.ConstructFilter(input, filter)
}
