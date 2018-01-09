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

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

func init() {
	registerOperator(scanOp, operatorInfo{name: "scan", class: scanClass{}})
}

func initScanExpr(e *Expr, tab optbase.Table) {
	e.op = scanOp
	e.private = tab
}

type scanClass struct{}

var _ operatorClass = scanClass{}

func (scanClass) layout() exprLayout {
	return exprLayout{}
}

func (scanClass) format(e *Expr, tp treeprinter.Node) {
	formatRelational(e, tp)
}
