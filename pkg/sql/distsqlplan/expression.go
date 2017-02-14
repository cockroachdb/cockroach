// Copyright 2017 The Cockroach Authors.
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
// Author: Radu Berinde (radu@cockroachlabs.com)

// This file contains helper code to populate distsqlrun.Expressions during
// planning.

package distsqlplan

import (
	"bytes"
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlrun"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// exprFmtFlagsBase are FmtFlags used for serializing expressions; a proper
// IndexedVar formatting function needs to be added on.
var exprFmtFlagsBase = parser.FmtStarDatumFormat(
	parser.FmtParsable,
	func(buf *bytes.Buffer, _ parser.FmtFlags) {
		fmt.Fprintf(buf, "0")
	},
)

// exprFmtFlagsNoMap are FmtFlags used for serializing expressions that don't
// need to remap IndexedVars.
var exprFmtFlagsNoMap = parser.FmtIndexedVarFormat(
	exprFmtFlagsBase,
	func(buf *bytes.Buffer, _ parser.FmtFlags, _ parser.IndexedVarContainer, idx int) {
		fmt.Fprintf(buf, "@%d", idx+1)
	},
)

// MakeExpression creates a distsqlrun.Expression.
//
// The distsqlrun.Expression uses the placeholder syntax (@1, @2, @3..) to refer
// to columns.
//
// The expr uses IndexedVars to refer to columns. The caller can optionally
// remap these columns by passing an indexVarMap: an IndexedVar with index i
// becomes column indexVarMap[i].
func MakeExpression(expr parser.TypedExpr, indexVarMap []int) distsqlrun.Expression {
	if expr == nil {
		return distsqlrun.Expression{}
	}

	// We format the expression using the IndexedVar and StarDatum formatting interceptors.
	var f parser.FmtFlags
	if indexVarMap == nil {
		f = exprFmtFlagsNoMap
	} else {
		f = parser.FmtIndexedVarFormat(
			exprFmtFlagsBase,
			func(buf *bytes.Buffer, _ parser.FmtFlags, _ parser.IndexedVarContainer, idx int) {
				remappedIdx := indexVarMap[idx]
				if remappedIdx < 0 {
					panic(fmt.Sprintf("unmapped index %d", idx))
				}
				fmt.Fprintf(buf, "@%d", remappedIdx+1)
			},
		)
	}
	var buf bytes.Buffer
	expr.Format(&buf, f)
	if log.V(1) {
		log.Infof(context.TODO(), "Expr %s:\n%s", buf.String(), parser.ExprDebugString(expr))
	}
	return distsqlrun.Expression{Expr: buf.String()}
}
