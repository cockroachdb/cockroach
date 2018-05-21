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

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

func (b *Builder) buildShowTrace(showTrace *tree.ShowTrace, inScope *scope) (outScope *scope) {
	var stmtGroup memo.GroupID
	var stmtProps props.Physical

	if showTrace.Statement != nil {
		// We don't allow the statement under ShowTrace to reference outer columns, so we
		// pass a "blank" scope rather than inScope.
		stmtScope := b.buildStmt(showTrace.Statement, &scope{builder: b})
		// Calculate the presentation, since we will store it in the ShowTrace op.
		stmtScope.setPresentation()
		stmtGroup = stmtScope.group
		stmtProps = stmtScope.physicalProps
	}

	outScope = inScope.push()

	switch showTrace.TraceType {
	case tree.ShowTraceRaw, tree.ShowTraceKV:
		if showTrace.Compact {
			b.synthesizeResultColumns(outScope, sqlbase.ShowCompactTraceColumns)
		} else {
			b.synthesizeResultColumns(outScope, sqlbase.ShowTraceColumns)
		}

	case tree.ShowTraceReplica:
		b.synthesizeResultColumns(outScope, sqlbase.ShowReplicaTraceColumns)

	default:
		panic(errorf("SHOW %s not supported", showTrace.TraceType))
	}

	def := memo.ShowTraceOpDef{
		Type:    showTrace.TraceType,
		Compact: showTrace.Compact,
		ColList: colsToColList(outScope.cols),
		Props:   stmtProps,
	}
	for i := range outScope.cols {
		def.ColList[i] = outScope.cols[i].id
	}
	if showTrace.Statement != nil {
		outScope.group = b.factory.ConstructShowTrace(stmtGroup, b.factory.InternShowTraceOpDef(&def))
	} else {
		outScope.group = b.factory.ConstructShowTraceForSession(b.factory.InternShowTraceOpDef(&def))
	}
	return outScope
}
