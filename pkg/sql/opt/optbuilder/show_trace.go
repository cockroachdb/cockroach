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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

func (b *Builder) buildShowTrace(
	showTrace *tree.ShowTraceForSession, inScope *scope,
) (outScope *scope) {
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
		panic(fmt.Errorf("SHOW %s not supported", showTrace.TraceType))
	}

	def := memo.ShowTraceOpDef{
		Type:    showTrace.TraceType,
		Compact: showTrace.Compact,
		ColList: colsToColList(outScope.cols),
	}
	for i := range outScope.cols {
		def.ColList[i] = outScope.cols[i].id
	}
	outScope.group = b.factory.ConstructShowTraceForSession(b.factory.InternShowTraceOpDef(&def))
	return outScope
}
