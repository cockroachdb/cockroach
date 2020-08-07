// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildShowTrace(
	showTrace *tree.ShowTraceForSession, inScope *scope,
) (outScope *scope) {
	outScope = inScope.push()

	switch showTrace.TraceType {
	case tree.ShowTraceRaw, tree.ShowTraceKV:
		if showTrace.Compact {
			b.synthesizeResultColumns(outScope, colinfo.ShowCompactTraceColumns)
		} else {
			b.synthesizeResultColumns(outScope, colinfo.ShowTraceColumns)
		}

	case tree.ShowTraceReplica:
		b.synthesizeResultColumns(outScope, colinfo.ShowReplicaTraceColumns)

	default:
		panic(errors.AssertionFailedf("SHOW %s not supported", showTrace.TraceType))
	}

	outScope.expr = b.factory.ConstructShowTraceForSession(&memo.ShowTracePrivate{
		TraceType: showTrace.TraceType,
		Compact:   showTrace.Compact,
		ColList:   colsToColList(outScope.cols),
	})
	return outScope
}
