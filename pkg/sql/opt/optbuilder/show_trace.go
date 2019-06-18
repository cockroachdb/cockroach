// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
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
		panic(errors.AssertionFailedf("SHOW %s not supported", showTrace.TraceType))
	}

	outScope.expr = b.factory.ConstructShowTraceForSession(&memo.ShowTracePrivate{
		TraceType: showTrace.TraceType,
		Compact:   showTrace.Compact,
		ColList:   colsToColList(outScope.cols),
	})
	return outScope
}
