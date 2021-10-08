// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo

import (
	"bytes"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// ColumnOrderInfo describes a column (as an index) and a desired order
// direction.
type ColumnOrderInfo struct {
	ColIdx    int
	Direction encoding.Direction
}

// ColumnOrdering is used to describe a desired column ordering. For example,
//     []ColumnOrderInfo{ {3, encoding.Descending}, {1, encoding.Ascending} }
// represents an ordering first by column 3 (descending), then by column 1 (ascending).
type ColumnOrdering []ColumnOrderInfo

func (ordering ColumnOrdering) String(columns ResultColumns) string {
	var buf bytes.Buffer
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	for i, o := range ordering {
		if i > 0 {
			buf.WriteByte(',')
		}
		prefix := byte('+')
		if o.Direction == encoding.Descending {
			prefix = byte('-')
		}
		buf.WriteByte(prefix)

		fmtCtx.FormatNameP(&columns[o.ColIdx].Name)
		_, _ = fmtCtx.WriteTo(&buf)
	}
	fmtCtx.Close()
	return buf.String()
}

// NoOrdering is used to indicate an empty ColumnOrdering.
var NoOrdering ColumnOrdering

// CompareDatums compares two datum rows according to a column ordering. Returns:
//  - 0 if lhs and rhs are equal on the ordering columns;
//  - less than 0 if lhs comes first;
//  - greater than 0 if rhs comes first.
func CompareDatums(ordering ColumnOrdering, evalCtx *tree.EvalContext, lhs, rhs tree.Datums) int {
	for _, c := range ordering {
		// TODO(pmattis): This is assuming that the datum types are compatible. I'm
		// not sure this always holds as `CASE` expressions can return different
		// types for a column for different rows. Investigate how other RDBMs
		// handle this.
		if cmp := lhs[c.ColIdx].Compare(evalCtx, rhs[c.ColIdx]); cmp != 0 {
			if c.Direction == encoding.Descending {
				cmp = -cmp
			}
			return cmp
		}
	}
	return 0
}
