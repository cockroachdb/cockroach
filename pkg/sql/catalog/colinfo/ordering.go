// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
//
//	[]ColumnOrderInfo{ {3, encoding.Descending}, {1, encoding.Ascending} }
//
// represents an ordering first by column 3 (descending), then by column 1 (ascending).
type ColumnOrdering []ColumnOrderInfo

// Equal returns whether two ColumnOrderings are the same.
func (ordering ColumnOrdering) Equal(other ColumnOrdering) bool {
	if len(ordering) != len(other) {
		return false
	}
	for i, o := range ordering {
		if o.ColIdx != other[i].ColIdx || o.Direction != other[i].Direction {
			return false
		}
	}
	return true
}

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
