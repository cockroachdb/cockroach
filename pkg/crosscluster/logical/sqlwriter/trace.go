// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlwriter

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// FormatRow formats a row with column names for logging purposes.
// Output format: [col1=val1, col2=val2, ...]
func FormatRow(columns []string, row tree.Datums) string {
	var sb strings.Builder
	sb.WriteByte('[')
	for i, d := range row {
		if i > 0 {
			sb.WriteString(", ")
		}
		if i < len(columns) {
			sb.WriteString(columns[i])
		} else {
			fmt.Fprintf(&sb, "col%d", i)
		}
		sb.WriteByte('=')
		if d == nil || d == tree.DNull {
			sb.WriteString("NULL")
		} else {
			fmt.Fprintf(&sb, "%s", d)
		}
	}
	sb.WriteByte(']')
	return sb.String()
}

// Columns returns the column names used by this RowWriter.
func (s *RowWriter) Columns() []string {
	return s.columns
}
