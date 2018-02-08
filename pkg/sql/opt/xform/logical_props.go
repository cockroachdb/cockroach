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

package xform

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// LogicalProps describe the content and characteristics of data returned by
// all expression variants within a memo group. While each expression in the
// group may return rows or columns in a different order, or compute the
// result using different algorithms, the complete set of data is returned
// and can be transformed into whatever layout or presentation format that is
// desired.
type LogicalProps struct {
	// Relational contains the set of properties that describe relational
	// operators, like select, join, and project. It is nil for scalar
	// operators.
	Relational *RelationalProps

	Scalar *ScalarProps
}

// RelationalProps are the subset of logical properties that are computed for
// relational expressions that return rows and columns rather than scalar
// values.
type RelationalProps struct {
	// OutputCols is the set of columns that can be projected by the
	// expression. Ordering, naming, and duplication of columns is not
	// representable by this property; those are physical properties.
	OutputCols opt.ColSet

	// NotNullCols is the subset of output columns which cannot be NULL.
	// The NULL-ability of columns flows from the inputs and can also be
	// derived from filters that are NULL-intolerant.
	NotNullCols opt.ColSet
}

// ScalarProps are the subset of logical properties that are computed for
// scalar expressions that return primitive-valued types.
type ScalarProps struct {
	// Type is the data type of the scalar expression (int, string, etc).
	Type types.T
}

func (p *LogicalProps) format(mem *memo, tp treeprinter.Node) {
	if p.Relational != nil {
		p.formatOutputCols(mem, tp)
	} else {
		tp.Child(fmt.Sprintf("type: %s", p.Scalar.Type))
	}
}

func (p *LogicalProps) formatOutputCols(mem *memo, tp treeprinter.Node) {
	if !p.Relational.OutputCols.Empty() {
		var buf bytes.Buffer
		buf.WriteString("columns:")
		p.Relational.OutputCols.ForEach(func(i int) {
			p.formatCol(mem, &buf, opt.ColumnIndex(i))
		})
		tp.Child(buf.String())
	}
}

func (p *LogicalProps) formatCol(mem *memo, buf *bytes.Buffer, colIndex opt.ColumnIndex) {
	label := mem.metadata.ColumnLabel(colIndex)
	typ := mem.metadata.ColumnType(colIndex)
	buf.WriteByte(' ')
	buf.WriteString(label)
	buf.WriteByte(':')
	buf.WriteString(typ.String())
	buf.WriteByte(':')
	if !p.Relational.NotNullCols.Contains(int(colIndex)) {
		buf.WriteString("null:")
	}
	fmt.Fprintf(buf, "%d", colIndex)
}
