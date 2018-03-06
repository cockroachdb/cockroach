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

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
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

	// OuterCols is the set of columns that are referenced by variables within
	// this relational sub-expression, but are not bound within the scope of
	// the expression. For example:
	//
	//   SELECT *
	//   FROM a
	//   WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x AND b.y = 5)
	//
	// For the inner SELECT expression, a.x is an outer column, meaning that it
	// is defined "outside" the SELECT expression (hence the name "outer"). The
	// SELECT expression binds the b.x and b.y references, so they are not
	// part of the outer column set. The outer SELECT binds the a.x column, and
	// so its outer column set is empty.
	//
	// TODO(andyk): populate this when we have subquery support
	OuterCols opt.ColSet
}

// ScalarProps are the subset of logical properties that are computed for
// scalar expressions that return primitive-valued types.
type ScalarProps struct {
	// Type is the data type of the scalar expression (int, string, etc).
	Type types.T

	// OuterCols is the set of columns that are referenced by variables within
	// this scalar sub-expression, but are not bound within the scope of the
	// expression. For example:
	//
	//   SELECT *
	//   FROM a
	//   WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x AND b.y = 5)
	//
	// For the EXISTS expression, only a.x is an outer column, meaning that
	// only it is defined "outside" the EXISTS expression (hence the name
	// "outer"). Note that what constitutes an "outer column" is dependent on
	// an expression's location in the query. For example, while the b.x and
	// b.y columns are not outer columns on the EXISTS expression, they *are*
	// outer columns on the inner WHERE condition.
	OuterCols opt.ColSet
}

func (p *LogicalProps) formatOutputCols(md *opt.Metadata, tp treeprinter.Node) {
	p.formatColSet("columns:", p.Relational.OutputCols, md, tp)
}

func (p *LogicalProps) formatColSet(
	heading string, colSet opt.ColSet, md *opt.Metadata, tp treeprinter.Node,
) {
	if !colSet.Empty() {
		var buf bytes.Buffer
		buf.WriteString(heading)
		colSet.ForEach(func(i int) {
			p.formatCol("", opt.ColumnIndex(i), md, &buf)
		})
		tp.Child(buf.String())
	}
}

func (p *LogicalProps) formatColList(
	heading string, colList opt.ColList, md *opt.Metadata, tp treeprinter.Node,
) {
	if len(colList) > 0 {
		var buf bytes.Buffer
		buf.WriteString(heading)
		for _, col := range colList {
			p.formatCol("", col, md, &buf)
		}
		tp.Child(buf.String())
	}
}

func (p *LogicalProps) formatCol(
	label string, index opt.ColumnIndex, md *opt.Metadata, buf *bytes.Buffer,
) {
	if label == "" {
		label = md.ColumnLabel(index)
	}
	typ := md.ColumnType(index)
	buf.WriteByte(' ')
	buf.WriteString(label)
	buf.WriteByte(':')
	fmt.Fprintf(buf, "%d", index)
	buf.WriteByte('(')
	buf.WriteString(typ.String())
	if p.Relational.NotNullCols.Contains(int(index)) {
		buf.WriteString("!null")
	}
	buf.WriteByte(')')
}
