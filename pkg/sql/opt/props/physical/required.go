// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package physical

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// Required properties are interesting characteristics of an expression that
// impact its layout, presentation, or location, but not its logical content.
// Examples include row order, column naming, and data distribution (physical
// location of data ranges). Physical properties exist outside of the relational
// algebra, and arise from both the SQL query itself (e.g. the non-relational
// ORDER BY operator) and by the selection of specific implementations during
// optimization (e.g. a merge join requires the inputs to be sorted in a
// particular order).
//
// Required properties are derived top-to-bottom - there is a required physical
// property on the root, and each expression can require physical properties on
// one or more of its operands. When an expression is optimized, it is always
// with respect to a particular set of required physical properties.  The goal
// is to find the lowest cost expression that provides those properties while
// still remaining logically equivalent.
type Required struct {
	// Presentation specifies the naming, membership (including duplicates),
	// and order of result columns. If Presentation is not defined, then no
	// particular column presentation is required or provided.
	Presentation Presentation

	// Ordering specifies the sort order of result rows. Rows can be sorted by
	// one or more columns, each of which can be sorted in either ascending or
	// descending order. If Ordering is not defined, then no particular ordering
	// is required or provided.
	Ordering OrderingChoice
}

// MinRequired are the default physical properties that require nothing and
// provide nothing.
var MinRequired = &Required{}

// Defined is true if any physical property is defined. If none is defined, then
// this is an instance of MinRequired.
func (p *Required) Defined() bool {
	return !p.Presentation.Any() || !p.Ordering.Any()
}

// ColSet returns the set of columns used by any of the physical properties.
func (p *Required) ColSet() opt.ColSet {
	colSet := p.Ordering.ColSet()
	for _, col := range p.Presentation {
		colSet.Add(col.ID)
	}
	return colSet
}

func (p *Required) String() string {
	hasProjection := !p.Presentation.Any()
	hasOrdering := !p.Ordering.Any()

	// Handle empty properties case.
	if !hasProjection && !hasOrdering {
		return "[]"
	}

	var buf bytes.Buffer

	if hasProjection {
		buf.WriteString("[presentation: ")
		p.Presentation.format(&buf)
		buf.WriteByte(']')

		if hasOrdering {
			buf.WriteString(" ")
		}
	}

	if hasOrdering {
		buf.WriteString("[ordering: ")
		p.Ordering.Format(&buf)
		buf.WriteByte(']')
	}

	return buf.String()
}

// Equals returns true if the two physical properties are identical.
func (p *Required) Equals(rhs *Required) bool {
	return p.Presentation.Equals(rhs.Presentation) && p.Ordering.Equals(&rhs.Ordering)
}

// Presentation specifies the naming, membership (including duplicates), and
// order of result columns that are required of or provided by an operator.
// While it cannot add unique columns, Presentation can rename, reorder,
// duplicate and discard columns. If Presentation is not defined, then no
// particular column presentation is required or provided. For example:
//   a.y:2 a.x:1 a.y:2 column1:3
type Presentation []opt.AliasedColumn

// Any is true if any column presentation is allowed or can be provided.
func (p Presentation) Any() bool {
	return p == nil
}

// Equals returns true iff this presentation exactly matches the given
// presentation.
func (p Presentation) Equals(rhs Presentation) bool {
	// The 0 column presentation is not the same as the nil presentation.
	if p.Any() != rhs.Any() {
		return false
	}
	if len(p) != len(rhs) {
		return false
	}

	for i := 0; i < len(p); i++ {
		if p[i] != rhs[i] {
			return false
		}
	}
	return true
}

func (p Presentation) String() string {
	var buf bytes.Buffer
	p.format(&buf)
	return buf.String()
}

func (p Presentation) format(buf *bytes.Buffer) {
	for i, col := range p {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(buf, "%s:%d", col.Alias, col.ID)
	}
}
