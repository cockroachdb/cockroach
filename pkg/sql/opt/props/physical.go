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

package props

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
)

// Physical properties are interesting characteristics of an expression that
// impact its layout, presentation, or location, but not its logical content.
// Examples include row order, column naming, and data distribution (physical
// location of data ranges). Physical properties exist outside of the relational
// algebra, and arise from both the SQL query itself (e.g. the non-relational
// ORDER BY operator) and by the selection of specific implementations during
// optimization (e.g. a merge join requires the inputs to be sorted in a
// particular order).
//
// Physical properties can be provided by an operator or required of it. Some
// operators "naturally" provide a physical property such as ordering on a
// particular column. Other operators require one or more of their operands to
// provide a particular physical property. When an expression is optimized, it
// is always with respect to a particular set of required physical properties.
// The goal is to find the lowest cost expression that provides those
// properties while still remaining logically equivalent.
type Physical struct {
	// Presentation specifies the naming, membership (including duplicates),
	// and order of result columns. If Presentation is not defined, then no
	// particular column presentation is required or provided.
	Presentation Presentation

	// Ordering specifies the sort order of result rows. Rows can be sorted by
	// one or more columns, each of which can be sorted in either ascending or
	// descending order. If Ordering is not defined, then no particular ordering
	// is required or provided.
	Ordering Ordering
}

// Defined returns true if any physical property is defined. If none is
// defined, then this is an instance of MinPhysProps.
func (p *Physical) Defined() bool {
	return p.Presentation.Defined() || p.Ordering.Defined()
}

// FormatString writes physical properties to a human-readable format.
func (p *Physical) FormatString(verbose bool) string {
	hasProjection := p.Presentation.Defined()
	hasOrdering := p.Ordering.Defined()

	// Handle empty properties case.
	if !hasProjection && !hasOrdering {
		return ""
	}

	var buf bytes.Buffer

	if hasProjection {
		if verbose {
			buf.WriteString("[presentation: ")
			p.Presentation.format(&buf)
			buf.WriteByte(']')
		} else {
			buf.WriteString("p:")
			p.Presentation.format(&buf)
		}

		if hasOrdering {
			buf.WriteString(" ")
		}
	}

	if hasOrdering {
		if verbose {
			buf.WriteString("[ordering: ")
			p.Ordering.format(&buf)
			buf.WriteByte(']')
		} else {
			buf.WriteString("o:")
			p.Ordering.format(&buf)
		}
	}

	return buf.String()
}

// Fingerprint returns a string that uniquely describes this set of physical
// properties. It is suitable for use as a hash key in a map.
func (p *Physical) Fingerprint() string {
	return p.FormatString(false /* verbose */)
}

func (p *Physical) String() string {
	return p.FormatString(true /* verbose */)
}

// Equals returns true if the two physical properties are identical.
func (p *Physical) Equals(rhs *Physical) bool {
	return p.Presentation.Equals(rhs.Presentation) && p.Ordering.Equals(rhs.Ordering)
}

// Presentation specifies the naming, membership (including duplicates), and
// order of result columns that are required of or provided by an operator.
// While it cannot add unique columns, Presentation can rename, reorder,
// duplicate and discard columns. If Presentation is not defined, then no
// particular column presentation is required or provided. For example:
//   a.y:2 a.x:1 a.y:2 column1:3
type Presentation []opt.LabeledColumn

// Defined is true if a particular column presentation is required or provided.
func (p Presentation) Defined() bool {
	return p != nil
}

// Equals returns true iff this presentation exactly matches the given
// presentation.
func (p Presentation) Equals(rhs Presentation) bool {
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
		fmt.Fprintf(buf, "%s:%d", col.Label, col.ID)
	}
}

// Ordering defines the order of rows provided or required by an operator. A
// negative value indicates descending order on the column id "-(value)".
type Ordering []opt.OrderingColumn

// Defined is true if a particular row ordering is required or provided.
func (o Ordering) Defined() bool {
	return len(o) != 0
}

func (o Ordering) String() string {
	var buf bytes.Buffer
	o.format(&buf)
	return buf.String()
}

func (o Ordering) format(buf *bytes.Buffer) {
	for i, col := range o {
		if i > 0 {
			buf.WriteString(",")
		}
		if col.Descending() {
			buf.WriteByte('-')
		} else {
			buf.WriteByte('+')
		}
		fmt.Fprintf(buf, "%d", col.ID())
	}
}

// ColSet returns the set of column IDs used in the ordering.
func (o Ordering) ColSet() opt.ColSet {
	var colSet opt.ColSet
	for _, col := range o {
		colSet.Add(int(col.ID()))
	}
	return colSet
}

// Provides returns true if the required ordering is a prefix of this ordering.
func (o Ordering) Provides(required Ordering) bool {
	if len(o) < len(required) {
		return false
	}

	for i := range required {
		if o[i] != required[i] {
			return false
		}
	}
	return true
}

// Equals returns true if the two orderings are identical.
func (o Ordering) Equals(rhs Ordering) bool {
	if len(o) != len(rhs) {
		return false
	}

	for i := range o {
		if o[i] != rhs[i] {
			return false
		}
	}
	return true
}
