// Copyright 2017 The Cockroach Authors.
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

package tree

import (
	"bytes"
	"fmt"
)

// ScrubType describes the SCRUB statement operation.
type ScrubType int

const (
	// ScrubTable describes the SCRUB operation SCRUB TABLE.
	ScrubTable = iota
	// ScrubDatabase describes the SCRUB operation SCRUB DATABASE.
	ScrubDatabase = iota
)

// Scrub represents a SCRUB statement.
type Scrub struct {
	Typ     ScrubType
	Options ScrubOptions
	// Table is only set during SCRUB TABLE statements.
	Table NormalizableTableName
	// Database is only set during SCRUB DATABASE statements.
	Database Name
}

// Format implements the NodeFormatter interface.
func (n *Scrub) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("EXPERIMENTAL SCRUB ")
	switch n.Typ {
	case ScrubTable:
		buf.WriteString("TABLE ")
		n.Table.Format(buf, f)
	case ScrubDatabase:
		buf.WriteString("DATABASE ")
		n.Database.Format(buf, f)
	default:
		panic("Unhandled ScrubType")
	}

	if len(n.Options) > 0 {
		buf.WriteString(" WITH OPTIONS ")
		n.Options.Format(buf, f)
	}
}

// ScrubOptions corresponds to a comma-delimited list of scrub options.
type ScrubOptions []ScrubOption

// Format implements the NodeFormatter interface.
func (n ScrubOptions) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, option := range n {
		if i > 0 {
			buf.WriteString(", ")
		}
		option.Format(buf, f)
	}
}

func (n ScrubOptions) String() string { return AsString(n) }

// ScrubOption represents a scrub option.
type ScrubOption interface {
	fmt.Stringer
	NodeFormatter

	scrubOptionType()
}

// scrubOptionType implements the ScrubOption interface
func (*ScrubOptionIndex) scrubOptionType()      {}
func (*ScrubOptionPhysical) scrubOptionType()   {}
func (*ScrubOptionConstraint) scrubOptionType() {}

func (n *ScrubOptionIndex) String() string      { return AsString(n) }
func (n *ScrubOptionPhysical) String() string   { return AsString(n) }
func (n *ScrubOptionConstraint) String() string { return AsString(n) }

// ScrubOptionIndex represents an INDEX scrub check.
type ScrubOptionIndex struct {
	IndexNames NameList
}

// Format implements the NodeFormatter interface.
func (n *ScrubOptionIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("INDEX ")
	if n.IndexNames != nil {
		buf.WriteByte('(')
		n.IndexNames.Format(buf, f)
		buf.WriteByte(')')
	} else {
		buf.WriteString("ALL")
	}
}

// ScrubOptionPhysical represents a PHYSICAL scrub check.
type ScrubOptionPhysical struct{}

// Format implements the NodeFormatter interface.
func (n *ScrubOptionPhysical) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("PHYSICAL")
}

// ScrubOptionConstraint represents a CONSTRAINT scrub check.
type ScrubOptionConstraint struct {
	ConstraintNames NameList
}

// Format implements the NodeFormatter interface.
func (n *ScrubOptionConstraint) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("CONSTRAINT ")
	if n.ConstraintNames != nil {
		buf.WriteByte('(')
		n.ConstraintNames.Format(buf, f)
		buf.WriteByte(')')
	} else {
		buf.WriteString("ALL")
	}
}
