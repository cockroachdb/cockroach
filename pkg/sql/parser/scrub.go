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

package parser

import (
	"bytes"
	"fmt"
)

// ScrubType describes the SCRUB statement operation.
type ScrubType int

const (
	// ScrubTable describes the SCRUB operation SCRUB TABLE
	ScrubTable = iota
)

// Scrub represents a SCRUB TABLE statement.
type Scrub struct {
	Typ     ScrubType
	Table   NormalizableTableName
	Options ScrubOptions
}

// Format implements the NodeFormatter interface.
func (n *Scrub) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("EXPERIMENTAL SCRUB ")
	switch n.Typ {
	case ScrubTable:
		buf.WriteString("TABLE ")
		n.Table.Format(buf, f)
	default:
		panic("Unhandled ScrubType")
	}

	if len(n.Options) > 0 {
		buf.WriteString(" WITH ")
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

// ScrubOption representts a scrub option.
type ScrubOption interface {
	fmt.Stringer
	NodeFormatter

	scrubOptionType()
}

func (*ScrubOptionIndex) scrubOptionType() {}

// ScrubOptionIndex represents an INDEX scrub check.
type ScrubOptionIndex struct {
	IndexNames NameList
}

// Format implements the NodeFormatter interface.
func (n *ScrubOptionIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("INDEX")
	if n.IndexNames != nil {
		buf.WriteString("(")
		n.IndexNames.Format(buf, f)
		buf.WriteString(")")
	}
}

func (n *ScrubOptionIndex) String() string { return AsString(n) }
