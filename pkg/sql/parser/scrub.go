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
)

// ScrubType describes the SCRUB statement operation.
type ScrubType int

const (
	// ScrubTable describes the SCRUB operation SCRUB TABLE
	ScrubTable = iota
)

// Scrub represents a SCRUB TABLE statement.
type Scrub struct {
	Typ   ScrubType
	Table Expr
}

// Format implements the NodeFormatter interface.
func (n *Scrub) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteString("EXPERIMENTAL SCRUB ")
	switch n.Typ {
	case ScrubTable:
		buf.WriteString("TABLE ")
		buf.WriteString(n.Table.String())
	default:
		panic("Unhandled ScrubType")
	}
}
