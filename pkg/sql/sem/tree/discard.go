// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

// Discard represents a DISCARD statement.
type Discard struct {
	Mode DiscardMode
}

var _ Statement = &Discard{}

// DiscardMode is an enum of the various discard modes.
type DiscardMode int

const (
	// DiscardModeAll represents a DISCARD ALL statement.
	DiscardModeAll DiscardMode = iota

	// DiscardModeSequences represents a DISCARD SEQUENCES statement
	DiscardModeSequences

	// DiscardModeTemp represents a DISCARD TEMPORARY statement
	DiscardModeTemp
)

// Format implements the NodeFormatter interface.
func (node *Discard) Format(ctx *FmtCtx) {
	switch node.Mode {
	case DiscardModeAll:
		ctx.WriteString("DISCARD ALL")
	case DiscardModeSequences:
		ctx.WriteString("DISCARD SEQUENCES")
	case DiscardModeTemp:
		ctx.WriteString("DISCARD TEMPORARY")
	}
}

// String implements the Statement interface.
func (node *Discard) String() string {
	return AsString(node)
}
