// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
)

// Format implements the NodeFormatter interface.
func (node *Discard) Format(ctx *FmtCtx) {
	switch node.Mode {
	case DiscardModeAll:
		ctx.WriteString("DISCARD ALL")
	}
}

// String implements the Statement interface.
func (node *Discard) String() string {
	return AsString(node)
}
