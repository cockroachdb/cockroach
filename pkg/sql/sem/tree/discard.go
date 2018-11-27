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
