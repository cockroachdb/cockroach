// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scpb

import "github.com/cockroachdb/cockroach/pkg/util/protoutil"

// SourceElementID elements ID's for identifying parent elements.
// This ID is dynamically allocated when any parent element is
// created and has no relation to the descriptor ID.
type SourceElementID uint32

// ElementMetadata contains materialized metadata for an element,
// where references inside the TargetMetadata are resolved to
// their actual values. This structure is mainly used during opgen
// where we need to know these values to emit event log entries for
// example.
type ElementMetadata struct {
	TargetMetadata
	Username  string
	AppName   string
	Statement string
}

// Clone clones a State and any associated  metadata (i.e. statement and
//authorization
// information) for that state.
func (s *State) Clone() State {
	clone := State{
		Nodes:      make([]*Node, len(s.Nodes)),
		Statements: make([]*Statement, len(s.Statements)),
	}
	for i, n := range s.Nodes {
		clone.Nodes[i] = &Node{
			Target: protoutil.Clone(n.Target).(*Target),
			Status: n.Status,
		}
	}
	for i, n := range s.Statements {
		clone.Statements[i] = &Statement{
			Statement: n.Statement,
		}
	}
	clone.Authorization.Username = s.Authorization.Username
	clone.Authorization.AppName = s.Authorization.AppName
	return clone
}
