// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

func (p *planner) SetSessionAuthorizationDefault() (planNode, error) {
	// This is currently a no-op - we don't support changing the session
	// authorization, and the parser only accepts DEFAULT.
	return newZeroNode(nil /* columns */), nil
}
