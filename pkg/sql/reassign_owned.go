// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// ReassignOwnedNode represents a REASSIGN OWNED BY <name> TO <name> statement.
type reassignOwnedNode struct {
	// TODO(angelaw): to implement
}

func (p *planner) ReassignOwned(ctx context.Context, n *tree.ReassignOwned) (planNode, error) {
	return nil, nil // TODO(angelaw): to implement
}
