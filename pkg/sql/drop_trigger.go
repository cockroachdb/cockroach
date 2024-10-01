// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// DropTrigger drops a trigger.
func (p *planner) DropTrigger(ctx context.Context, n *tree.DropTrigger) (ret planNode, err error) {
	return nil, unimplemented.NewWithIssue(126359, "DROP TRIGGER")
}
