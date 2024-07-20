// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// DropTrigger drops a trigger.
func (p *planner) DropTrigger(ctx context.Context, n *tree.DropTrigger) (ret planNode, err error) {
	return nil, unimplemented.NewWithIssue(126359, "DROP TRIGGER")
}
