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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// AlterTableRegionalAffinity transforms a tree.AlterTableRegionalAffinity into a plan node.
func (p *planner) AlterTableRegionalAffinity(
	ctx context.Context, n *tree.AlterTableRegionalAffinity,
) (planNode, error) {
	return nil, unimplemented.New("alter table locality", "implementation pending")
}
