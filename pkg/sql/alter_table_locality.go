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

// AlterTableLocality transforms a tree.AlterTableLocality into a plan node.
func (p *planner) AlterTableLocality(
	ctx context.Context, n *tree.AlterTableLocality,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		&p.ExecCfg().Settings.SV,
		"ALTER TABLE",
	); err != nil {
		return nil, err
	}
	return nil, unimplemented.New("alter table locality", "implementation pending")
}
