// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) ReparentDatabase(
	ctx context.Context, n *tree.ReparentDatabase,
) (planNode, error) {
	return nil, pgerror.Newf(pgcode.FeatureNotSupported, "cannot perform ALTER DATABASE CONVERT TO SCHEMA")
}
