// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func (p *planner) Unlisten(ctx context.Context, n *tree.Unlisten) (planNode, error) {
	p.BufferClientNotice(ctx,
		pgerror.WithSeverity(
			unimplemented.NewWithIssuef(41522, "CRDB does not support LISTEN, making UNLISTEN a no-op"),
			"NOTICE",
		),
	)
	return newZeroNode(nil /* columns */), nil
}
