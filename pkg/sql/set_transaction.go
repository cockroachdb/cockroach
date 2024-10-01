// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// SetTransaction sets a transaction's isolation level, priority, ro/rw state,
// and as of timestamp.
func (p *planner) SetTransaction(ctx context.Context, n *tree.SetTransaction) (planNode, error) {
	var asOfTs hlc.Timestamp
	if n.Modes.AsOf.Expr != nil {
		asOf, err := p.EvalAsOfTimestamp(ctx, n.Modes.AsOf)
		if err != nil {
			return nil, err
		}
		p.extendedEvalCtx.AsOfSystemTime = &asOf
		asOfTs = asOf.Timestamp
	}
	if n.Modes.Deferrable == tree.Deferrable {
		return nil, unimplemented.NewWithIssue(53432, "DEFERRABLE transactions")
	}

	if err := p.extendedEvalCtx.TxnModesSetter.setTransactionModes(ctx, n.Modes, asOfTs); err != nil {
		return nil, err
	}
	return newZeroNode(nil /* columns */), nil
}
