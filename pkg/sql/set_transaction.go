// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// SetTransaction sets a transaction's isolation level, priority, ro/rw state,
// and as of timestamp.
func (p *planner) SetTransaction(ctx context.Context, n *tree.SetTransaction) (planNode, error) {
	var asOfTs hlc.Timestamp
	if n.Modes.AsOf.Expr != nil {
		var err error
		asOfTs, err = p.EvalAsOfTimestamp(ctx, n.Modes.AsOf)
		if err != nil {
			return nil, err
		}
		p.semaCtx.AsOfTimestamp = &asOfTs
	}
	if n.Modes.Deferrable == tree.Deferrable {
		return nil, unimplemented.NewWithIssue(53432, "DEFERRABLE transactions")
	}

	if err := p.extendedEvalCtx.TxnModesSetter.setTransactionModes(n.Modes, asOfTs); err != nil {
		return nil, err
	}
	return newZeroNode(nil /* columns */), nil
}
