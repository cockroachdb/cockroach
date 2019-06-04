// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// SetTransaction sets a transaction's isolation level, priority, ro/rw state,
// and as of timestamp.
func (p *planner) SetTransaction(n *tree.SetTransaction) (planNode, error) {
	var asOfTs hlc.Timestamp
	if n.Modes.AsOf.Expr != nil {
		var err error
		asOfTs, err = p.EvalAsOfTimestamp(n.Modes.AsOf)
		if err != nil {
			return nil, err
		}
		p.semaCtx.AsOfTimestamp = &asOfTs
	}

	if err := p.extendedEvalCtx.TxnModesSetter.setTransactionModes(n.Modes, asOfTs); err != nil {
		return nil, err
	}
	return newZeroNode(nil /* columns */), nil
}
