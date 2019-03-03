// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
