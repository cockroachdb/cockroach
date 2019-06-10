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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func (p *planner) SetSessionCharacteristics(n *tree.SetSessionCharacteristics) (planNode, error) {
	// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... ' above.
	// Ensure both versions stay in sync.
	switch n.Modes.Isolation {
	case tree.SerializableIsolation, tree.UnspecifiedIsolation:
		// Do nothing. All transactions execute with serializable isolation.
	default:
		return nil, fmt.Errorf("unsupported default isolation level: %s", n.Modes.Isolation)
	}

	switch n.Modes.ReadWriteMode {
	case tree.ReadOnly:
		p.sessionDataMutator.SetDefaultReadOnly(true)
	case tree.ReadWrite:
		p.sessionDataMutator.SetDefaultReadOnly(false)
	case tree.UnspecifiedReadWriteMode:
	default:
		return nil, fmt.Errorf("unsupported default read write mode: %s", n.Modes.ReadWriteMode)
	}

	switch n.Modes.UserPriority {
	case tree.UnspecifiedUserPriority:
	default:
		return nil, unimplemented.New("default transaction priority",
			"unsupported session default: transaction priority")
	}
	return newZeroNode(nil /* columns */), nil
}
