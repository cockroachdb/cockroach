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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) SetSessionCharacteristics(n *tree.SetSessionCharacteristics) (planNode, error) {
	// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... '.
	switch n.Modes.Isolation {
	case tree.SerializableIsolation, tree.UnspecifiedIsolation:
		// Do nothing. All transactions execute with serializable isolation.
	default:
		return nil, fmt.Errorf("unsupported default isolation level: %s", n.Modes.Isolation)
	}

	// Note: We also support SET DEFAULT_TRANSACTION_PRIORITY TO ' .... '.
	switch n.Modes.UserPriority {
	case tree.UnspecifiedUserPriority:
	default:
		p.sessionDataMutator.SetDefaultTransactionPriority(n.Modes.UserPriority)
	}

	// Note: We also support SET DEFAULT_TRANSACTION_READ_ONLY TO ' .... '.
	switch n.Modes.ReadWriteMode {
	case tree.ReadOnly:
		p.sessionDataMutator.SetDefaultReadOnly(true)
	case tree.ReadWrite:
		p.sessionDataMutator.SetDefaultReadOnly(false)
	case tree.UnspecifiedReadWriteMode:
	default:
		return nil, fmt.Errorf("unsupported default read write mode: %s", n.Modes.ReadWriteMode)
	}
	return newZeroNode(nil /* columns */), nil
}
