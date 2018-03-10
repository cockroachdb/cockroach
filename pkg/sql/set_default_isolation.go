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
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
)

func (p *planner) SetSessionCharacteristics(n *tree.SetSessionCharacteristics) (planNode, error) {
	// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... ' above.
	// Ensure both versions stay in sync.
	switch n.Modes.Isolation {
	case tree.SerializableIsolation:
		p.sessionDataMutator.SetDefaultIsolationLevel(enginepb.SERIALIZABLE)
	case tree.SnapshotIsolation:
		p.sessionDataMutator.SetDefaultIsolationLevel(enginepb.SNAPSHOT)
	case tree.UnspecifiedIsolation:
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
		return nil, pgerror.Unimplemented("default transaction priority",
			"unsupported session default: transaction priority")
	}
	return newZeroNode(nil /* columns */), nil
}
