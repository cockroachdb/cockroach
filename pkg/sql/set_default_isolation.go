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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func (p *planner) SetSessionCharacteristics(n *tree.SetSessionCharacteristics) (planNode, error) {
	// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... '.
	switch n.Modes.Isolation {
	case tree.SerializableIsolation, tree.UnspecifiedIsolation:
		// Do nothing. All transactions execute with serializable isolation.
	default:
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"unsupported default isolation level: %s", n.Modes.Isolation)
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
		p.sessionDataMutator.SetDefaultTransactionReadOnly(true)
	case tree.ReadWrite:
		p.sessionDataMutator.SetDefaultTransactionReadOnly(false)
	case tree.UnspecifiedReadWriteMode:
	default:
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"unsupported default read write mode: %s", n.Modes.ReadWriteMode)
	}

	// Note: We also support SET DEFAULT_TRANSACTION_USE_FOLLOWER_READS TO ' .... '.
	//
	// TODO(nvanbenschoten): now that we have a way to set follower_read_timestamp()
	// as the default AS OF SYSTEM TIME value, do we need a way to unset it using
	// the same SET SESSION CHARACTERISTICS AS TRANSACTION mechanism? Currently, the
	// way to do this is SET DEFAULT_TRANSACTION_USE_FOLLOWER_READS TO FALSE;
	if n.Modes.AsOf.Expr != nil {
		if tree.IsFollowerReadTimestampFunction(n.Modes.AsOf, p.semaCtx.SearchPath) {
			p.sessionDataMutator.SetDefaultTransactionUseFollowerReads(true)
		} else {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue,
				"unsupported default as of system time expression, only %s() allowed",
				tree.FollowerReadTimestampFunctionName)
		}
	}

	// Note: We do not support SET DEFAULT_TRANSACTION_DEFERRABLE TO ' ... '.
	switch n.Modes.Deferrable {
	case tree.NotDeferrable, tree.UnspecifiedDeferrableMode:
		// Do nothing. All transactions execute in a NOT DEFERRABLE mode.
	case tree.Deferrable:
		return nil, unimplemented.NewWithIssue(53432, "DEFERRABLE transactions")
	default:
		return nil, pgerror.Newf(pgcode.InvalidParameterValue,
			"unsupported default deferrable mode: %s", n.Modes.Deferrable)
	}

	return newZeroNode(nil /* columns */), nil
}
