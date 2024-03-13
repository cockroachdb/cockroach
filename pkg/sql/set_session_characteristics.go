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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func (p *planner) SetSessionCharacteristics(
	ctx context.Context, n *tree.SetSessionCharacteristics,
) (planNode, error) {
	originalLevel := n.Modes.Isolation
	upgradedLevel := false
	upgradedDueToLicense := false
	allowReadCommitted := allowReadCommittedIsolation.Get(&p.execCfg.Settings.SV)
	allowSnapshot := allowSnapshotIsolation.Get(&p.execCfg.Settings.SV)
	hasLicense := base.CCLDistributionAndEnterpriseEnabled(p.ExecCfg().Settings)
	if err := p.sessionDataMutatorIterator.applyOnEachMutatorError(func(m sessionDataMutator) error {
		// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... '.
		switch n.Modes.Isolation {
		case tree.UnspecifiedIsolation:
		// Nothing to do.
		case tree.ReadUncommittedIsolation:
			upgradedLevel = true
			fallthrough
		case tree.ReadCommittedIsolation:
			level := tree.SerializableIsolation
			if allowReadCommitted && hasLicense {
				level = tree.ReadCommittedIsolation
			} else {
				upgradedLevel = true
				if allowReadCommitted && !hasLicense {
					upgradedDueToLicense = true
				}
			}
			m.SetDefaultTransactionIsolationLevel(level)
		case tree.RepeatableReadIsolation:
			upgradedLevel = true
			fallthrough
		case tree.SnapshotIsolation:
			level := tree.SerializableIsolation
			if allowSnapshot && hasLicense {
				level = tree.SnapshotIsolation
			} else {
				upgradedLevel = true
				if allowSnapshot && !hasLicense {
					upgradedDueToLicense = true
				}
			}
			m.SetDefaultTransactionIsolationLevel(level)
		default:
			m.SetDefaultTransactionIsolationLevel(n.Modes.Isolation)
		}

		// Note: We also support SET DEFAULT_TRANSACTION_PRIORITY TO ' .... '.
		switch n.Modes.UserPriority {
		case tree.UnspecifiedUserPriority:
		default:
			m.SetDefaultTransactionPriority(n.Modes.UserPriority)
		}

		// Note: We also support SET DEFAULT_TRANSACTION_READ_ONLY TO ' .... '.
		switch n.Modes.ReadWriteMode {
		case tree.ReadOnly:
			m.SetDefaultTransactionReadOnly(true)
		case tree.ReadWrite:
			m.SetDefaultTransactionReadOnly(false)
		case tree.UnspecifiedReadWriteMode:
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue,
				"unsupported default read write mode: %s", n.Modes.ReadWriteMode)
		}

		// Note: We also support SET DEFAULT_TRANSACTION_USE_FOLLOWER_READS TO ' .... '.
		//
		// TODO(nvanbenschoten): now that we have a way to set follower_read_timestamp()
		// as the default AS OF SYSTEM TIME value, do we need a way to unset it using
		// the same SET SESSION CHARACTERISTICS AS TRANSACTION mechanism? Currently, the
		// way to do this is SET DEFAULT_TRANSACTION_USE_FOLLOWER_READS TO FALSE;
		if n.Modes.AsOf.Expr != nil {
			if asof.IsFollowerReadTimestampFunction(ctx, n.Modes.AsOf, p.semaCtx.SearchPath) {
				m.SetDefaultTransactionUseFollowerReads(true)
			} else {
				return pgerror.Newf(pgcode.InvalidParameterValue,
					"unsupported default as of system time expression, only %s() allowed",
					asof.FollowerReadTimestampFunctionName)
			}
		}
		return nil
	}); err != nil {
		return nil, err
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

	if f := p.sessionDataMutatorIterator.upgradedIsolationLevel; upgradedLevel && f != nil {
		f(ctx, originalLevel, upgradedDueToLicense)
	}

	return newZeroNode(nil /* columns */), nil
}
