// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	allowReadCommitted := allowReadCommittedIsolation.Get(&p.execCfg.Settings.SV)
	allowRepeatableRead := allowRepeatableReadIsolation.Get(&p.execCfg.Settings.SV)
	hasLicense := base.CCLDistributionAndEnterpriseEnabled(p.ExecCfg().Settings)
	if err := p.sessionDataMutatorIterator.applyOnEachMutatorError(func(m sessionDataMutator) error {
		// Note: We also support SET DEFAULT_TRANSACTION_ISOLATION TO ' .... '.
		if n.Modes.Isolation != tree.UnspecifiedIsolation {
			level, upgraded, upgradedDueToLicense := n.Modes.Isolation.UpgradeToEnabledLevel(
				allowReadCommitted, allowRepeatableRead, hasLicense)
			if f := p.sessionDataMutatorIterator.upgradedIsolationLevel; upgraded && f != nil {
				f(ctx, n.Modes.Isolation, upgradedDueToLicense)
			}
			m.SetDefaultTransactionIsolationLevel(level)
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

	return newZeroNode(nil /* columns */), nil
}
