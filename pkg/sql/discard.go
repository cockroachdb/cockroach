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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// Discard implements the DISCARD statement.
// See https://www.postgresql.org/docs/9.6/static/sql-discard.html for details.
func (p *planner) Discard(ctx context.Context, s *tree.Discard) (planNode, error) {
	switch s.Mode {
	case tree.DiscardModeAll:
		if !p.autoCommit {
			return nil, pgerror.New(pgcode.ActiveSQLTransaction,
				"DISCARD ALL cannot run inside a transaction block")
		}

		// RESET ALL
		if err := p.sessionDataMutatorIterator.applyOnEachMutatorError(
			func(m sessionDataMutator) error {
				return resetSessionVars(ctx, m)
			},
		); err != nil {
			return nil, err
		}

		// DEALLOCATE ALL
		p.preparedStatements.DeleteAll(ctx)
	default:
		return nil, errors.AssertionFailedf("unknown mode for DISCARD: %d", s.Mode)
	}
	return newZeroNode(nil /* columns */), nil
}

func resetSessionVars(ctx context.Context, m sessionDataMutator) error {
	// Always do intervalstyle_enabled and datestyle_enabled first so that
	// IntervalStyle and DateStyle which depend on these flags are correctly
	// configured.
	if err := resetSessionVar(ctx, m, "datestyle_enabled"); err != nil {
		return err
	}
	if err := resetSessionVar(ctx, m, "intervalstyle_enabled"); err != nil {
		return err
	}
	for _, varName := range varNames {
		if err := resetSessionVar(ctx, m, varName); err != nil {
			return err
		}
	}
	return nil
}

func resetSessionVar(ctx context.Context, m sessionDataMutator, varName string) error {
	v := varGen[varName]
	if v.Set != nil {
		hasDefault, defVal := getSessionVarDefaultString(varName, v, m.sessionDataMutatorBase)
		if hasDefault {
			if err := v.Set(ctx, m, defVal); err != nil {
				return err
			}
		}
	}
	return nil
}
