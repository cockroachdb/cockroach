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
	return &discardNode{mode: s.Mode}, nil
}

type discardNode struct {
	mode tree.DiscardMode
}

func (n *discardNode) Next(_ runParams) (bool, error) { return false, nil }
func (n *discardNode) Values() tree.Datums            { return nil }
func (n *discardNode) Close(_ context.Context)        {}
func (n *discardNode) startExec(params runParams) error {
	switch n.mode {
	case tree.DiscardModeAll:
		if !params.p.autoCommit {
			return pgerror.New(pgcode.ActiveSQLTransaction,
				"DISCARD ALL cannot run inside a transaction block")
		}

		// RESET ALL
		if err := params.p.sessionDataMutatorIterator.applyOnEachMutatorError(
			func(m sessionDataMutator) error {
				return resetSessionVars(params.ctx, m)
			},
		); err != nil {
			return err
		}

		// DEALLOCATE ALL
		params.p.preparedStatements.DeleteAll(params.ctx)
	default:
		return errors.AssertionFailedf("unknown mode for DISCARD: %d", n.mode)
	}
	return nil
}

func resetSessionVars(ctx context.Context, m sessionDataMutator) error {
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
