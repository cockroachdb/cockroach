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

		//	DISCARD TEMP
		err := deleteTempTables(ctx, p)
		if err != nil {
			return nil, err
		}

	case tree.DiscardModeTemp:
		err := deleteTempTables(ctx, p)
		if err != nil {
			return nil, err
		}

	default:
		return nil, errors.AssertionFailedf("unknown mode for DISCARD: %d", s.Mode)
	}
	return newZeroNode(nil /* columns */), nil
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

func deleteTempTables(ctx context.Context, p *planner) error {
	codec := p.execCfg.Codec
	descCol := p.Descriptors()
	allDbDescs, err := descCol.GetAllDatabaseDescriptors(ctx, p.Txn())
	if err != nil {
		return err
	}
	ie := p.execCfg.InternalExecutor

	for _, dbDesc := range allDbDescs {
		schemaName := p.TemporarySchemaName()
		err = cleanupSchemaObjects(ctx, p.execCfg.Settings, p.Txn(), descCol, codec, ie, dbDesc, schemaName)
		if err != nil {
			return err
		}
	}
	return nil
}
