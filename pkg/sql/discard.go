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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
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

		// SET SESSION AUTHORIZATION DEFAULT
		if err := params.p.setRole(params.ctx, false /* local */, params.p.SessionData().SessionUser()); err != nil {
			return err
		}

		// RESET ALL
		if err := params.p.resetAllSessionVars(params.ctx); err != nil {

			return err
		}

		// DEALLOCATE ALL
		params.p.preparedStatements.DeleteAll(params.ctx)

		// DISCARD SEQUENCES
		params.p.sessionDataMutatorIterator.applyOnEachMutator(func(m sessionDataMutator) {
			m.data.SequenceState = sessiondata.NewSequenceState()
			m.initSequenceCache()
		})

		// DISCARD TEMP
		err := deleteTempTables(params.ctx, params.p)
		if err != nil {
			return err
		}

	case tree.DiscardModeSequences:
		params.p.sessionDataMutatorIterator.applyOnEachMutator(func(m sessionDataMutator) {
			m.data.SequenceState = sessiondata.NewSequenceState()
			m.initSequenceCache()
		})
	case tree.DiscardModeTemp:
		err := deleteTempTables(params.ctx, params.p)
		if err != nil {
			return err
		}
	default:
		return errors.AssertionFailedf("unknown mode for DISCARD: %d", n.mode)
	}
	return nil
}

func deleteTempTables(ctx context.Context, p *planner) error {
	// If this session has no temp schemas, then there is nothing to do here.
	// This is the common case.
	if len(p.SessionData().DatabaseIDToTempSchemaID) == 0 {
		return nil
	}
	return p.WithInternalExecutor(ctx, func(ctx context.Context, txn *kv.Txn, ie sqlutil.InternalExecutor) error {
		codec := p.execCfg.Codec
		descCol := p.Descriptors()
		allDbDescs, err := descCol.GetAllDatabaseDescriptors(ctx, p.Txn())
		if err != nil {
			return err
		}
		for _, dbDesc := range allDbDescs {
			schemaName := p.TemporarySchemaName()
			err = cleanupSchemaObjects(ctx, p.Txn(), descCol, codec, ie, dbDesc, schemaName)
			if err != nil {
				return err
			}
		}
		return nil
	})
}
