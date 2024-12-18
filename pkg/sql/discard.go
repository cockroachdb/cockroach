// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// Discard implements the DISCARD statement.
// See https://www.postgresql.org/docs/9.6/static/sql-discard.html for details.
func (p *planner) Discard(ctx context.Context, s *tree.Discard) (planNode, error) {
	return &discardNode{mode: s.Mode}, nil
}

type discardNode struct {
	zeroInputPlanNode
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
	codec := p.execCfg.Codec
	descCol := p.Descriptors()
	// Note: grabbing all the databases here is somewhat suspect. It appears
	// that the logic related to maintaining the set of database temp schemas
	// is somewhat incomplete, so there can be temp schemas in the sessiondata
	// map which don't exist any longer.
	allDbDescs, err := descCol.GetAllDatabaseDescriptors(ctx, p.Txn())
	if err != nil {
		return err
	}
	g := p.byNameGetterBuilder().MaybeGet()
	for _, db := range allDbDescs {
		if _, ok := p.SessionData().DatabaseIDToTempSchemaID[uint32(db.GetID())]; !ok {
			continue
		}
		sc, err := g.Schema(ctx, db, p.TemporarySchemaName())
		if err != nil {
			return err
		}
		if sc == nil {
			continue
		}
		err = cleanupTempSchemaObjects(
			ctx, p.InternalSQLTxn(), descCol, codec, db, sc,
		)
		if err != nil {
			return err
		}
	}
	return nil
}
