// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/embedding"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/vectorizer"
	"github.com/cockroachdb/errors"
)

type dropVectorizerNode struct {
	zeroInputPlanNode
	n         *tree.DropVectorizer
	tableDesc *tabledesc.Mutable
	tableName tree.TableName
}

// DropVectorizer removes the automatic embedding generator from a table.
func (p *planner) DropVectorizer(ctx context.Context, n *tree.DropVectorizer) (planNode, error) {
	if !embedding.VectorizationEnabled.Get(&p.ExecCfg().Settings.SV) {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"vectorization is disabled; enable it with "+
				"SET CLUSTER SETTING sql.vectorize.enabled = true")
	}

	if err := checkSchemaChangeEnabled(
		ctx, p.ExecCfg(), "DROP VECTORIZER",
	); err != nil {
		return nil, err
	}

	tn := n.TableName.ToTableName()
	_, tableDesc, err := p.ResolveMutableTableDescriptor(
		ctx, &tn, !n.IfExists, tree.ResolveRequireTableDesc,
	)
	if err != nil {
		return nil, err
	}
	if tableDesc == nil {
		// IF EXISTS and table not found.
		return newZeroNode(nil /* columns */), nil
	}

	// Check ownership.
	hasOwnership, err := p.HasOwnership(ctx, tableDesc)
	if err != nil {
		return nil, err
	}
	if !hasOwnership {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of table %s", tree.Name(tableDesc.GetName()))
	}

	// Check that a vectorizer is configured.
	if tableDesc.Vectorizer == nil {
		if n.IfExists {
			return newZeroNode(nil /* columns */), nil
		}
		return nil, pgerror.Newf(pgcode.UndefinedObject,
			"table %s does not have a vectorizer configured",
			tree.Name(tableDesc.GetName()))
	}

	return &dropVectorizerNode{
		n:         n,
		tableDesc: tableDesc,
		tableName: tn,
	}, nil
}

func (n *dropVectorizerNode) startExec(params runParams) error {
	ctx := params.ctx
	p := params.p

	// Drop the companion view first (depends on the companion table).
	viewName := vectorizer.CompanionViewName(n.tableName)
	dropViewSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName.String())
	if _, err := p.InternalSQLTxn().ExecEx(
		ctx, "drop-vectorizer-companion-view", p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		dropViewSQL,
	); err != nil {
		return errors.Wrap(err, "dropping companion embeddings view")
	}

	// Drop the companion table.
	companionName := vectorizer.CompanionTableName(n.tableName)
	dropTableSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", companionName.String())
	if _, err := p.InternalSQLTxn().ExecEx(
		ctx, "drop-vectorizer-companion-table", p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		dropTableSQL,
	); err != nil {
		return errors.Wrap(err, "dropping companion embeddings table")
	}

	// Delete the vectorizer schedule if one exists.
	if schedID := n.tableDesc.Vectorizer.ScheduleID; schedID != 0 {
		storage := jobs.ScheduledJobTxn(p.InternalSQLTxn())
		env := jobs.JobSchedulerEnv(p.ExecCfg().JobsKnobs())
		if err := storage.DeleteByID(
			ctx, env, schedID,
		); err != nil {
			// Log but don't fail — the schedule may already be gone.
			params.p.BufferClientNotice(ctx, errors.WithHintf(
				errors.Newf("could not delete vectorizer schedule %d", schedID),
				"manually delete with: DROP SCHEDULE %d", schedID,
			))
		}
	}

	// Clear the Vectorizer config from the source table descriptor.
	n.tableDesc.Vectorizer = nil

	return p.writeSchemaChange(
		ctx, n.tableDesc, descpb.InvalidMutationID,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
}

func (n *dropVectorizerNode) Next(runParams) (bool, error) { return false, nil }
func (n *dropVectorizerNode) Values() tree.Datums          { return tree.Datums{} }
func (n *dropVectorizerNode) Close(context.Context)        {}
