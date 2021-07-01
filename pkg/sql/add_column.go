// Copyright 2020 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// addColumnImpl performs the logic of adding a column within an ALTER TABLE.
func (p *planner) addColumnImpl(
	params runParams,
	n *alterTableNode,
	tn *tree.TableName,
	desc *tabledesc.Mutable,
	t *tree.AlterTableAddColumn,
) error {
	d := t.ColumnDef

	if d.IsComputed() {
		d.Computed.Expr = schemaexpr.MaybeRewriteComputedColumn(d.Computed.Expr, params.SessionData())
	}

	version := params.ExecCfg().Settings.Version.ActiveVersionOrEmpty(params.ctx)
	toType, err := tree.ResolveType(params.ctx, d.Type, params.p.semaCtx.GetTypeResolver())
	if err != nil {
		return err
	}
	switch toType.Oid() {
	case oid.T_int2vector, oid.T_oidvector:
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"VECTOR column types are unsupported",
		)
	}
	if supported, err := isTypeSupportedInVersion(version, toType); err != nil {
		return err
	} else if !supported {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"type %s is not supported until version upgrade is finalized",
			toType.SQLString(),
		)
	}

	newDef, seqPrefix, seqName, seqOpts, err := params.p.processSerialInColumnDef(params.ctx, d, tn)
	if err != nil {
		return err
	}
	if seqName != nil {
		if err := doCreateSequence(
			params,
			seqPrefix.Database,
			seqPrefix.Schema,
			seqName,
			n.tableDesc.Persistence(),
			seqOpts,
			tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}
	}
	d = newDef

	col, idx, expr, err := tabledesc.MakeColumnDefDescs(params.ctx, d, &params.p.semaCtx, params.EvalContext())
	if err != nil {
		return err
	}
	incTelemetryForNewColumn(d, col)

	// Ensure all new indexes are partitioned appropriately.
	if idx != nil {
		if n.tableDesc.IsLocalityRegionalByRow() {
			if err := params.p.checkNoRegionChangeUnderway(
				params.ctx,
				n.tableDesc.GetParentID(),
				"add an UNIQUE COLUMN on a REGIONAL BY ROW table",
			); err != nil {
				return err
			}
		}

		*idx, err = p.configureIndexDescForNewIndexPartitioning(
			params.ctx,
			desc,
			*idx,
			nil, /* PartitionByIndex */
		)
		if err != nil {
			return err
		}
	}

	// If the new column has a DEFAULT expression that uses a sequence, add references between
	// its descriptor and this column descriptor.
	if d.HasDefaultExpr() {
		changedSeqDescs, err := maybeAddSequenceDependencies(
			params.ctx, params.ExecCfg().Settings, params.p, n.tableDesc, col, expr, nil,
		)
		if err != nil {
			return err
		}
		for _, changedSeqDesc := range changedSeqDescs {
			if err := params.p.writeSchemaChange(
				params.ctx, changedSeqDesc, descpb.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
			); err != nil {
				return err
			}
		}
	}

	// We're checking to see if a user is trying add a non-nullable column without a default to a
	// non empty table by scanning the primary index span with a limit of 1 to see if any key exists.
	if !col.Nullable && (col.DefaultExpr == nil && !col.IsComputed()) {
		span := n.tableDesc.PrimaryIndexSpan(params.ExecCfg().Codec)
		kvs, err := params.p.txn.Scan(params.ctx, span.Key, span.EndKey, 1)
		if err != nil {
			return err
		}
		if len(kvs) > 0 {
			return sqlerrors.NewNonNullViolationError(col.Name)
		}
	}
	if isPublic, err := checkColumnDoesNotExist(n.tableDesc, d.Name); err != nil {
		if isPublic && t.IfNotExists {
			return nil
		}
		return err
	}

	n.tableDesc.AddColumnMutation(col, descpb.DescriptorMutation_ADD)
	if idx != nil {
		if err := n.tableDesc.AddIndexMutation(idx, descpb.DescriptorMutation_ADD); err != nil {
			return err
		}
	}
	if d.HasColumnFamily() {
		err := n.tableDesc.AddColumnToFamilyMaybeCreate(
			col.Name, string(d.Family.Name), d.Family.Create,
			d.Family.IfNotExists)
		if err != nil {
			return err
		}
	}

	if d.IsComputed() {
		serializedExpr, _, err := schemaexpr.ValidateComputedColumnExpression(
			params.ctx, n.tableDesc, d, tn, "computed column", params.p.SemaCtx(),
		)
		if err != nil {
			return err
		}
		col.ComputeExpr = &serializedExpr
	}

	if !col.Virtual {
		// Add non-virtual column name and ID to primary index.
		primaryIndex := n.tableDesc.GetPrimaryIndex().IndexDescDeepCopy()
		primaryIndex.StoreColumnNames = append(primaryIndex.StoreColumnNames, col.Name)
		primaryIndex.StoreColumnIDs = append(primaryIndex.StoreColumnIDs, col.ID)
		n.tableDesc.SetPrimaryIndex(primaryIndex)
	}

	// Zone configuration logic is only required for REGIONAL BY ROW tables
	// with newly created indexes.
	if n.tableDesc.IsLocalityRegionalByRow() && idx != nil {
		// We need to allocate new IDs for the created columns and indexes
		// in case we need to configure their zone partitioning.
		// This must be done after every object is created.
		if err := n.tableDesc.AllocateIDs(params.ctx); err != nil {
			return err
		}

		// Configure zone configuration if required. This must happen after
		// all the IDs have been allocated.
		if err := p.configureZoneConfigForNewIndexPartitioning(
			params.ctx,
			n.tableDesc,
			*idx,
		); err != nil {
			return err
		}
	}

	return nil
}

func checkColumnDoesNotExist(
	tableDesc catalog.TableDescriptor, name tree.Name,
) (isPublic bool, err error) {
	col, _ := tableDesc.FindColumnWithName(name)
	if col == nil {
		return false, nil
	}
	if col.IsSystemColumn() {
		return false, pgerror.Newf(pgcode.DuplicateColumn,
			"column name %q conflicts with a system column name",
			col.GetName())
	}
	if col.Public() {
		return true, sqlerrors.NewColumnAlreadyExistsError(tree.ErrString(&name), tableDesc.GetName())
	}
	if col.Adding() {
		return false, pgerror.Newf(pgcode.DuplicateColumn,
			"duplicate: column %q in the middle of being added, not yet public",
			col.GetName())
	}
	if col.Dropped() {
		return false, pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"column %q being dropped, try again later", col.GetName())
	}
	return false, errors.AssertionFailedf("mutation in direction NONE")
}
