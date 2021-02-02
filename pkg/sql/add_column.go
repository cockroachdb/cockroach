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
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// addColumnImpl performs the logic of adding a column within an ALTER TABLE.
func (p *planner) addColumnImpl(
	params runParams,
	n *alterTableNode,
	tn *tree.TableName,
	desc *tabledesc.Mutable,
	t *tree.AlterTableAddColumn,
	sessionData *sessiondata.SessionData,
) error {
	d := t.ColumnDef
	version := params.ExecCfg().Settings.Version.ActiveVersionOrEmpty(params.ctx)
	toType, err := tree.ResolveType(params.ctx, d.Type, params.p.semaCtx.GetTypeResolver())
	if err != nil {
		return err
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

	newDef, seqDbDesc, seqName, seqOpts, err := params.p.processSerialInColumnDef(params.ctx, d, tn)
	if err != nil {
		return err
	}
	if seqName != nil {
		if err := doCreateSequence(
			params,
			n.n.String(),
			seqDbDesc,
			n.tableDesc.GetParentSchemaID(),
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

	// If the new column has a DEFAULT expression that uses a sequence, add references between
	// its descriptor and this column descriptor.
	if d.HasDefaultExpr() {
		changedSeqDescs, err := maybeAddSequenceDependencies(
			params.ctx, params.p, n.tableDesc, col, expr, nil,
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
		if d.IsVirtual() && !sessionData.VirtualColumnsEnabled {
			return unimplemented.NewWithIssue(57608, "virtual computed columns")
		}
		computedColValidator := schemaexpr.MakeComputedColumnValidator(
			params.ctx,
			n.tableDesc,
			&params.p.semaCtx,
			tn,
		)
		serializedExpr, err := computedColValidator.Validate(d)
		if err != nil {
			return err
		}
		col.ComputeExpr = &serializedExpr
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
