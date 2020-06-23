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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// addColumnImpl performs the logic of adding a column within an ALTER TABLE.
func (p *planner) addColumnImpl(
	params runParams,
	n *alterTableNode,
	tn *tree.TableName,
	desc *sqlbase.MutableTableDescriptor,
	t *tree.AlterTableAddColumn,
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
			n.tableDesc.Temporary,
			seqOpts,
			tree.AsStringWithFQNames(n.n, params.Ann()),
		); err != nil {
			return err
		}
	}
	d = newDef
	incTelemetryForNewColumn(d)

	col, idx, expr, err := sqlbase.MakeColumnDefDescs(params.ctx, d, &params.p.semaCtx, params.EvalContext())
	if err != nil {
		return err
	}
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
				params.ctx, changedSeqDesc, sqlbase.InvalidMutationID, tree.AsStringWithFQNames(n.n, params.Ann()),
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
			return sqlbase.NewNonNullViolationError(col.Name)
		}
	}
	_, err = n.tableDesc.FindActiveColumnByName(string(d.Name))
	if m := n.tableDesc.FindColumnMutationByName(d.Name); m != nil {
		switch m.Direction {
		case sqlbase.DescriptorMutation_ADD:
			return pgerror.Newf(pgcode.DuplicateColumn,
				"duplicate: column %q in the middle of being added, not yet public",
				col.Name)
		case sqlbase.DescriptorMutation_DROP:
			return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"column %q being dropped, try again later", col.Name)
		default:
			if err != nil {
				return errors.AssertionFailedf(
					"mutation in state %s, direction %s, and no column descriptor",
					errors.Safe(m.State), errors.Safe(m.Direction))
			}
		}
	}
	if err == nil {
		if t.IfNotExists {
			return nil
		}
		return sqlbase.NewColumnAlreadyExistsError(string(d.Name), n.tableDesc.Name)
	}

	n.tableDesc.AddColumnMutation(col, sqlbase.DescriptorMutation_ADD)
	if idx != nil {
		if err := n.tableDesc.AddIndexMutation(idx, sqlbase.DescriptorMutation_ADD); err != nil {
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
		computedColValidator := schemaexpr.NewComputedColumnValidator(
			params.ctx,
			n.tableDesc,
			&params.p.semaCtx,
			tn,
		)
		if err := computedColValidator.Validate(d); err != nil {
			return err
		}
	}

	return nil
}
