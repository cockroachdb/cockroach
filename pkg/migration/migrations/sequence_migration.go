// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/sequence"
	"github.com/cockroachdb/errors"
)

func sequenceMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.SQLDeps,
) error {
	var lastUpgradedID descpb.ID
	for {
		done, idToUpgrade, err := findNextDescriptorToUpdate(
			ctx, d.InternalExecutor, lastUpgradedID, false /* isFKUpgrade */)
		if err != nil || done {
			return err
		}
		if err := upgradeSequenceRepresentation(ctx, idToUpgrade, d); err != nil {
			return err
		}
		lastUpgradedID = idToUpgrade
	}
}

func setup(
	ctx context.Context,
	txn *kv.Txn,
	descriptors *descs.Collection,
	upgrade descpb.ID,
	d migration.SQLDeps,
) (*tabledesc.Mutable, resolver.SchemaResolver, func(), error) {
	t, err := descriptors.GetMutableTableByID(ctx, txn, upgrade, tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		return nil, nil, nil, err
	}

	// Get the database of the table to pass to the planner constructor.
	_, dbDesc, err := descriptors.GetImmutableDatabaseByID(
		ctx, txn, t.GetParentID(), tree.DatabaseLookupFlags{Required: true})
	if err != nil {
		return nil, nil, nil, err
	}

	planner, cleanup := d.InternalPlannerConstructor(txn, descriptors, dbDesc.Name)
	sc, ok := planner.(resolver.SchemaResolver)
	if !ok {
		cleanup()
		return nil, nil, nil, errors.New("expected SchemaResolver")
	}
	return t, sc, cleanup, nil
}

func upgradeSequenceRepresentation(
	ctx context.Context, upgrade descpb.ID, d migration.SQLDeps,
) error {
	return descs.Txn(ctx, d.Settings, d.LeaseManager, d.InternalExecutor, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		t, sc, cleanup, err := setup(ctx, txn, descriptors, upgrade, d)
		if err != nil {
			return err
		}
		defer cleanup()

		if t.IsView() {
			if err := upgradeSequenceRepresentationInView(
				ctx, txn, descriptors, sc, t,
			); err != nil {
				return err
			}
		} else {
			if err := upgradeSequenceRepresentationInDefaultExpr(
				ctx, txn, descriptors, sc, t,
			); err != nil {
				return err
			}
		}
		return nil
	})
}

func upgradeSequenceRepresentationInDefaultExpr(
	ctx context.Context,
	txn *kv.Txn,
	descriptors *descs.Collection,
	sc resolver.SchemaResolver,
	t *tabledesc.Mutable,
) error {
	var changedSeqDescs []*tabledesc.Mutable
	for i := range t.Columns {
		column := &t.Columns[i]
		if column.HasDefault() {
			defaultExpr, err := parser.ParseExpr(*column.DefaultExpr)
			if err != nil {
				return err
			}
			seqIdentifiers, err := sequence.GetUsedSequences(defaultExpr)
			if err != nil {
				return err
			}
			if len(seqIdentifiers) == 0 {
				continue
			}

			seqNameToID, err := maybeUpdateBackRefsAndBuildMap(ctx, sc, t, seqIdentifiers, &changedSeqDescs)
			if err != nil {
				return err
			}

			// Perform the sequence replacement in the default expression.
			newExpr, err := sequence.ReplaceSequenceNamesWithIDs(defaultExpr, seqNameToID)
			if err != nil {
				return err
			}
			s := tree.Serialize(newExpr)
			column.DefaultExpr = &s
		}
	}

	// Write the schema change for all referenced sequence descriptors.
	for _, changedSeqDesc := range changedSeqDescs {
		if err := descriptors.WriteDesc(ctx, false, changedSeqDesc, txn); err != nil {
			return err
		}
	}

	// Update the table's default expressions.
	return descriptors.WriteDesc(ctx, false /* kvTrace */, t, txn)
}

func upgradeSequenceRepresentationInView(
	ctx context.Context,
	txn *kv.Txn,
	descriptors *descs.Collection,
	sc resolver.SchemaResolver,
	v *tabledesc.Mutable,
) error {
	var changedSeqDescs []*tabledesc.Mutable
	replaceSeqFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		seqIdentifiers, err := sequence.GetUsedSequences(expr)
		if err != nil {
			return false, expr, err
		}
		seqNameToID, err := maybeUpdateBackRefsAndBuildMap(ctx, sc, v, seqIdentifiers, &changedSeqDescs)
		if err != nil {
			return false, expr, err
		}

		newExpr, err = sequence.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
		if err != nil {
			return false, expr, err
		}
		return false, newExpr, nil
	}

	stmt, err := parser.ParseOne(v.ViewQuery)
	if err != nil {
		return err
	}
	newStmt, err := tree.SimpleStmtVisit(stmt.AST, replaceSeqFunc)
	if err != nil {
		return err
	}
	v.ViewQuery = newStmt.String()

	// Write the schema change for all referenced sequence descriptors.
	for _, changedSeqDesc := range changedSeqDescs {
		if err := descriptors.WriteDesc(ctx, false, changedSeqDesc, txn); err != nil {
			return err
		}
	}

	// Update the view query.
	return descriptors.WriteDesc(ctx, false /* kvTrace */, v, txn)
}

// maybeUpdateBackRefsAndBuildMap iterates over all the sequence identifiers
// and the table that contains them, checks if they're referenced by ID.
// If not, it will update the back reference to reflect that it will now be
// stored by ID. It also builds a mapping of sequence names mapped to their IDs,
// and accumulates a list of changed descriptors.
func maybeUpdateBackRefsAndBuildMap(
	ctx context.Context,
	sc resolver.SchemaResolver,
	t *tabledesc.Mutable,
	seqIdentifiers []sequence.SeqIdentifier,
	changedSeqDescs *[]*tabledesc.Mutable,
) (map[string]int64, error) {
	seqNameToID := make(map[string]int64)
	for _, seqIdentifier := range seqIdentifiers {
		seqDesc, err := sql.GetSequenceDescFromIdentifier(ctx, sc, seqIdentifier)
		if err != nil {
			return nil, err
		}

		// Get all the indexes of all references to the sequence by this table.
		var refIdxs []int
		for i, reference := range seqDesc.DependedOnBy {
			if reference.ID == t.ID {
				refIdxs = append(refIdxs, i)
			}
		}

		// Check if we're already referencing the sequence by ID. If so, skip.
		// If not, update the back reference to reflect that it's now by ID.
		for _, refIdx := range refIdxs {
			if seqDesc.DependedOnBy[refIdx].ByID {
				continue
			} else {
				seqDesc.DependedOnBy[refIdx].ByID = true
				*changedSeqDescs = append(*changedSeqDescs, seqDesc)
			}
		}
		seqNameToID[seqIdentifier.SeqName] = int64(seqDesc.ID)
	}

	return seqNameToID, nil
}
