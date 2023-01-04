// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/seqexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func upgradeSequenceToBeReferencedByID(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	return d.InternalExecutorFactory.DescsTxnWithExecutor(ctx, d.DB, d.SessionData, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, ie sqlutil.InternalExecutor,
	) (err error) {
		var lastUpgradedID descpb.ID
		// Upgrade each table/view, one at a time, until we exhaust all of them.
		for {
			done, idToUpgrade, err := findNextTableToUpgrade(ctx, d.InternalExecutor, txn, lastUpgradedID,
				func(table *descpb.TableDescriptor) bool {
					return table.IsTable() || table.IsView()
				})
			if err != nil || done {
				return err
			}

			// Table/View `idToUpgrade` might contain reference to sequences by name. If so, we need to upgrade
			// those references to be by ID.
			err = maybeUpgradeSeqReferencesInTableOrView(ctx, idToUpgrade, d)
			if err != nil {
				return err
			}
			lastUpgradedID = idToUpgrade
		}
	})
}

// Find the next table descriptor ID that is > `lastUpgradedID`
// and satisfy the `tableSelector`.
// If no such ID exists, `done` will be true.
func findNextTableToUpgrade(
	ctx context.Context,
	ie sqlutil.InternalExecutor,
	txn *kv.Txn,
	lastUpgradedID descpb.ID,
	tableSelector func(table *descpb.TableDescriptor) bool,
) (done bool, idToUpgrade descpb.ID, err error) {
	var rows sqlutil.InternalRows
	rows, err = ie.QueryIterator(ctx, "upgrade-seq-find-desc", txn,
		`SELECT id, descriptor, crdb_internal_mvcc_timestamp FROM system.descriptor WHERE id > $1 ORDER BY ID ASC`, lastUpgradedID)
	if err != nil {
		return false, 0, err
	}
	defer func() { _ = rows.Close() }()

	var ok bool
	for ok, err = rows.Next(ctx); ok; ok, err = rows.Next(ctx) {
		row := rows.Cur()
		id := descpb.ID(tree.MustBeDInt(row[0]))
		ts, err := hlc.DecimalToHLC(&row[2].(*tree.DDecimal).Decimal)
		if err != nil {
			return false, 0, errors.Wrapf(err,
				"failed to convert MVCC timestamp decimal to HLC for ID %d", id)
		}
		b, err := descbuilder.FromBytesAndMVCCTimestamp([]byte(tree.MustBeDBytes(row[1])), ts)
		if err != nil {
			return false, 0, errors.Wrapf(err,
				"failed to unmarshal descriptor with ID %d", id)
		}
		// Return this descriptor if it's a non-dropped table or view.
		if b != nil && b.DescriptorType() == catalog.Table {
			tableDesc := b.BuildImmutable().(catalog.TableDescriptor)
			if tableDesc != nil && !tableDesc.Dropped() && tableSelector(tableDesc.TableDesc()) {
				return false, id, nil
			}
		}
	}

	// Break out of the above loop either because we exhausted rows or an error occurred.
	if err != nil {
		return false, 0, err
	}
	return true, 0, nil
}

// maybeUpgradeSeqReferencesInTableOrView updates descriptor `idToUpgrade` if it references any sequences by name.
func maybeUpgradeSeqReferencesInTableOrView(
	ctx context.Context, idToUpgrade descpb.ID, d upgrade.TenantDeps,
) error {
	return d.InternalExecutorFactory.DescsTxn(ctx, d.DB, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection,
	) error {
		// Set up: retrieve table desc for `idToUpgrade` and a schema resolver
		tableDesc, sc, cleanup, err := upgradeSetUpForTableOrView(ctx, d, txn, descriptors, idToUpgrade)
		if err != nil {
			return err
		}
		defer cleanup()

		// Act: upgrade the table's (or view's) sequence references accordingly.
		if tableDesc.IsTable() {
			if err = upgradeSequenceReferenceInTable(ctx, txn, tableDesc, sc, descriptors); err != nil {
				return err
			}
		} else if tableDesc.IsView() {
			if err = upgradeSequenceReferenceInView(ctx, txn, tableDesc, sc, descriptors); err != nil {
				return err
			}
		} else {
			return errors.Errorf("Expect table or view desc to upgrade sequence references; "+
				"Got %v", tableDesc.String())
		}
		return nil
	})
}

// upgradeSetUpForTableOrView upgrades the given table or view (with id `idToUpgrade`) to reference sequence from
// by name to by ID, if any.
func upgradeSetUpForTableOrView(
	ctx context.Context,
	d upgrade.TenantDeps,
	txn *kv.Txn,
	descriptors *descs.Collection,
	idToUpgrade descpb.ID,
) (*tabledesc.Mutable, resolver.SchemaResolver, func(), error) {
	// Get the table descriptor that we are going to upgrade.
	tableDesc, err := descriptors.ByID(txn).Mutable().Table(ctx, idToUpgrade)
	if err != nil {
		return nil, nil, nil, err
	}

	// Get the database of the table to pass to the planner constructor.
	dbDesc, err := descriptors.ByID(txn).WithoutNonPublic().Immutable().Database(ctx, tableDesc.GetParentID())
	if err != nil {
		return nil, nil, nil, err
	}

	// Construct an internal planner
	sc, cleanup, err := d.SchemaResolverConstructor(txn, descriptors, dbDesc.GetName())
	if err != nil {
		return nil, nil, nil, err
	}

	return tableDesc, sc, cleanup, nil
}

// upgradeSequenceReferenceInTable upgrade sequence reference from by name to by ID in table `tableDesc`.
func upgradeSequenceReferenceInTable(
	ctx context.Context,
	txn *kv.Txn,
	tableDesc *tabledesc.Mutable,
	sc resolver.SchemaResolver,
	descriptors *descs.Collection,
) error {
	// 'changedSeqDescs' stores all sequences that are referenced by name from 'tableDesc'. They will soon
	// be upgraded to be referenced by ID, so we store them here so that we can later schedule schema changes to
	// them, after their backreference's ByID are set to true.
	var changedSeqDescs []*tabledesc.Mutable

	// a function that will modify `expr` in-place if it contains any sequence reference by name (to by ID).
	maybeUpgradeSeqReferencesInExpr := func(expr *string) error {
		parsedExpr, err := parser.ParseExpr(*expr)
		if err != nil {
			return err
		}
		seqIdentifiers, err := seqexpr.GetUsedSequences(parsedExpr)
		if err != nil {
			return err
		}
		if len(seqIdentifiers) == 0 {
			return nil
		}

		seqNameToID, err := maybeUpdateBackRefsAndBuildMap(ctx, sc, tableDesc, seqIdentifiers, &changedSeqDescs)
		if err != nil {
			return err
		}

		// Perform the sequence replacement in the default expression.
		newExpr, err := seqexpr.ReplaceSequenceNamesWithIDs(parsedExpr, seqNameToID)
		if err != nil {
			return err
		}

		// Modify the input `expr` in-place.
		*expr = tree.Serialize(newExpr)
		return nil
	}

	// Check each column's DEFAULT and ON UPDATE expression and update sequence references if any.
	for _, column := range tableDesc.Columns {
		if column.HasDefault() {
			if err := maybeUpgradeSeqReferencesInExpr(column.DefaultExpr); err != nil {
				return err
			}
		}
		if column.HasOnUpdate() {
			if err := maybeUpgradeSeqReferencesInExpr(column.OnUpdateExpr); err != nil {
				return err
			}
		}
	}

	// Write the schema change for all referenced sequence descriptors.
	for _, changedSeqDesc := range changedSeqDescs {
		if err := descriptors.WriteDesc(ctx, false, changedSeqDesc, txn); err != nil {
			return err
		}
	}

	// Write the schema change for the table.
	return descriptors.WriteDesc(ctx, false /* kvTrace */, tableDesc, txn)
}

// upgradeSequenceReferenceInView similarly upgrade sequence reference from by name to by ID in view `viewDesc`.
func upgradeSequenceReferenceInView(
	ctx context.Context,
	txn *kv.Txn,
	viewDesc *tabledesc.Mutable,
	sc resolver.SchemaResolver,
	descriptors *descs.Collection,
) error {
	var changedSeqDescs []*tabledesc.Mutable
	replaceSeqFunc := func(expr tree.Expr) (recurse bool, newExpr tree.Expr, err error) {
		seqIdentifiers, err := seqexpr.GetUsedSequences(expr)
		if err != nil {
			return false, expr, err
		}
		seqNameToID, err := maybeUpdateBackRefsAndBuildMap(ctx, sc, viewDesc, seqIdentifiers, &changedSeqDescs)
		if err != nil {
			return false, expr, err
		}

		newExpr, err = seqexpr.ReplaceSequenceNamesWithIDs(expr, seqNameToID)
		if err != nil {
			return false, expr, err
		}
		return false, newExpr, nil
	}

	stmt, err := parser.ParseOne(viewDesc.ViewQuery)
	if err != nil {
		return err
	}
	newStmt, err := tree.SimpleStmtVisit(stmt.AST, replaceSeqFunc)
	if err != nil {
		return err
	}
	viewDesc.ViewQuery = newStmt.String()

	// Write the schema change for all referenced sequence descriptors.
	for _, changedSeqDesc := range changedSeqDescs {
		if err := descriptors.WriteDesc(ctx, false, changedSeqDesc, txn); err != nil {
			return err
		}
	}

	// Write the schema change for updated view descriptor.
	return descriptors.WriteDesc(ctx, false /* kvTrace */, viewDesc, txn)
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
	seqIdentifiers []seqexpr.SeqIdentifier,
	changedSeqDescs *[]*tabledesc.Mutable,
) (map[string]descpb.ID, error) {
	seqNameToID := make(map[string]descpb.ID)
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
		seqNameToID[seqDesc.GetName()] = seqDesc.ID
	}

	return seqNameToID, nil
}
