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
	"sort"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

func updateInvalidColumnIDsInSequenceBackReferences(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	// Upgrade each sequence, one at a time, until we exhaust all of them.
	return d.CollectionFactory.TxnWithExecutor(ctx, d.DB, d.SessionData, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, ie sqlutil.InternalExecutor,
	) error {
		var lastUpgradedID descpb.ID
		for {
			done, idToUpgrade, err := findNextTableToUpgrade(ctx, ie, lastUpgradedID,
				func(table *descpb.TableDescriptor) bool {
					return table.IsSequence()
				})
			if err != nil || done {
				return err
			}

			// Sequence `idToUpgrade` might contain back reference with invalid column IDs. If so, we need to
			// update them with valid column IDs.
			err = maybeUpdateInvalidColumnIdsInSequenceBackReferencesBestEffort(ctx, txn, idToUpgrade, descriptors)
			if err != nil {
				return err
			}

			lastUpgradedID = idToUpgrade
		}
	})
}

// maybeUpdateInvalidColumnIdsInSequenceBackReferencesBestEffort looks at sequence, identified
// by `idToUpgrade`, and upgrade invalid column IDs in its back references, if any,
// on a best-effort basis.
// The upgrade works by taking all invalid column IDs in a back reference and go look
// for the same number of columns in the referencing table that uses this sequence. If
// this is possible, then we update those invalid column IDs with those valid column IDs
// we found. If, somehow, we failed to do so, we will skip this troubled reference and
// leave a warning in log.
//
// A canonical example to create a corrupt sequence descriptor with invalid column IDs is
// to run the following in v21.1:
// `CREATE SEQUENCE s; CREATE TABLE t (i INT PRIMARY KEY);`
// followed by
// `ALTER TABLE t ADD COLUMN j INT DEFAULT nextval('s'), ADD COLUMN k INT DEFAULT nextval('s')`
// which erroneously added the back reference in sequence `s` before allocating a column ID
// to the newly added column, causing the reference in `s.DependedOnBy` to be
// `ref.ColumnIDs = [0, 0]` while it should instead be `ref.ColumnIDs = [2, 3]`.
// This upgrade logic will identify those two invalid 0-valued column IDs and look at the
// `UsesSequenceIds` field of each column (`i`, `j`, and `k`) in `t`. In this case, exactly
// two columns (`j` and `k`) will have `UsesSequenceIds` that contains id of `s`. We thus
// conclude a matching and can go update `ref.ColumnIDs` to be `[2, 3]`.
func maybeUpdateInvalidColumnIdsInSequenceBackReferencesBestEffort(
	ctx context.Context, txn *kv.Txn, idToUpgrade descpb.ID, descriptors *descs.Collection,
) error {
	// Get the sequence descriptor that we are going to upgrade.
	seqDesc, err := descriptors.GetMutableTableByID(ctx, txn, idToUpgrade, tree.ObjectLookupFlagsWithRequired())
	if err != nil {
		return err
	}
	if !seqDesc.IsSequence() {
		return errors.AssertionFailedf("input id to upgrade %v is is not a sequence", idToUpgrade)
	}

	anyUpdate := false
	for i, ref := range seqDesc.DependedOnBy {
		if !catalog.MakeTableColSet(ref.ColumnIDs...).Contains(0) {
			continue
		}

		// This back reference contains 0-valued column ID(s), go to
		// the referencing table to reconstruct `ref.ColumnIds`.
		updatedColumnIDsInRef := make([]descpb.ColumnID, 0)
		tableDesc, err := descriptors.GetMutableTableByID(ctx, txn, ref.ID, tree.ObjectLookupFlagsWithRequired())
		if err != nil {
			return err
		}
		for _, col := range tableDesc.GetColumns() {
			if descpb.IDs(col.UsesSequenceIds).Contains(seqDesc.ID) {
				updatedColumnIDsInRef = append(updatedColumnIDsInRef, col.ID)
			}
		}

		// Finally, update `ref.ColumnIds`.
		sort.Slice(updatedColumnIDsInRef, func(i, j int) bool {
			return updatedColumnIDsInRef[i] < updatedColumnIDsInRef[j]
		})
		seqDesc.DependedOnBy[i].ColumnIDs = updatedColumnIDsInRef
		anyUpdate = true
	}

	if anyUpdate {
		// Write the updated sequence descriptor to storage.
		if err = descriptors.WriteDesc(ctx, false, seqDesc, txn); err != nil {
			return err
		}
	}

	return nil

}
