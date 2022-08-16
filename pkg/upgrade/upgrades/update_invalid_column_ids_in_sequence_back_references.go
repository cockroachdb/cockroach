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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func updateInvalidColumnIDsInSequenceBackReferences(
	ctx context.Context, version clusterversion.ClusterVersion, d upgrade.TenantDeps, job *jobs.Job,
) error {

	var lastUpgradedID descpb.ID
	// Upgrade each sequence, one at a time, until we exhaust all of them.
	for {
		done, idToUpgrade, err := findNextTableToUpgrade(ctx, d.InternalExecutor, lastUpgradedID,
			func(table *descpb.TableDescriptor) bool {
				return table.IsSequence()
			})
		if err != nil || done {
			return err
		}

		// Sequence `idToUpgrade` might contain back reference with invalid column IDs. If so, we need to
		// update them with valid column IDs.
		err = maybeUpdateInvalidColumnIdsInSequenceBackReferencesBestEffort(ctx, idToUpgrade, d)
		if err != nil {
			return err
		}

		lastUpgradedID = idToUpgrade
	}

}

// maybeUpdateInvalidColumnIdsInSequenceBackReferencesBestEffort looks at sequence, identified
// by `idToUpgrade`, and upgrade invalid column IDs in its back references, if any,
// on a best-effort basis.
// The upgrade works by taking all invalid column IDs in a back reference and go look
// for the same number of columns in the referencing table that uses this sequence. If
// this is possible, then we update those invalid column IDs with those valid column IDs
// we found. If, somehow, we failed to do so, we will skip this troubled reference and
// leave a warning in log.
func maybeUpdateInvalidColumnIdsInSequenceBackReferencesBestEffort(
	ctx context.Context, idToUpgrade descpb.ID, d upgrade.TenantDeps,
) error {
	return d.CollectionFactory.TxnWithExecutor(ctx, d.DB, d.SessionData, func(
		ctx context.Context, txn *kv.Txn, descriptors *descs.Collection, ie sqlutil.InternalExecutor,
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
			invalidColumnIDFreq := freqOfColumnIDIn(ref.ColumnIDs, 0)
			if invalidColumnIDFreq == 0 {
				continue
			}

			validColumnIDsInRef := catalog.MakeTableColSet(nonZeroColumnIDsIn(ref.ColumnIDs)...)
			invalidColumnIDsReplaceCandidates := make([]descpb.ColumnID, 0)

			// Since we have `invalidColumnIDFreq` invalid column IDs, we attempt to
			// find the same number of columns in the referencing table that uses
			// this sequence.
			tableDesc, err := descriptors.GetMutableTableByID(ctx, txn, ref.ID, tree.ObjectLookupFlagsWithRequired())
			if err != nil {
				return err
			}
			for _, col := range tableDesc.GetColumns() {
				if validColumnIDsInRef.Contains(col.ID) {
					// Skip columns that are properly referenced in the reference.
					continue
				}

				if descIDsContains(col.UsesSequenceIds, seqDesc.ID) {
					invalidColumnIDFreq--
					invalidColumnIDsReplaceCandidates = append(invalidColumnIDsReplaceCandidates, col.ID)
				}
			}

			if invalidColumnIDFreq != 0 {
				// Sadly, we cannot find a perfect matching for all the invalid column IDs in
				// the back reference of this sequence.
				log.Warningf(ctx, "sequence %v (%v) contains back reference %v that has invalid column ID(s) we cannot"+
					"resolve automatically. Manual intervention might be required", seqDesc.Name, seqDesc.ID, ref)
				continue
			}

			// Finally, we can update the troubled back reference by replacing those invalid
			// column IDs with the replace-candidates we found for them.
			updatedColumnIDsInRef := append(validColumnIDsInRef.Ordered(), invalidColumnIDsReplaceCandidates...)
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
	})
}

func nonZeroColumnIDsIn(slice []descpb.ColumnID) (res []descpb.ColumnID) {
	for _, elem := range slice {
		if elem != 0 {
			res = append(res, elem)
		}
	}
	return res
}

func freqOfColumnIDIn(slice []descpb.ColumnID, target descpb.ColumnID) (res int) {
	for _, elem := range slice {
		if elem == target {
			res++
		}
	}
	return res
}

func descIDsContains(slice []descpb.ID, target descpb.ID) bool {
	for _, elem := range slice {
		if elem == target {
			return true
		}
	}
	return false
}
