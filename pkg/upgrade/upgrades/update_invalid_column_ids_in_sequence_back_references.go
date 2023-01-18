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
	"reflect"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

func updateInvalidColumnIDsInSequenceBackReferences(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// Scan all sequences and repair those with 0-valued column IDs in their back references,
	// one transaction for each repair.
	var lastSeqID descpb.ID

	for {
		var currSeqID descpb.ID
		var done bool
		if err := d.DB.DescsTxn(ctx, func(
			ctx context.Context, txn descs.Txn,
		) (err error) {
			currSeqID = lastSeqID
			for {
				done, currSeqID, err = findNextTableToUpgrade(ctx, txn, txn.KV(), currSeqID,
					func(table *descpb.TableDescriptor) bool {
						return table.IsSequence()
					})
				if err != nil || done {
					return err
				}

				// Sequence `nextIdToUpgrade` might contain back reference with invalid column IDs. If so, we need to
				// update them with valid column IDs.
				hasUpgrade, err := maybeUpdateInvalidColumnIdsInSequenceBackReferences(
					ctx, txn.KV(), currSeqID, txn.Descriptors(),
				)
				if err != nil {
					return err
				}
				if hasUpgrade {
					return nil
				}
			}
		}); err != nil {
			return err
		}

		// Break out of the loop if we upgraded all sequences.
		if done {
			break
		}
		lastSeqID = currSeqID
	}

	return nil
}

// maybeUpdateInvalidColumnIdsInSequenceBackReferences looks at sequence, identified
// by `idToUpgrade`, and upgrade invalid column IDs in its back references, if any.
//
// The upgrade works by reconstructing the referenced column IDs in each back reference
// in the sequence's `DependedOnBy` field. Reconstruction is possible with the help of
// the `UsesSequenceIDs` field of each column in the referencing table.
//
// A canonical example to create a corrupt sequence descriptor with invalid column IDs is
// to run the following in v21.1:
// `CREATE SEQUENCE s; CREATE TABLE t (i INT PRIMARY KEY);`
// followed by
// `ALTER TABLE t ADD COLUMN j INT DEFAULT nextval('s'), ADD COLUMN k INT DEFAULT nextval('s')`
// which erroneously added the back reference in sequence `s` before allocating a column ID
// to the newly added column, causing the reference in `s.DependedOnBy` to be
// `ref.ColumnIDs = [0, 0]` while it should instead be `ref.ColumnIDs = [2, 3]`.
// This upgrade logic will look at the `UsesSequenceIds` field of each column (`i`, `j`, and `k`)
// in `t`. In this case, exactly two columns (`j` and `k`) will have `UsesSequenceIds` that
// contains id of `s`. We thus reconstruct and update `ref.ColumnIDs` to be `[2, 3]`.
func maybeUpdateInvalidColumnIdsInSequenceBackReferences(
	ctx context.Context, txn *kv.Txn, idToUpgrade descpb.ID, descriptors *descs.Collection,
) (hasUpgraded bool, err error) {
	// Get the sequence descriptor that we are going to upgrade.
	seqDesc, err := descriptors.MutableByID(txn).Table(ctx, idToUpgrade)
	if err != nil {
		return false, err
	}
	if !seqDesc.IsSequence() {
		return false, errors.AssertionFailedf("input id to upgrade %v is is not a sequence", idToUpgrade)
	}

	for i, ref := range seqDesc.DependedOnBy {
		// Re-construct the expected column IDs in `ref` and update
		// `ref.ColumnIDs` if the actual value is not equal to the
		// expected value.
		expectedColumnIDsInRef := make([]descpb.ColumnID, 0)
		tableDesc, err := descriptors.MutableByID(txn).Table(ctx, ref.ID)
		if err != nil {
			return false, err
		}
		for _, col := range tableDesc.GetColumns() {
			if descpb.IDs(col.UsesSequenceIds).Contains(seqDesc.ID) {
				expectedColumnIDsInRef = append(expectedColumnIDsInRef, col.ID)
			}
		}
		sort.Slice(expectedColumnIDsInRef, func(i, j int) bool {
			return expectedColumnIDsInRef[i] < expectedColumnIDsInRef[j]
		})

		if !reflect.DeepEqual(ref.ColumnIDs, expectedColumnIDsInRef) {
			seqDesc.DependedOnBy[i].ColumnIDs = expectedColumnIDsInRef
			hasUpgraded = true
		}
	}

	if hasUpgraded {
		// Write the updated sequence descriptor to storage.
		log.Infof(ctx, "updated invalid column IDs in back references in sequence %v (%v)",
			seqDesc.Name, idToUpgrade)
		if err = descriptors.WriteDesc(ctx, false, seqDesc, txn); err != nil {
			return false, err
		}
	}

	return hasUpgraded, err
}
