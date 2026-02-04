// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// repairTriggerBackrefs repairs trigger backrefs that have TriggerID=0.
// Previously, trigger dependencies in DependedOnBy had TriggerID=0, making them
// indistinguishable from view dependencies. This upgrade ensures each trigger
// backref has the correct TriggerID set.
func repairTriggerBackrefs(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	// First, get all database descriptors.
	var databases nstree.Catalog
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) (err error) {
		databases, err = txn.Descriptors().GetAllDatabases(ctx, txn.KV())
		return err
	}); err != nil {
		return err
	}

	// Process each database separately to avoid loading all descriptors at once.
	if err := databases.ForEachDescriptor(func(deac catalog.Descriptor) error {
		db := deac.(catalog.DatabaseDescriptor)
		if err := repairTriggerBackrefsInDatabase(ctx, d, db); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// repairTriggerBackrefsInDatabase repairs trigger backrefs for tables within a
// single database.
func repairTriggerBackrefsInDatabase(
	ctx context.Context, d upgrade.TenantDeps, db catalog.DatabaseDescriptor,
) error {
	// Build a map of referenced relation ID -> list of (triggerTableID, triggerID) pairs.
	// This tells us which backrefs need to be repaired.
	type triggerInfo struct {
		tableID   descpb.ID
		triggerID descpb.TriggerID
	}
	refsToRepair := make(map[descpb.ID][]triggerInfo)

	// Find all tables with triggers in this database.
	if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		tables, err := txn.Descriptors().GetAllTablesInDatabase(ctx, txn.KV(), db)
		if err != nil {
			return err
		}
		return tables.ForEachDescriptor(func(desc catalog.Descriptor) error {
			tbl, ok := desc.(catalog.TableDescriptor)
			if !ok {
				return nil
			}
			triggers := tbl.GetTriggers()
			if len(triggers) == 0 {
				return nil
			}
			for _, trigger := range triggers {
				for _, refID := range trigger.DependsOn {
					refsToRepair[refID] = append(refsToRepair[refID], triggerInfo{
						tableID:   tbl.GetID(),
						triggerID: trigger.ID,
					})
				}
			}
			return nil
		})
	}); err != nil {
		return err
	}

	if len(refsToRepair) == 0 {
		return nil
	}

	log.Dev.Infof(ctx, "repairing trigger backrefs in %d relations for database %s",
		len(refsToRepair), db.GetName())

	// Collect all ref IDs to process in batches.
	refIDs := make([]descpb.ID, 0, len(refsToRepair))
	for refID := range refsToRepair {
		refIDs = append(refIDs, refID)
	}

	// Repair the backrefs in batches of 100 to avoid overly large transactions.
	const batchSize = 100
	for i := 0; i < len(refIDs); i += batchSize {
		end := i + batchSize
		if end > len(refIDs) {
			end = len(refIDs)
		}
		batch := refIDs[i:end]

		if err := d.DB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
			for _, refID := range batch {
				triggerInfos := refsToRepair[refID]
				refTbl, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, refID)
				if err != nil {
					return err
				}

				// Count how many triggers per source table reference this
				// table. When multiple triggers from the same table share a
				// backref, we can't determine which column IDs belong to
				// which trigger, so we clear them.
				tableCount := make(map[descpb.ID]int)
				for _, ti := range triggerInfos {
					tableCount[ti.tableID]++
				}
				for _, ti := range triggerInfos {
					repairTriggerBackrefInTable(refTbl, ti.tableID, ti.triggerID, tableCount[ti.tableID] > 1)
				}
				if err := txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, refTbl, txn.KV()); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

// repairTriggerBackrefInTable finds a backref with TriggerID=0 from the given
// triggerTableID and updates it to have the correct triggerID. If
// clearDependencyDetails is true, the column IDs and index ID are also cleared
// because we can't determine which columns/index this specific trigger
// references (this happens when multiple triggers from the same table share a
// backref). An empty column IDs slice is treated as meaning "depends on the
// whole table" in the dependency tracking logic, so this is being overly
// conservative.
func repairTriggerBackrefInTable(
	tbl *tabledesc.Mutable, triggerTableID descpb.ID, triggerID descpb.TriggerID, clearDependencyDetails bool,
) {
	for i := range tbl.DependedOnBy {
		ref := &tbl.DependedOnBy[i]
		if ref.ID == triggerTableID && ref.TriggerID == 0 {
			ref.TriggerID = triggerID
			if clearDependencyDetails {
				ref.ColumnIDs = nil
				ref.IndexID = 0
			}
			return
		}
	}
	// Check if this trigger already has a backref (this makes this step
	// idempotent in case it gets retried).
	alreadyExists := false
	for _, ref := range tbl.DependedOnBy {
		if ref.ID == triggerTableID && ref.TriggerID == triggerID {
			alreadyExists = true
			break
		}
	}
	if alreadyExists {
		return
	}
	// If we didn't find a matching backref, it means that this table is used
	// by multiple triggers, which in older versions would only be tracked
	// by one shared backref. That backref already had its TriggerID updated,
	// so in this case we need to add another separate backref. We don't know
	// for sure which column IDs are referenced, so we leave them empty. An
	// empty column IDs slice is treated as meaning "depends on the whole table"
	// in the dependency tracking logic, so this is being overly conservative.
	tbl.DependedOnBy = append(tbl.DependedOnBy, descpb.TableDescriptor_Reference{
		ID:        triggerTableID,
		ByID:      tbl.IsSequence(),
		TriggerID: triggerID,
	})
}
