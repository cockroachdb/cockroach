// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestTriggerBackrefRepair(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V26_2_TriggerBackrefRepair)

	clusterArgs := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					DisableAutomaticVersionUpgrade: make(chan struct{}),
					ClusterVersionOverride:         clusterversion.MinSupported.Version(),
				},
			},
		},
	}

	ctx := context.Background()
	ts := serverutils.StartServerOnly(t, clusterArgs.ServerArgs)
	defer ts.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(ts.SQLConn(t))

	// Create tables and triggers with dependencies.
	sqlDB.Exec(t, `CREATE TABLE ref_table (id INT PRIMARY KEY, data TEXT)`)
	sqlDB.Exec(t, `CREATE TABLE trigger_table (id INT PRIMARY KEY)`)
	sqlDB.Exec(t, `
		CREATE FUNCTION trigger_fn() RETURNS TRIGGER LANGUAGE PLpgSQL AS $$
		BEGIN
			SELECT id FROM ref_table LIMIT 1;
			RETURN NEW;
		END;
		$$
	`)
	sqlDB.Exec(t, `CREATE TRIGGER tr1 AFTER INSERT ON trigger_table FOR EACH ROW EXECUTE FUNCTION trigger_fn()`)
	sqlDB.Exec(t, `CREATE TRIGGER tr2 AFTER UPDATE ON trigger_table FOR EACH ROW EXECUTE FUNCTION trigger_fn()`)

	// Get the table IDs.
	var refTableID, triggerTableID int
	sqlDB.QueryRow(t, `SELECT 'ref_table'::REGCLASS::OID::INT`).Scan(&refTableID)
	sqlDB.QueryRow(t, `SELECT 'trigger_table'::REGCLASS::OID::INT`).Scan(&triggerTableID)

	// Simulate the legacy state by setting TriggerID=0 on the backrefs.
	// This mimics what would happen if triggers were created before the
	// TriggerID field was added to TableDescriptor_Reference.
	descDB := ts.InternalDB().(descs.DB)
	err := descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		refTbl, err := txn.Descriptors().MutableByID(txn.KV()).Table(ctx, descpb.ID(refTableID))
		if err != nil {
			return err
		}

		// Set all trigger backrefs to have TriggerID=0 (legacy state).
		// Also deduplicate them as the old code would have.
		seen := make(map[descpb.ID]int)
		for i := range refTbl.DependedOnBy {
			ref := &refTbl.DependedOnBy[i]
			if ref.ID == descpb.ID(triggerTableID) {
				ref.TriggerID = 0 // Set to legacy zero value.
				if idx, ok := seen[ref.ID]; ok {
					// Deduplicate by merging column IDs.
					for _, colID := range ref.ColumnIDs {
						found := false
						for _, existingColID := range refTbl.DependedOnBy[idx].ColumnIDs {
							if existingColID == colID {
								found = true
								break
							}
						}
						if !found {
							refTbl.DependedOnBy[idx].ColumnIDs = append(refTbl.DependedOnBy[idx].ColumnIDs, colID)
						}
					}
					// Mark this entry for removal by setting ID to 0.
					ref.ID = 0
				} else {
					seen[ref.ID] = i
				}
			}
		}
		// Remove duplicates (entries with ID=0).
		var filtered []int
		for i := range refTbl.DependedOnBy {
			if refTbl.DependedOnBy[i].ID != 0 {
				filtered = append(filtered, i)
			}
		}
		if len(filtered) < len(refTbl.DependedOnBy) {
			// Rebuild DependedOnBy with only non-zero ID entries.
			newDeps := refTbl.DependedOnBy[:0]
			for _, i := range filtered {
				newDeps = append(newDeps, refTbl.DependedOnBy[i])
			}
			refTbl.DependedOnBy = newDeps
		}

		return txn.Descriptors().WriteDesc(ctx, false /* kvTrace */, refTbl, txn.KV())
	})
	require.NoError(t, err)

	// Verify the backrefs are in legacy state (TriggerID=0).
	err = descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		refTbl, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, descpb.ID(refTableID))
		if err != nil {
			return err
		}
		foundLegacyBackref := false
		for _, ref := range refTbl.TableDesc().DependedOnBy {
			if ref.ID == descpb.ID(triggerTableID) {
				require.Equal(t, descpb.TriggerID(0), ref.TriggerID, "expected TriggerID=0 before upgrade")
				foundLegacyBackref = true
			}
		}
		require.True(t, foundLegacyBackref, "expected to find legacy backref with TriggerID=0")
		return nil
	})
	require.NoError(t, err)

	// Run the upgrade.
	upgrades.Upgrade(t, ts.SQLConn(t), clusterversion.V26_2_TriggerBackrefRepair, nil, false)

	// Verify the backrefs are now repaired with correct TriggerIDs.
	err = descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		refTbl, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, descpb.ID(refTableID))
		if err != nil {
			return err
		}
		triggerIDs := make(map[descpb.TriggerID]bool)
		for _, ref := range refTbl.TableDesc().DependedOnBy {
			if ref.ID == descpb.ID(triggerTableID) {
				require.NotEqual(t, descpb.TriggerID(0), ref.TriggerID,
					"expected TriggerID!=0 after upgrade, got TriggerID=%d", ref.TriggerID)
				triggerIDs[ref.TriggerID] = true
			}
		}
		// We should have two distinct trigger backrefs (for tr1 and tr2).
		require.Len(t, triggerIDs, 2, "expected 2 distinct trigger backrefs after upgrade")
		return nil
	})
	require.NoError(t, err)

	// Verify that dropping a trigger now correctly removes only its backref.
	sqlDB.Exec(t, `DROP TRIGGER tr1 ON trigger_table`)

	err = descDB.DescsTxn(ctx, func(ctx context.Context, txn descs.Txn) error {
		refTbl, err := txn.Descriptors().ByIDWithoutLeased(txn.KV()).WithoutNonPublic().Get().Table(ctx, descpb.ID(refTableID))
		if err != nil {
			return err
		}
		triggerCount := 0
		for _, ref := range refTbl.TableDesc().DependedOnBy {
			if ref.ID == descpb.ID(triggerTableID) {
				triggerCount++
				// Should be trigger 2 (tr2).
				require.Equal(t, descpb.TriggerID(2), ref.TriggerID,
					"expected only tr2's backref to remain")
			}
		}
		require.Equal(t, 1, triggerCount, "expected exactly 1 trigger backref after dropping tr1")
		return nil
	})
	require.NoError(t, err)
}
