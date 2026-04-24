// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catkv

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestSystemDatabaseCacheUpdate verifies SystemDatabaseCache.update behavior for
// the scenarios that matter to PCR reader tenants. A dynamically allocated
// system table (e.g. system.privileges) is bootstrapped at one ID and later
// re-pointed to a different ID by SetupOrAdvanceStandbyReaderCatalog. The cache
// must pick up the new mapping (otherwise lookups fail with "resolved <name> to
// <stale id> but found no descriptor with id <stale id>"), but it must never
// regress to an older mapping if a stale read races in late.
func TestSystemDatabaseCacheUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	v := clusterversion.ClusterVersion{Version: st.Version.LatestVersion()}

	// privKey models system.public.privileges, a dynamically allocated system
	// table whose ID can differ between bootstrap (with a tenant ID offset) and
	// the externalized source-cluster ID.
	privKey := descpb.NameInfo{
		ParentID:       keys.SystemDatabaseID,
		ParentSchemaID: keys.SystemPublicSchemaID,
		Name:           "privileges",
	}

	const oldID = descpb.ID(1000000052)
	const newID = descpb.ID(52)
	tsOld := hlc.Timestamp{WallTime: 100}
	tsNew := hlc.Timestamp{WallTime: 200}

	upsert := func(c *SystemDatabaseCache, id descpb.ID, ts hlc.Timestamp) {
		var mc nstree.MutableCatalog
		mc.UpsertNamespaceEntry(privKey, id, ts)
		c.update(v, mc.Catalog)
	}

	t.Run("newer ID at newer timestamp replaces stale entry", func(t *testing.T) {
		// This is the PCR reader-tenant bug (#153555): the bootstrap-time ID
		// must be replaced by the externalized ID once SetupOrAdvanceStandbyReaderCatalog
		// has rewritten the namespace.
		c := NewSystemDatabaseCache(keys.SystemSQLCodec, st)
		upsert(c, oldID, tsOld)
		gotID, _ := c.lookupDescriptorID(v, privKey)
		require.Equal(t, oldID, gotID)

		upsert(c, newID, tsNew)
		gotID, _ = c.lookupDescriptorID(v, privKey)
		require.Equal(t, newID, gotID)
	})

	t.Run("older ID at older timestamp does not regress newer entry", func(t *testing.T) {
		// A read at an older AOST or a stale rangefeed event must not overwrite
		// a fresher mapping.
		c := NewSystemDatabaseCache(keys.SystemSQLCodec, st)
		upsert(c, newID, tsNew)
		upsert(c, oldID, tsOld)
		gotID, _ := c.lookupDescriptorID(v, privKey)
		require.Equal(t, newID, gotID)
	})

	t.Run("same ID is a no-op", func(t *testing.T) {
		// Re-observing the same mapping should never cause work; it must not
		// move the cached MVCC timestamp backward either.
		c := NewSystemDatabaseCache(keys.SystemSQLCodec, st)
		upsert(c, newID, tsNew)
		upsert(c, newID, tsOld)
		gotID, gotTS := c.lookupDescriptorID(v, privKey)
		require.Equal(t, newID, gotID)
		require.Equal(t, tsNew, gotTS)
	})
}
