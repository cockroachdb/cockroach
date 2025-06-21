// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestSqlInstancesAddLocalityAddressList(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clusterversion.SkipWhenMinSupportedVersionIsAtLeast(t, clusterversion.V25_3)

	ctx := context.Background()
	var s, db, _ = serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{
				DisableAutomaticVersionUpgrade: make(chan struct{}),
				ClusterVersionOverride:         clusterversion.MinSupported.Version(),
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	// Create a fake session ID
	fakeSessionID := uuid.MakeV4().GetBytes()

	// Insert a test SQL instance row with the fake session
	const insertSQL = `
        INSERT INTO system.sql_instances (
            id,
            addr,
            session_id,
            locality,
            sql_addr,
            crdb_region,
            binary_version,
            is_draining
        ) VALUES (
            1001,
            'test-addr:26257',
            $1,
            '{"region": "test-region", "zone": "test-zone"}'::JSONB,
            'test-sql-addr:26257',
            'test-region',
            'v25.1.0-test',
            false
        )`

	_, err := db.Exec(insertSQL, fakeSessionID)
	require.NoError(t, err, "failed to insert test sql instance row")

	// Verify row exists before upgrade
	var id int
	err = db.QueryRow("SELECT id FROM system.sql_instances WHERE id = 1001").Scan(&id)
	require.NoError(t, err)
	require.Equal(t, 1001, id, "test row should exist before upgrade")

	// Try to access the column that doesn't exist yet
	_, err = db.Exec("SELECT locality_address_list FROM system.sql_instances WHERE id = 1001")
	require.Error(t, err, "locality_address_list column should not exist before upgrade")

	// Perform the upgrade
	upgrades.Upgrade(t, db, clusterversion.V25_3_SQLInstancesAddLocalityAddressList, nil, false)

	// Verify the column exists now and the row is still there
	var binVer string
	err = db.QueryRow(
		"SELECT binary_version FROM system.sql_instances WHERE id = 1001 AND locality_address_list IS NULL",
	).Scan(&binVer)
	require.NoError(t, err, "row should still exist after upgrade")
	require.Equal(t, "v25.1.0-test", binVer)

	// Update the new column with locality-specific addresses
	const updateSQL = `
        UPDATE system.sql_instances 
        SET locality_address_list = '[
            {"locality":"region=test-region,zone=test-zone", "address":"region-addr:26257"},
            {"locality":"region=test-region", "address":"default-addr:26257"}
        ]'::JSONB 
        WHERE id = 1001`

	_, err = db.Exec(updateSQL)
	require.NoError(t, err, "should be able to update the new column")

	// Verify the update worked
	var localityAddressList []byte
	err = db.QueryRow(
		"SELECT locality_address_list FROM system.sql_instances WHERE id = 1001",
	).Scan(&localityAddressList)
	require.NoError(t, err)

	// Parse and verify the JSON content
	var addresses []map[string]string
	err = json.Unmarshal(localityAddressList, &addresses)
	require.NoError(t, err)
	require.Len(t, addresses, 2)
	require.Equal(t, "region=test-region,zone=test-zone", addresses[0]["locality"])
	require.Equal(t, "region-addr:26257", addresses[0]["address"])
}
