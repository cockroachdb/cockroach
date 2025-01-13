// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"math/rand/v2"
	"net/url"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/stretchr/testify/require"
)

func TestBackupCompactionTriggered(t *testing.T) {
	defer testutils.HookGlobal(&compactForScheduledOnly, false)()
	ctx := context.Background()

	tempDir, tempDirCleanup := testutils.TempDir(t)
	defer tempDirCleanup()

	compactionThreshold := 3
	settings := cluster.MakeTestingClusterSettings()
	backupCompactionThreshold.Override(ctx, &settings.SV, int64(compactionThreshold))
	_, db, cleanup := backupRestoreTestSetupEmpty(
		t,
		singleNode,
		tempDir,
		InitManualReplication,
		base.TestClusterArgs{
			ServerArgs: base.TestServerArgs{
				Settings: settings,
			},
		},
	)
	defer cleanup()
	db.Exec(t, "BACKUP INTO 'nodelocal://1/backup'")
	var backupPath string
	db.QueryRow(t, "SHOW BACKUPS IN 'nodelocal://1/backup'").Scan(&backupPath)
	countBackups := func() (count int) {
		db.QueryRow(
			t,
			"SELECT COUNT(DISTINCT (start_time, end_time)) FROM "+
				"[SHOW BACKUP FROM $1 IN 'nodelocal://1/backup']",
			backupPath,
		).Scan(&count)
		return
	}
	for i := 2; i <= compactionThreshold; i++ {
		db.Exec(t, "BACKUP INTO LATEST IN 'nodelocal://1/backup'")
		if i != compactionThreshold {
			require.Equal(t, i, countBackups())
		}
	}
	require.Equal(t, compactionThreshold+1, countBackups())
}

func TestBackupCompaction(t *testing.T) {
	defer testutils.HookGlobal(&compactForScheduledOnly, false)()
	ctx := context.Background()

	compactionThreshold := 3
	settings := cluster.MakeTestingClusterSettings()
	backupCompactionThreshold.Override(ctx, &settings.SV, int64(compactionThreshold))

	testcases := []struct {
		name           string
		numNodes       int
		clusterArgs    base.TestClusterArgs
		collectionURIs []string
	}{
		{
			"single-node",
			singleNode,
			base.TestClusterArgs{},
			[]string{"nodelocal://1/backup"},
		},
		{
			"locality-aware-backups",
			multiNode,
			base.TestClusterArgs{
				ServerArgsPerNode: map[int]base.TestServerArgs{
					0: {
						Locality: roachpb.Locality{Tiers: []roachpb.Tier{
							{Key: "region", Value: "west"},
							{Key: "az", Value: "az1"},
							{Key: "dc", Value: "dc1"},
						}},
					},
					1: {
						Locality: roachpb.Locality{Tiers: []roachpb.Tier{
							{Key: "region", Value: "east"},
							{Key: "az", Value: "az1"},
							{Key: "dc", Value: "dc2"},
						}},
					},
					2: {
						Locality: roachpb.Locality{Tiers: []roachpb.Tier{
							{Key: "region", Value: "east"},
							{Key: "az", Value: "az2"},
							{Key: "dc", Value: "dc3"},
						}},
					},
				},
			},
			[]string{
				fmt.Sprintf(
					"nodelocal://1/backup?COCKROACH_LOCALITY=%s",
					url.QueryEscape("default"),
				),
				fmt.Sprintf(
					"nodelocal://2/backup?COCKROACH_LOCALITY=%s",
					url.QueryEscape("dc=dc2"),
				),
				fmt.Sprintf(
					"nodelocal://3/backup?COCKROACH_LOCALITY=%s",
					url.QueryEscape("region=west"),
				),
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			tempDir, tempDirCleanup := testutils.TempDir(t)
			defer tempDirCleanup()
			tc.clusterArgs.ServerArgs.Settings = settings
			for node := range tc.clusterArgs.ServerArgsPerNode {
				serverArgs := tc.clusterArgs.ServerArgsPerNode[node]
				serverArgs.Settings = settings
				tc.clusterArgs.ServerArgsPerNode[node] = serverArgs
			}
			_, db, cleanupDB := backupRestoreTestSetupEmpty(
				t, tc.numNodes, tempDir, InitManualReplication, tc.clusterArgs,
			)
			defer cleanupDB()
			insertRows := func(nRows int, table string) {
				for i := 0; i < nRows; i++ {
					a, b := rand.IntN(1000), rand.IntN(100)
					db.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES (%d, %d)", table, a, b))
				}
			}
			collectionURIs := fmt.Sprintf(
				"(%s)",
				strings.Join(util.Map(tc.collectionURIs, func(u string) string {
					return "'" + u + "'"
				}), ", "),
			)

			t.Run("inserts", func(t *testing.T) {
				db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
				defer func() {
					db.Exec(t, "DROP TABLE foo")
				}()
				insertRows(10, "foo")
				db.Exec(t, fmt.Sprintf("BACKUP INTO %s", collectionURIs))

				// Trigger compaction twice to ensure compactions chain correctly.
				for i := 2; i <= compactionThreshold; i++ {
					insertRows(10, "foo")
					db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN %s", collectionURIs))
				}

				validateCompactedBackupForTables(t, db, []string{"foo"}, collectionURIs)
			})

			t.Run("deletes", func(t *testing.T) {
				db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
				defer func() {
					db.Exec(t, "DROP TABLE foo")
				}()
				insertRows(10, "foo")
				db.Exec(t, fmt.Sprintf("BACKUP INTO %s", collectionURIs))

				for i := 2; i <= compactionThreshold; i++ {
					db.Exec(t, "DELETE FROM foo ORDER BY RANDOM() LIMIT 3")
					insertRows(10, "foo")
					db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN %s", collectionURIs))
				}

				validateCompactedBackupForTables(t, db, []string{"foo"}, collectionURIs)
			})

			t.Run("updates", func(t *testing.T) {
				db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
				defer func() {
					db.Exec(t, "DROP TABLE foo")
				}()
				insertRows(15, "foo")
				db.Exec(t, fmt.Sprintf("BACKUP INTO %s", collectionURIs))

				for i := 2; i <= compactionThreshold; i++ {
					db.Exec(t, "UPDATE foo SET b = b + 1 ORDER BY RANDOM() LIMIT 3")
					db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN %s", collectionURIs))
				}

				validateCompactedBackupForTables(t, db, []string{"foo"}, collectionURIs)
			})

			t.Run("create tables", func(t *testing.T) {
				db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
				defer func() {
					db.Exec(t, "DROP TABLE foo, bar")
				}()
				insertRows(10, "foo")
				db.Exec(t, fmt.Sprintf("BACKUP INTO %s", collectionURIs))

				for i := 2; i <= compactionThreshold-1; i++ {
					insertRows(10, "foo")
					db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN %s", collectionURIs))
				}

				db.Exec(t, "CREATE TABLE bar (a INT, b INT)")
				insertRows(10, "bar")
				db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN %s", collectionURIs))

				validateCompactedBackupForTables(t, db, []string{"foo", "bar"}, collectionURIs)
			})

			t.Run("create indexes", func(t *testing.T) {
				db.Exec(t, "CREATE TABLE foo (a INT, b INT)")
				defer func() {
					db.Exec(t, "DROP TABLE foo")
				}()
				db.Exec(t, fmt.Sprintf("BACKUP INTO %s", collectionURIs))

				for i := 2; i <= compactionThreshold-1; i++ {
					insertRows(10, "foo")
					db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN %s", collectionURIs))
				}

				db.Exec(t, "CREATE INDEX ON foo (a)")
				db.Exec(t, fmt.Sprintf("BACKUP INTO LATEST IN %s", collectionURIs))

				var numIndexes, restoredNumIndexes int
				db.QueryRow(t, "SELECT COUNT(*) FROM [SHOW INDEXES FROM foo]").Scan(&numIndexes)
				db.Exec(t, "DROP TABLE foo")
				db.Exec(t, fmt.Sprintf("RESTORE TABLE foo FROM LATEST IN %s", collectionURIs))
				db.QueryRow(t, "SELECT COUNT(*) FROM [SHOW INDEXES FROM foo]").Scan(&restoredNumIndexes)
				require.Equal(t, numIndexes, restoredNumIndexes)
			})

			// TODO (kev-cao): Once range keys are supported by the compaction
			// iterator, add tests for dropped tables/indexes.
		})
	}
}

func validateCompactedBackupForTables(
	t *testing.T, db *sqlutils.SQLRunner, tables []string, collectionURIs string,
) {
	t.Helper()
	for _, table := range tables {
		originalRows := db.QueryStr(t, "SELECT * FROM "+table)
		db.Exec(t, "DROP TABLE "+table)
		db.Exec(t, fmt.Sprintf("RESTORE TABLE %s FROM LATEST IN %s", table, collectionURIs))
		restoredRows := db.QueryStr(t, "SELECT * FROM "+table)
		require.Equal(t, originalRows, restoredRows, "table %s", table)
	}
}
