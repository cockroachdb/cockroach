// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// runUniquenessCheck is a test helper that creates and runs a uniqueness check
// on the given table and index. It has some hardcoded assumptions around the
// table structure.
func runUniquenessCheck(
	ctx context.Context,
	t *testing.T,
	s serverutils.ApplicationLayerInterface,
	tableDesc catalog.TableDescriptor,
	asOf hlc.Timestamp,
) []*inspectIssue {
	t.Helper()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)

	check := &uniquenessCheck{
		uniquenessCheckApplicability: uniquenessCheckApplicability{
			tableID: tableDesc.GetID(),
		},
		execCfg: &execCfg,
		indexID: tableDesc.GetPrimaryIndex().GetID(),
		asOf:    asOf,
	}

	cfg := &execCfg.DistSQLSrv.ServerConfig

	index := tableDesc.GetPrimaryIndex()

	indexPrefix := cfg.Codec.IndexPrefix(uint32(tableDesc.GetID()), uint32(index.GetID()))

	regionColID := index.GetKeyColumnID(0)
	regionCol, err := catalog.MustFindColumnByID(tableDesc, regionColID)
	require.NoError(t, err)

	// Use the faked "us-east1" region for testing. And the second part of the
	// key is an INT64 [1, 10000).

	testRegion := "us-east1"
	regionDatum, err := tree.MakeDEnumFromLogicalRepresentation(regionCol.GetType(), testRegion)
	require.NoError(t, err)

	regionPrefixBytes, err := keyside.Encode(indexPrefix, &regionDatum, encoding.Ascending)
	require.NoError(t, err)

	startID := tree.NewDInt(1)
	startKey, err := keyside.Encode(regionPrefixBytes, startID, encoding.Ascending)
	require.NoError(t, err)
	endID := tree.NewDInt(10000)
	endKey, err := keyside.Encode(regionPrefixBytes, endID, encoding.Ascending)
	require.NoError(t, err)

	span := roachpb.Span{
		Key:    roachpb.Key(startKey),
		EndKey: roachpb.Key(endKey),
	}

	// Start the check
	err = check.Start(ctx, cfg, span, 0 /* workerIndex */)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, check.Close(ctx))
	}()

	// Collect all issues
	var issues []*inspectIssue
	for !check.Done(ctx) {
		issue, err := check.Next(ctx, cfg)
		require.NoError(t, err)
		if issue != nil {
			issues = append(issues, issue)
		}
	}

	return issues
}

func TestUniquenessCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	codec := s.ApplicationLayer().Codec()
	r := sqlutils.MakeSQLRunner(db)

	r.Exec(t, `CREATE DATABASE test`)
	r.Exec(t, `USE test`)

	r.Exec(t, `CREATE TYPE test.region_enum AS ENUM ('eu-central1', 'us-east1', 'us-west1', 'us-west4')`)

	createTableStmt := `CREATE TABLE test.%s (
				crdb_region test.region_enum NOT NULL,
				id INT64 DEFAULT unordered_unique_rowid(),
				data TEXT,
				PRIMARY KEY (crdb_region, id)
				)`

	testCases := []struct {
		name             string
		insertStmts      []string // must include an id value [1, 10000)
		expectDuplicates bool
		expectedCount    int
	}{
		{
			name: "single_region_no_duplicates",
			insertStmts: []string{
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 100, 'foo')`,
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 200, 'bar')`,
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 300, 'baz')`,
			},
		},
		{
			name: "multi_region_no_duplicates",
			insertStmts: []string{
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 100, 'foo')`,
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-west1', 200, 'bar')`,
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('eu-central1', 300, 'baz')`,
			},
		},
		{
			name: "two_region_with_duplicate_lesser_region",
			insertStmts: []string{
				// Insert a row in us-east1 with a specific ID
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 100, 'foo')`,
				// Insert to a lesser region which is not queried
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('eu-central1', 100, 'bar')`,
			},
		},
		{
			name: "two_region_with_duplicate",
			insertStmts: []string{
				// Insert a row in us-east1 with a specific ID
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 100, 'foo')`,
				// "Unsafe" insert to duplicate ID in a second region
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-west1', 100, 'bar')`,
			},
			expectDuplicates: true,
			expectedCount:    1,
		},
		{
			name: "multi_region_with_duplicates",
			insertStmts: []string{
				// Insert a row in us-east1 with a specific ID
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 100, 'foo')`,
				// "Unsafe" insert to duplicate ID in a second region
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-west1', 100, 'bar')`,
				// "Unsafe" insert to duplicate ID in a third region
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-west4', 100, 'bar')`,

				// "Unsafe" insert to duplicate ID in a fourth region which is lesser and not checked
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('eu-central1', 100, 'baz')`,
			},
			expectDuplicates: true,
			expectedCount:    2,
		},
		{
			name: "multi_region_with_multiple_duplicates",
			insertStmts: []string{
				// Insert rows in us-east1 with specific IDs
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 100, 'foo')`,
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-east1', 200, 'titi')`,
				// "Unsafe" inserts to duplicate IDs in a second region
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-west1', 100, 'bar')`,
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-west1', 200, 'toto')`,
				// "Unsafe" insert to duplicate ID in a third region
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-west4', 100, 'baz')`,
			},
			expectDuplicates: true,
			expectedCount:    3,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableName := fmt.Sprintf("t_%s", tc.name)
			r.Exec(t, fmt.Sprintf(createTableStmt, tableName))
			defer r.Exec(t, fmt.Sprintf("DROP TABLE test.%s", tableName))

			r.Exec(t, "BEGIN")
			defer r.Exec(t, "ROLLBACK")
			for _, stmt := range tc.insertStmts {
				r.Exec(t, fmt.Sprintf(stmt, tableName))
			}
			r.Exec(t, "COMMIT")
			var timestampStr string
			r.QueryRow(t, "SELECT cluster_logical_timestamp()::STRING").Scan(&timestampStr)
			asOfTimestamp, err := hlc.ParseHLC(timestampStr)
			require.NoError(t, err)

			// FIXME(bghal): is this leaking?
			basicTableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", tableName)
			tableID := basicTableDesc.GetID()
			tableVersion := basicTableDesc.GetVersion()

			// Load the table descriptor properly to get the enum
			execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
			tableDesc, err := loadTableDesc(ctx, &execCfg, tableID, tableVersion, asOfTimestamp)
			require.NoError(t, err)

			issues := runUniquenessCheck(ctx, t, s.ApplicationLayer(), tableDesc, asOfTimestamp)

			require.Equal(t, tc.expectedCount, len(issues), "expected %d issues, got %d", tc.expectedCount, len(issues))

			if tc.expectDuplicates {
				require.Greater(t, len(issues), 0, "expected to find duplicate issues")
				for _, issue := range issues {
					require.Equal(t, DuplicateUniqueValue, issue.ErrorType)
					require.NotEmpty(t, issue.PrimaryKey)
					require.Contains(t, issue.Details, redact.RedactableString("index_name"))
				}
			}
		})
	}
}
