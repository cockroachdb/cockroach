// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
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
	createSpan createSpanFunc,
	asOf hlc.Timestamp,
) ([]*inspectIssue, error) {
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

	span := createSpan(cfg, tableDesc)

	if err := check.Start(ctx, cfg, span, 0 /* workerIndex */); err != nil {
		return nil, err
	}

	// Collect all issues
	var issues []*inspectIssue
	for !check.Done(ctx) {
		issue, err := check.Next(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if issue != nil {
			issues = append(issues, issue)
		}
	}

	if err := check.Close(ctx); err != nil {
		return nil, err
	}

	return issues, nil
}

type createSpanFunc func(cfg *execinfra.ServerConfig, tableDesc catalog.TableDescriptor) roachpb.Span

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

	defaultCreateTableStmt := `CREATE TABLE test.%s (
				crdb_region test.region_enum NOT NULL,
				id INT64 DEFAULT unordered_unique_rowid(),
				data TEXT,
				PRIMARY KEY (crdb_region, id)
				)`

	defaultCreateSpan := func(cfg *execinfra.ServerConfig, tableDesc catalog.TableDescriptor) roachpb.Span {
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
		startKey, err := keyside.Encode(slices.Clone(regionPrefixBytes), startID, encoding.Ascending)
		require.NoError(t, err)
		endID := tree.NewDInt(10000)
		endKey, err := keyside.Encode(slices.Clone(regionPrefixBytes), endID, encoding.Ascending)
		require.NoError(t, err)

		span := roachpb.Span{
			Key:    roachpb.Key(startKey),
			EndKey: roachpb.Key(endKey),
		}

		return span
	}

	testCases := []struct {
		name             string
		createTableStmt  string   // optional override of the default create table statement
		insertStmts      []string // must include an id value [1, 10000)
		createSpan       createSpanFunc
		expectDuplicates bool
		expectedCount    int

		expectError string
	}{
		{
			name: "no_unique_rowid",
			createTableStmt: `CREATE TABLE test.%s (
				crdb_region test.region_enum NOT NULL,
				data TEXT,
				PRIMARY KEY (crdb_region)
				)`,
			expectError: "index has no `unique_rowid()` or `unordered_unique_rowid()` column",
		},
		{
			name: "multiple_unique_rowid",
			createTableStmt: `CREATE TABLE test.%s (
				crdb_region test.region_enum NOT NULL,
				id_a INT64 DEFAULT unordered_unique_rowid(),
				id_b INT64 DEFAULT unique_rowid(),
				data TEXT,
				PRIMARY KEY (crdb_region, id_a, id_b)
				)`,
			expectError: "uniqueness check for indexes with multiple `unique_rowid()`",
		},
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
			name: "no_rows_in_region",
			insertStmts: []string{
				// Insert to a lesser region which is not queried
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('eu-central1', 100, 'bar')`,
				// Insert to a greater region
				`INSERT INTO test.%s (crdb_region, id, data) VALUES ('us-west1', 200, 'bar')`,
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
			expectedCount:    1,
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
			expectedCount:    2,
		},
		{
			// The unique column is not immediately after the region column
			name: "complex_primary_key",
			createTableStmt: `CREATE TABLE test.%s (
				crdb_region test.region_enum NOT NULL,
				filler TEXT,
				id INT64 DEFAULT unordered_unique_rowid(),
				data TEXT,
				PRIMARY KEY (crdb_region, filler, id)
				)`,
			// TODO(154551): Add support for this case.
			expectError: "the unique column must be immediately after the regionality column",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var createTableStmt string
			if tc.createTableStmt == "" {
				createTableStmt = defaultCreateTableStmt
			} else {
				createTableStmt = tc.createTableStmt
			}

			var createSpan createSpanFunc
			if tc.createSpan == nil {
				createSpan = defaultCreateSpan
			} else {
				createSpan = tc.createSpan
			}

			tableName := fmt.Sprintf("t_%s", tc.name)
			r.Exec(t, fmt.Sprintf(createTableStmt, tableName))
			defer r.Exec(t, fmt.Sprintf("DROP TABLE test.%s", tableName))

			for _, stmt := range tc.insertStmts {
				r.Exec(t, fmt.Sprintf(stmt, tableName))
			}

			var timestampStr string
			r.QueryRow(t, "SELECT cluster_logical_timestamp()::STRING").Scan(&timestampStr)
			asOfTimestamp, err := hlc.ParseHLC(timestampStr)
			require.NoError(t, err)

			basicTableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", tableName)
			tableID := basicTableDesc.GetID()
			tableVersion := basicTableDesc.GetVersion()

			// Load the table descriptor properly to get the enum
			execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
			tableDesc, err := loadTableDesc(ctx, &execCfg, tableID, tableVersion, asOfTimestamp)
			require.NoError(t, err)

			issues, err := runUniquenessCheck(ctx, t, s.ApplicationLayer(), tableDesc, createSpan, asOfTimestamp)
			if tc.expectError != "" {
				require.ErrorContains(t, err, tc.expectError)
			} else {
				require.NoError(t, err, "unexpected error: %v", err)
			}

			require.Equal(t, tc.expectedCount, len(issues), "expected %d issues, got %s", tc.expectedCount, issues)

			if tc.expectDuplicates {
				require.Greater(t, len(issues), 0, "expected to find duplicate issues")
				for _, issue := range issues {
					require.Equal(t, DuplicateUniqueValue, issue.ErrorType, issue)
					require.Contains(t, issue.PrimaryKey, redact.RedactableString("us-east1"))
					require.Contains(t, issue.Details, redact.RedactableString("index_name"))
					require.Contains(t, issue.Details, redact.RedactableString("remote_regions"))
					require.IsType(t, []string{}, issue.Details["remote_regions"])
					require.NotEmpty(t, issue.Details["remote_regions"].([]string))
				}
			}
		})
	}
}
