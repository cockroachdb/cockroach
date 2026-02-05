// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package inspect

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

// runUniquenessCheck is a test helper that creates and runs a uniqueness check on the given table and index.
// If indexName is empty, it checks the primary index. Otherwise, it finds and checks the named secondary index.
func runUniquenessCheck(
	ctx context.Context,
	t *testing.T,
	s serverutils.ApplicationLayerInterface,
	tableDesc catalog.TableDescriptor,
	indexName string,
	asOf hlc.Timestamp,
) []*inspectIssue {
	t.Helper()

	execCfg := s.ExecutorConfig().(sql.ExecutorConfig)
	codec := s.Codec()

	// Find the index to check
	var index catalog.Index
	if indexName == "" {
		index = tableDesc.GetPrimaryIndex()
	} else {
		found := false
		for _, idx := range tableDesc.PublicNonPrimaryIndexes() {
			if idx.GetName() == indexName {
				index = idx
				found = true
				break
			}
		}
		require.True(t, found, "index %q not found", indexName)
	}

	// Create the uniqueness check
	check := &uniquenessCheck{
		uniquenessCheckApplicability: uniquenessCheckApplicability{
			tableID: tableDesc.GetID(),
		},
		execCfg:      &execCfg,
		indexID:      index.GetID(),
		tableVersion: tableDesc.GetVersion(),
		asOf:         asOf,
	}

	// Get the table span
	tablePrefix := codec.TablePrefix(uint32(tableDesc.GetID()))
	tableSpan := roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	}

	// Use the DistSQL server config which has all the necessary fields
	cfg := &execCfg.DistSQLSrv.ServerConfig

	// Start the check
	err := check.Start(ctx, cfg, tableSpan, 0 /* workerIndex */)
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

	testCases := []struct {
		name             string
		createTableStmt  string
		insertStmts      []string
		indexName        string // empty string means check primary index
		expectDuplicates bool
		expectedCount    int
	}{
		{
			name:            "no_duplicates",
			createTableStmt: `CREATE TABLE test.t (id INT PRIMARY KEY)`,
			insertStmts: []string{
				`INSERT INTO test.t VALUES (1), (2), (3), (4), (5)`,
			},
		},
		{
			name:            "empty_table",
			createTableStmt: `CREATE TABLE test.t (id INT PRIMARY KEY)`,
			insertStmts:     []string{},
		},
		{
			name:            "larger_dataset_no_duplicates",
			createTableStmt: `CREATE TABLE test.t (id INT PRIMARY KEY)`,
			insertStmts: []string{
				`INSERT INTO test.t SELECT * FROM generate_series(1, 100)`,
			},
		},
		{
			name:            "larger_dataset_multiple_spans",
			createTableStmt: `CREATE TABLE test.t (id INT PRIMARY KEY)`,
			insertStmts: []string{
				`ALTER TABLE test.t SPLIT AT VALUES (33), (66)`,
				`INSERT INTO test.t SELECT * FROM generate_series(1, 100)`,
			},
		},
		{
			name:            "duplicates_on_secondary_index",
			createTableStmt: `CREATE TABLE test.t (id INT PRIMARY KEY, a INT, INDEX idx_a (a))`,
			insertStmts: []string{
				`INSERT INTO test.t VALUES (1, 10), (2, 10), (3, 10), (4, 40), (5, 40)`,
			},
			indexName:        "idx_a",
			expectDuplicates: true,
			expectedCount:    2,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create table
			r.Exec(t, tc.createTableStmt)
			defer r.Exec(t, `DROP TABLE test.t`)

			// Insert data
			for _, stmt := range tc.insertStmts {
				r.Exec(t, stmt)
			}

			// Get a timestamp for AS OF SYSTEM TIME
			var timestampStr string
			r.QueryRow(t, "SELECT cluster_logical_timestamp()::STRING").Scan(&timestampStr)
			asOfTimestamp, err := hlc.ParseHLC(timestampStr)
			require.NoError(t, err)

			// Get the table descriptor
			tableDesc := desctestutils.TestingGetPublicTableDescriptor(kvDB, codec, "test", "t")

			// Run the uniqueness check
			issues := runUniquenessCheck(ctx, t, s.ApplicationLayer(), tableDesc, tc.indexName, asOfTimestamp)

			// Verify results
			require.Equal(t, tc.expectedCount, len(issues), "expected %d issues, got %d", tc.expectedCount, len(issues))

			if tc.expectDuplicates {
				require.Greater(t, len(issues), 0, "expected to find duplicate issues")
				for _, issue := range issues {
					require.Equal(t, DuplicateUniqueValue, issue.ErrorType)
					require.NotEmpty(t, issue.PrimaryKey)
					require.Contains(t, issue.Details, redact.RedactableString("duplicate_count"))
					require.Contains(t, issue.Details, redact.RedactableString("index_name"))
				}
			}
		})
	}
}
