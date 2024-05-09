// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestValidateSizeOfResult tests `validateSizeOfResult` function and how its
// result depends on `sql.stats.response.max` cluster setting.
func TestValidateSizeOfResult(t *testing.T) {
	defer log.Scope(t).Close(t)
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	s, sqlDb, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	appName := "test"
	statsCount := 100
	_, _ = sqlDb.Exec(fmt.Sprintf("SET application_name = '%s'", appName))

	err := generateStats(sqlDb, statsCount)
	require.NoError(t, err)

	testCases := []struct {
		name     string
		maxLimit int
		evalErr  func(require.TestingT, error)
	}{
		{
			"exceeds max limit",
			statsCount - 1,
			func(t require.TestingT, err error) {
				require.ErrorIs(t, err, errMaxResultsSize)
			},
		},
		{
			"less than max limit",
			statsCount + 1,
			func(t require.TestingT, err error) {
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _ = sqlDb.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.stats.response.max = %d", tc.maxLimit))
			err := validateSizeOfResult(
				ctx,
				s.ApplicationLayer().InternalExecutor().(*sql.InternalExecutor),
				s.ClusterSettings(),
				CrdbInternalStmtStatsCombined,
				fmt.Sprintf("WHERE app_name = '%s'", appName),
				"",
				"",
				nil,
				"SELECT * FROM %s %s %s %s %s",
			)
			tc.evalErr(t, err)
		})
	}
}

// TestStatementStatsRunner tests integration of `validateSizeOfResult` with
// caller functions.
func TestStatementStatsRunner(t *testing.T) {
	defer log.Scope(t).Close(t)
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	s, sqlDb, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLStatsKnobs: sqlstats.CreateTestingKnobs(),
		},
	})
	defer s.Stopper().Stop(ctx)

	stmtTable := CrdbInternalStmtStatsCombined
	txnTable := CrdbInternalTxnStatsCombined
	appName := "test"
	statsCount := 100
	_, _ = sqlDb.Exec(fmt.Sprintf("SET application_name = '%s'", appName))
	settings := s.ClusterSettings()
	knobs := s.TestingKnobs().SQLStatsKnobs.(*sqlstats.TestingKnobs)
	err := generateStats(sqlDb, statsCount)
	require.NoError(t, err)

	runner := &statementStatsRunner{
		stmtSourceTable: stmtTable,
		txnSourceTable:  txnTable,
		ie:              s.ApplicationLayer().InternalExecutor().(*sql.InternalExecutor),
		testingKnobs:    knobs,
		settings:        settings,
	}
	where := fmt.Sprintf("WHERE app_name = '%s'", appName)
	args := []interface{}{}
	order := ""
	limit := ""

	testCases := []struct {
		name     string
		maxLimit int
		assertFn func(t *testing.T, resultsCount int, err error)
	}{
		{
			"less than max limit",
			statsCount * 10, // maxLimit
			func(t *testing.T, resultsCount int, err error) {
				require.NoError(t, err)
				require.GreaterOrEqual(t, resultsCount, statsCount)
			},
		},
		{
			"exceeds max limit",
			1, // maxLimit
			func(t *testing.T, resultsCount int, err error) {
				require.ErrorIs(t, err, errMaxResultsSize)
				require.Equal(t, 0, resultsCount)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, _ = sqlDb.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.stats.response.max = %d", tc.maxLimit))

			t.Run("collectCombinedStatements", func(t *testing.T) {
				stats, err := runner.collectCombinedStatements(ctx, where, args, order, limit, settings)
				tc.assertFn(t, len(stats), err)
			})

			t.Run("collectCombinedTransactions", func(t *testing.T) {
				stats, err := runner.collectCombinedTransactions(ctx, where, args, order, limit)
				tc.assertFn(t, len(stats), err)
			})

			t.Run("collectStmtsForTxns", func(t *testing.T) {
				// Set max response size to some large number to ensure we get transactions stats as a prerequisite
				// without any restrictions.
				_, _ = sqlDb.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.stats.response.max = %d", statsCount*10))
				transactions, err := runner.collectCombinedTransactions(ctx, where, args, order, limit)
				require.NoError(t, err)
				require.GreaterOrEqual(t, len(transactions), statsCount)

				_, _ = sqlDb.Exec(fmt.Sprintf("SET CLUSTER SETTING sql.stats.response.max = %d", tc.maxLimit))
				req := serverpb.CombinedStatementsStatsRequest{}
				stats, err := runner.collectStmtsForTxns(ctx, &req, transactions)
				tc.assertFn(t, len(stats), err)
			})
		})
	}
}

func generateStats(sqlDb *gosql.DB, count int) error {
	vals := make([]string, count)
	for i := range vals {
		vals[i] = fmt.Sprintf("%d", i)
	}
	for i := 1; i <= len(vals); i++ {
		tx, err := sqlDb.Begin()
		if err != nil {
			return err
		}
		_, err = tx.Exec("SELECT " + strings.Join(vals[:i], ", "))
		if err != nil {
			_ = tx.Rollback()
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}
