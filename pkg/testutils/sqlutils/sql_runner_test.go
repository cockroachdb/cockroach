// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlutils_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Test that the RowsToStrMatrix doesn't swallow errors.
func TestRowsToStrMatrixError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	// We'll run a query that only fails after returning some rows, so that the
	// error is discovered by RowsToStrMatrix below.
	rows, err := db.Query(
		"select case x when 5 then crdb_internal.force_error('00000', 'testing error') else x end from generate_series(1,5) as v(x);")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := sqlutils.RowsToStrMatrix(rows); !testutils.IsError(err, "testing error") {
		t.Fatalf("expected 'testing error', got: %v", err)
	}
}

// TestRunWithRetriableTxn tests that we actually do retry the
// transaction.
func TestRunWithRetriableTxn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	runner := sqlutils.MakeSQLRunner(db)
	runner.Exec(t, "SET inject_retry_errors_enabled=true")

	t.Run("retries transaction restart errors", func(t *testing.T) {
		callCount := 0
		runner.RunWithRetriableTxn(t, func(txn *gosql.Tx) error {
			callCount++
			_, err := txn.Exec("SELECT crdb_internal.force_retry('5ms')")
			return err
		})
		require.GreaterOrEqual(t, callCount, 2)
	})
	t.Run("respects max retries", func(t *testing.T) {
		callCount := 0
		runner.MaxTxnRetries = 5
		f := &mockFataler{}
		runner.RunWithRetriableTxn(f, func(txn *gosql.Tx) error {
			callCount++
			_, err := txn.Exec("SELECT crdb_internal.force_retry('5h')")
			return err
		})
		require.Equal(t, callCount, runner.MaxTxnRetries+1)
		require.Contains(t, f.err, "restart transaction")
		require.Contains(t, f.err, "retries exhausted")
	})
}

type mockFataler struct {
	err string
}

func (f *mockFataler) Fatalf(s string, args ...interface{}) {
	f.err = fmt.Sprintf(s, args...)
}
