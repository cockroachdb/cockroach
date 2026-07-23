// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs_test

import (
	"context"
	gosql "database/sql"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach-go/v2/crdb"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/sqllivenesstestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/stretchr/testify/require"
)

// TestTxnWithStepping tests that if the user opts into stepping, they
// get stepping.
func TestTxnWithStepping(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)

	s := srv.ApplicationLayer()

	db := s.InternalDB().(descs.DB)

	scratchKey := append(s.Codec().TenantPrefix(), keys.ScratchRangeMin...)
	_, _, err := srv.StorageLayer().SplitRange(scratchKey)
	require.NoError(t, err)
	// Write a key, read in the transaction without stepping, ensure we
	// do not see the value, step the transaction, then ensure that we do.
	require.NoError(t, db.DescsTxn(ctx, func(
		ctx context.Context, txn descs.Txn,
	) error {
		if err := txn.KV().Put(ctx, scratchKey, 1); err != nil {
			return err
		}
		{
			got, err := txn.KV().Get(ctx, scratchKey)
			if err != nil {
				return err
			}
			if got.Exists() {
				return errors.AssertionFailedf("expected no value, got %v", got)
			}
		}
		if err := txn.KV().Step(ctx, true /* allowReadTimestampStep */); err != nil {
			return err
		}
		{
			got, err := txn.KV().Get(ctx, scratchKey)
			if err != nil {
				return err
			}
			if got.ValueInt() != 1 {
				return errors.AssertionFailedf("expected 1, got %v", got)
			}
		}
		return nil
	}, isql.SteppingEnabled()))
}

// TestLivenessSessionExpiredErrorResultsInRestartAtSQLLayer ensures that if
// a transaction sees a deadline which is in the past of the current
// transaction timestamp, it'll propagate a retriable error. This should only
// happen in transient situations.
//
// Note that the sqlliveness session is not the only source of a deadline. In
// fact, it's probably more common that schema changes lead to descriptor
// deadlines kicking in. By default, at time of writing, sqlliveness sessions
// only apply to secondary tenants. The test leverages them because they are
// the easiest way to interact with the deadline at a session level.
func TestLivenessSessionExpiredErrorResultsInRestartAtSQLLayer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	var (
		toFail = struct {
			syncutil.Mutex
			remaining int
		}{}
		shouldFail = func() bool {
			toFail.Lock()
			defer toFail.Unlock()
			if toFail.remaining > 0 {
				toFail.remaining--
				return true
			}
			return false
		}
		checkFailed = func(t *testing.T) {
			toFail.Lock()
			defer toFail.Unlock()
			require.Zero(t, toFail.remaining)
		}
		setRemaining = func(t *testing.T, n int) {
			toFail.Lock()
			defer toFail.Unlock()
			toFail.remaining = n
		}
	)
	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			SQLExecutor: &sql.ExecutorTestingKnobs{
				ForceSQLLivenessSession: true,
			},
			SQLLivenessKnobs: &sqlliveness.TestingKnobs{
				SessionOverride: func(ctx context.Context) (sqlliveness.Session, error) {
					// Only client sessions have the client tag. We control the only
					// client session.
					tags := logtags.FromContext(ctx)
					if _, hasClient := tags.GetTag("client"); hasClient && shouldFail() {
						// This fake session is certainly expired.
						return &sqllivenesstestutils.FakeSession{
							ExpTS: hlc.Timestamp{WallTime: 1},
						}, nil
					}
					return nil, nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	tdb.Exec(t, "CREATE TABLE t (i INT PRIMARY KEY)")

	// Ensure that the internal retry works seamlessly
	t.Run("auto-retry", func(t *testing.T) {
		setRemaining(t, rand.Intn(20))
		tdb.Exec(t, `SELECT * FROM t`)
		checkFailed(t)
	})
	t.Run("explicit transaction", func(t *testing.T) {
		setRemaining(t, rand.Intn(20))
		require.NoError(t, crdb.ExecuteTx(ctx, sqlDB, nil, func(tx *gosql.Tx) error {
			_, err := tx.Exec("SELECT * FROM t")
			if err != nil {
				return err
			}
			_, err = tx.Exec(`INSERT INTO t VALUES (1)`)
			return err
		}))
		checkFailed(t)
	})
}
