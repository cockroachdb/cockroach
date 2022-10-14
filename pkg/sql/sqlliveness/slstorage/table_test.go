// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slstorage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness/slstorage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// test random read/writes to the table while stepping through the migration state
// ensure the table can be converted to regional by row in the final migration state

func TestSqlLivenessTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	tDB := sqlutils.MakeSQLRunner(sqlDB)

	t0 := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	timeSource := timeutil.NewManualTime(t0)
	clock := hlc.NewClock(timeSource, base.DefaultMaxClockOffset)

	setup := func(t *testing.T) slstorage.Table {
		dbName := t.Name()
		tableID := newSystemTable(t, tDB, dbName, "sqlliveness", systemschema.SqllivenessTableSchema)
		return slstorage.MakeTable(keys.SystemSQLCodec, tableID)
	}

	t.Run("NotFound", func(t *testing.T) {
		table := setup(t)
		session, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			exists, _, err := table.GetExpiration(ctx, txn, session)
			if err != nil {
				return err
			}
			require.False(t, exists)
			return nil
		}))
	})

	t.Run("CreateAndUpdate", func(t *testing.T) {
		table := setup(t)
		session, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)

		writeExpiration := clock.Now().Add(13, 37)
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return table.SetExpiration(ctx, txn, session, writeExpiration)
		}))

		var readExpiration hlc.Timestamp
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			exists, expiration, err := table.GetExpiration(ctx, txn, session)
			if err != nil {
				return err
			}
			require.True(t, exists)
			readExpiration = expiration
			return nil
		}))

		require.Equal(t, readExpiration, writeExpiration)
		require.NoError(t, err)
	})

	t.Run("DeleteSession", func(t *testing.T) {
		table := setup(t)
		session, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)

		writeExpiration := clock.Now().Add(10, 00)
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return table.SetExpiration(ctx, txn, session, writeExpiration)
		}))
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return table.Delete(ctx, txn, session)
		}))
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			exists, _, err := table.GetExpiration(ctx, txn, session)
			if err != nil {
				return err
			}
			require.False(t, exists)
			return nil
		}))
	})

	t.Run("RbtSql", func(t *testing.T) {
		table := setup(t)
		session, err := slstorage.MakeSessionID(enum.One, uuid.MakeV4())
		require.NoError(t, err)

		writeExpiration := clock.Now().Add(10, 00)
		require.NoError(t, kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return table.SetExpiration(ctx, txn, session, writeExpiration)
		}))

		var rawExpiration apd.Decimal
		var readSession sqlliveness.SessionID

		row := sqlDB.QueryRowContext(ctx, fmt.Sprintf(`SELECT session_id, expiration FROM "%s".sqlliveness`, t.Name()))
		require.NoError(t, row.Err())
		require.NoError(t, row.Scan(&readSession, &rawExpiration))

		readExpiration, err := hlc.DecimalToHLC(&rawExpiration)
		require.NoError(t, err)

		require.Equal(t, writeExpiration, readExpiration)
		require.Equal(t, session, readSession)
	})
}
