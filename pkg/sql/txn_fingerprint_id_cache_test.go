// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestTxnFingerprintIDCacheDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	memMonitor := execinfra.NewTestMemMonitor(ctx, st)
	memAccount := memMonitor.MakeBoundAccount()
	var txnFingerprintIDCache *TxnFingerprintIDCache

	datadriven.Walk(t, datapathutils.TestDataPath(t, "txn_fingerprint_id_cache"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				var capacity int
				d.ScanArgs(t, "capacity", &capacity)

				txnFingerprintIDCache = NewTxnFingerprintIDCache(ctx, st, &memAccount)

				TxnFingerprintIDCacheCapacity.Override(ctx, &st.SV, int64(capacity))

				return fmt.Sprintf("size: %d", txnFingerprintIDCache.size())

			case "override":
				var capacity int
				d.ScanArgs(t, "capacity", &capacity)
				TxnFingerprintIDCacheCapacity.Override(ctx, &txnFingerprintIDCache.st.SV, int64(capacity))
				capacityClusterSetting := TxnFingerprintIDCacheCapacity.Get(&txnFingerprintIDCache.st.SV)
				return fmt.Sprintf("TxnFingerprintIDCacheCapacity: %d", capacityClusterSetting)

			case "enqueue":
				var idStr string
				d.ScanArgs(t, "id", &idStr)

				id, err := strconv.ParseUint(idStr, 10, 64)
				require.NoError(t, err)
				txnFingerprintID := appstatspb.TransactionFingerprintID(id)

				err = txnFingerprintIDCache.Add(ctx, txnFingerprintID)
				require.NoError(t, err)

				return fmt.Sprintf("size: %d", txnFingerprintIDCache.size())

			case "show":
				return printTxnFingerprintIDCache(txnFingerprintIDCache)

			case "accounting":
				return fmt.Sprintf("%d bytes", memAccount.Used())

			default:
				return ""
			}
		})
	})
}

func printTxnFingerprintIDCache(txnFingerprintCache *TxnFingerprintIDCache) string {
	txnFingerprintIDs := txnFingerprintCache.GetAllTxnFingerprintIDs()

	return fmt.Sprintf("%d", txnFingerprintIDs)
}

func TestTxnFingerprintIDCache(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	txnFingerprintIDsRecorded := make([]appstatspb.TransactionFingerprintID, 0)
	appName := "testTxnFingerprintIDCache"

	var params base.TestServerArgs
	params.Knobs.SQLExecutor = &ExecutorTestingKnobs{
		BeforeTxnStatsRecorded: func(
			sessionData *sessiondata.SessionData,
			_ uuid.UUID,
			txnFingerprintID appstatspb.TransactionFingerprintID,
			_ error,
		) {
			if !sessionData.Internal {
				// Record every query we issue through our sql connection.
				txnFingerprintIDsRecorded = append(txnFingerprintIDsRecorded, txnFingerprintID)
			}
		},
	}

	testServer, sqlConn, _ := serverutils.StartServer(t, params)

	defer func() {
		require.NoError(t, sqlConn.Close())
		testServer.Stopper().Stop(ctx)
	}()

	testConn := sqlutils.MakeSQLRunner(sqlConn)

	testConn.Exec(t, "SET application_name = $1", appName)
	testConn.Exec(t, "CREATE TABLE test AS SELECT generate_series(1, 10)")
	testConn.Exec(t, "SELECT * FROM test")
	testConn.Exec(t, "BEGIN; SELECT 1; SELECT 1, 2, 3; COMMIT;")

	sessions := testServer.SQLServer().(*Server).GetExecutorConfig().SessionRegistry.SerializeAll()

	var session *serverpb.Session
	for i, s := range sessions {
		if s.ApplicationName == appName {
			session = &sessions[i]
			break
		}
	}
	require.NotNil(t, session)

	sort.Slice(session.TxnFingerprintIDs, func(i, j int) bool {
		return session.TxnFingerprintIDs[i] < session.TxnFingerprintIDs[j]
	})

	sort.Slice(txnFingerprintIDsRecorded, func(i, j int) bool {
		return txnFingerprintIDsRecorded[i] < txnFingerprintIDsRecorded[j]
	})

	require.Equal(t, txnFingerprintIDsRecorded, session.TxnFingerprintIDs)
}
