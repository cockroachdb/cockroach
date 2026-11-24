// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestDebugResolveTxnID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var mu syncutil.Mutex
	var capturedTxnID uuid.UUID
	var capturedFingerprintID appstatspb.TransactionFingerprintID

	c := newCLITestWithArgs(TestCLIParams{T: t}, func(args *base.TestServerArgs) {
		args.Knobs.SQLExecutor = &sql.ExecutorTestingKnobs{
			BeforeTxnStatsRecorded: func(
				sessionData *sessiondata.SessionData,
				txnID uuid.UUID,
				txnFingerprintID appstatspb.TransactionFingerprintID,
				_ error,
			) {
				if strings.Contains(sessionData.ApplicationName, "resolveTxnIDTest") {
					mu.Lock()
					defer mu.Unlock()
					capturedTxnID = txnID
					capturedFingerprintID = txnFingerprintID
				}
			},
		}
	})
	defer c.Cleanup()
	s := c.Server.ApplicationLayer()

	// Execute a SQL statement with a unique application name so the hook
	// captures the txn ID.
	conn := s.SQLConn(t)
	_, err := conn.Exec("SET application_name = 'resolveTxnIDTest'")
	require.NoError(t, err)
	_, err = conn.Exec("SELECT 1")
	require.NoError(t, err)

	// Drain the txn ID cache write buffer so the entry is available for
	// lookup.
	sqlServer := s.SQLServer().(*sql.Server)
	sqlServer.GetTxnIDCache().DrainWriteBuffer()

	mu.Lock()
	txnID := capturedTxnID
	fingerprintID := capturedFingerprintID
	mu.Unlock()
	require.NotEqual(t, uuid.UUID{}, txnID, "expected to capture a txn ID")

	encoded := encoding.EncodeUint64Ascending(nil, uint64(fingerprintID))
	expectedOutput := fmt.Sprintf(
		"txn ID %v -> txn fingerprint ID %v -> transaction_fingerprint_id '\\x%x'",
		txnID, fingerprintID, encoded,
	)

	t.Run("with_node_id", func(t *testing.T) {
		output, err := c.RunWithCapture(
			fmt.Sprintf("debug resolve-txn-id %s 1", txnID),
		)
		require.NoError(t, err)
		require.Contains(t, output, expectedOutput)
		require.Contains(t, output, "n1: ")
	})

	t.Run("without_node_id", func(t *testing.T) {
		output, err := c.RunWithCapture(
			fmt.Sprintf("debug resolve-txn-id %s", txnID),
		)
		require.NoError(t, err)
		require.Contains(t, output, expectedOutput)
		require.Contains(t, output, "n1: ")
	})

	t.Run("unknown_txn_id", func(t *testing.T) {
		unknownID := uuid.MakeV4()
		output, err := c.RunWithCapture(
			fmt.Sprintf("debug resolve-txn-id %s 1", unknownID),
		)
		require.NoError(t, err)
		require.Contains(t, output, fmt.Sprintf("didn't resolve txn ID %v", unknownID))
	})

	t.Run("invalid_uuid", func(t *testing.T) {
		output, err := c.RunWithCapture("debug resolve-txn-id not-a-uuid")
		require.NoError(t, err)
		require.Contains(t, output, "ERROR")
	})
}
