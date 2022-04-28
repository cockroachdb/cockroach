// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/stretchr/testify/require"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func TestTxnFingerprintIDBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var txnFingerprintIDBuffer *TxnFingerprintIDBuffer

	datadriven.Walk(t, testutils.TestDataPath(t, "txn_fingerprint_id_buffer"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			ctx := context.Background()
			switch d.Cmd {
			case "init":
				var capacity int
				d.ScanArgs(t, "capacity", &capacity)

				st := &cluster.Settings{}
				monitor := mon.NewUnlimitedMonitor(
					ctx,
					"test",
					mon.MemoryResource,
					nil, /* currCount */
					nil, /* maxHist */
					math.MaxInt64,
					st,
				)
				txnFingerprintIDBuffer = NewTxnFingerprintIDBuffer(st, monitor)

				TxnFingerprintIDBufferCapacity.Override(ctx, &st.SV, int64(capacity))

				return fmt.Sprintf("buffer_size: %d", txnFingerprintIDBuffer.size())
			case "enqueue":
				var idStr string
				d.ScanArgs(t, "id", &idStr)

				id, err := strconv.ParseUint(idStr, 10, 64)
				require.NoError(t, err)
				txnFingerprintID := roachpb.TransactionFingerprintID(id)

				err = txnFingerprintIDBuffer.Enqueue(txnFingerprintID)
				require.NoError(t, err)

				return fmt.Sprintf("buffer_size: %d", txnFingerprintIDBuffer.size())
			case "dequeue":
				txnFingerprintID := txnFingerprintIDBuffer.dequeue()
				return fmt.Sprintf("txnFingerprintID: %d", txnFingerprintID)
			case "override":
				var capacity int
				d.ScanArgs(t, "capacity", &capacity)
				TxnFingerprintIDBufferCapacity.Override(ctx, &txnFingerprintIDBuffer.st.SV, int64(capacity))
				capacityClusterSetting := TxnFingerprintIDBufferCapacity.Get(&txnFingerprintIDBuffer.st.SV)
				return fmt.Sprintf("TxnFingerprintIDBufferCapacity: %d", capacityClusterSetting)
			case "show":
				return printTxnFingerprintIDBuffer(txnFingerprintIDBuffer)
			case "getAllTxnFingerprintIDs":
				txnFingerprintIDs := txnFingerprintIDBuffer.GetAllTxnFingerprintIDs()

				return fmt.Sprintf("%d\n", txnFingerprintIDs)
			}
			return ""

		})
	})
}

func printTxnFingerprintIDBuffer(buffer *TxnFingerprintIDBuffer) string {
	buffer.mu.Lock()
	defer buffer.mu.Unlock()

	var result []string
	for i, txnFingerprintID := range buffer.mu.data {
		result = append(result, fmt.Sprintf("%d -> %d", i, txnFingerprintID))
	}
	if len(result) == 0 {
		return "empty"
	}

	return strings.Join(result, "\n")
}
