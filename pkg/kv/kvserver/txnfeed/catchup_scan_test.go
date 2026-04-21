// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestCatchUpScan is a data-driven test for the CatchUpScan function.
//
// Supported commands:
//
//	put-txn-record anchor=<key> mvcc-ts=<int> commit-ts=<int> status=<COMMITTED|ABORTED|PENDING> [id=<name>] [lock-spans=<key1-key2,key3-key4>]
//	  Writes a transaction record at the given MVCC timestamp. The id parameter
//	  assigns a short name for the transaction so it can be referenced later in
//	  del-txn-record and matched in output.
//
//	del-txn-record anchor=<key> mvcc-ts=<int> id=<name>
//	  Writes an MVCC tombstone for the transaction record at the given timestamp.
//
//	catchup-scan start-ts=<int> [range-start=<key>] [range-end=<key>]
//	  Runs CatchUpScan and prints emitted events sorted by anchor key.
func TestCatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "catchup_scan"),
		func(t *testing.T, path string) {
			eng := storage.NewDefaultInMemForTesting()
			defer eng.Close()

			// txnIDs maps short names to UUIDs for cross-referencing between
			// put-txn-record and del-txn-record commands.
			txnIDs := make(map[string]uuid.UUID)

			datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
				switch d.Cmd {
				case "put-txn-record":
					return putTxnRecord(t, ctx, eng, d, txnIDs)
				case "del-txn-record":
					return delTxnRecord(t, ctx, eng, d, txnIDs)
				case "catchup-scan":
					return runCatchUpScan(t, ctx, eng, d)
				default:
					t.Fatalf("unknown command: %s", d.Cmd)
					return ""
				}
			})
		})
}

func putTxnRecord(
	t *testing.T,
	ctx context.Context,
	eng storage.Engine,
	d *datadriven.TestData,
	txnIDs map[string]uuid.UUID,
) string {
	t.Helper()

	var anchor string
	var mvccTS, commitTS int
	var status string
	d.ScanArgs(t, "anchor", &anchor)
	d.ScanArgs(t, "mvcc-ts", &mvccTS)
	d.ScanArgs(t, "commit-ts", &commitTS)
	d.ScanArgs(t, "status", &status)

	var id string
	if d.HasArg("id") {
		d.ScanArgs(t, "id", &id)
	}

	txnID := uuid.MakeV4()
	if id != "" {
		if existing, ok := txnIDs[id]; ok {
			txnID = existing
		} else {
			txnIDs[id] = txnID
		}
	}

	var txnStatus roachpb.TransactionStatus
	switch status {
	case "COMMITTED":
		txnStatus = roachpb.COMMITTED
	case "ABORTED":
		txnStatus = roachpb.ABORTED
	case "PENDING":
		txnStatus = roachpb.PENDING
	case "STAGING":
		txnStatus = roachpb.STAGING
	default:
		t.Fatalf("unknown status: %s", status)
	}

	anchorKey := roachpb.Key(anchor)
	txn := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            anchorKey,
			ID:             txnID,
			WriteTimestamp: hlc.Timestamp{WallTime: int64(commitTS)},
		},
		Status: txnStatus,
	}

	if d.HasArg("lock-spans") {
		var lockSpansStr string
		d.ScanArgs(t, "lock-spans", &lockSpansStr)
		for _, spanStr := range strings.Split(lockSpansStr, ",") {
			parts := strings.SplitN(spanStr, "-", 2)
			span := roachpb.Span{Key: roachpb.Key(parts[0])}
			if len(parts) == 2 {
				span.EndKey = roachpb.Key(parts[1])
			}
			txn.LockSpans = append(txn.LockSpans, span)
		}
	}

	txnKey := keys.TransactionKey(anchorKey, txnID)
	require.NoError(t, storage.MVCCPutProto(
		ctx, eng, txnKey, hlc.Timestamp{WallTime: int64(mvccTS)}, &txn,
		storage.MVCCWriteOptions{},
	))
	return ""
}

func delTxnRecord(
	t *testing.T,
	ctx context.Context,
	eng storage.Engine,
	d *datadriven.TestData,
	txnIDs map[string]uuid.UUID,
) string {
	t.Helper()

	var anchor string
	var mvccTS int
	var id string
	d.ScanArgs(t, "anchor", &anchor)
	d.ScanArgs(t, "mvcc-ts", &mvccTS)
	d.ScanArgs(t, "id", &id)

	txnID, ok := txnIDs[id]
	require.True(t, ok, "unknown txn id: %s", id)

	txnKey := keys.TransactionKey(roachpb.Key(anchor), txnID)
	_, _, err := storage.MVCCDelete(
		ctx, eng, txnKey, hlc.Timestamp{WallTime: int64(mvccTS)},
		storage.MVCCWriteOptions{},
	)
	require.NoError(t, err)
	return ""
}

func runCatchUpScan(
	t *testing.T, ctx context.Context, eng storage.Engine, d *datadriven.TestData,
) string {
	t.Helper()

	var startTS int
	d.ScanArgs(t, "start-ts", &startTS)

	rangeStart := roachpb.RKey("a")
	rangeEnd := roachpb.RKey("z")
	if d.HasArg("range-start") {
		var s string
		d.ScanArgs(t, "range-start", &s)
		rangeStart = roachpb.RKey(s)
	}
	if d.HasArg("range-end") {
		var s string
		d.ScanArgs(t, "range-end", &s)
		rangeEnd = roachpb.RKey(s)
	}

	snap := eng.NewSnapshot()
	defer snap.Close()

	var events []*kvpb.TxnFeedEvent
	err := CatchUpScan(ctx, snap, rangeStart, rangeEnd,
		hlc.Timestamp{WallTime: int64(startTS)},
		func(event *kvpb.TxnFeedEvent) error {
			events = append(events, event)
			return nil
		},
	)
	require.NoError(t, err)

	if len(events) == 0 {
		return "<no events>"
	}

	// Sort by anchor key for deterministic output.
	sort.Slice(events, func(i, j int) bool {
		return events[i].Committed.AnchorKey.Compare(events[j].Committed.AnchorKey) < 0
	})

	var b strings.Builder
	for _, ev := range events {
		c := ev.Committed
		fmt.Fprintf(&b, "committed anchor=%s commit-ts=%d",
			string(c.AnchorKey), c.CommitTimestamp.WallTime)
		if len(c.WriteSpans) > 0 {
			var spans []string
			for _, sp := range c.WriteSpans {
				if sp.EndKey != nil {
					spans = append(spans, fmt.Sprintf("%s-%s",
						string(sp.Key), string(sp.EndKey)))
				} else {
					spans = append(spans, string(sp.Key))
				}
			}
			fmt.Fprintf(&b, " write-spans=[%s]", strings.Join(spans, ","))
		}
		b.WriteByte('\n')
	}
	return b.String()
}
