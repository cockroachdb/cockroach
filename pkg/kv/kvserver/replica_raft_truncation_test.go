// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/logstore"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestHandleTruncatedStateBelowRaft(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This test verifies the expected behavior of the downstream-of-Raft log
	// truncation code.

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t, "truncated_state"), func(t *testing.T, path string) {
		const rangeID = 12
		loader := logstore.NewStateLoader(rangeID)
		prefixBuf := &loader.RangeIDPrefixBuf
		eng := storage.NewDefaultInMemForTesting()
		defer eng.Close()

		var prevTruncatedState kvserverpb.RaftTruncatedState
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "prev":
				var v uint64
				d.ScanArgs(t, "index", &v)
				prevTruncatedState.Index = kvpb.RaftIndex(v)
				d.ScanArgs(t, "term", &v)
				prevTruncatedState.Term = kvpb.RaftTerm(v)
				return ""

			case "put":
				var index, term uint64
				d.ScanArgs(t, "index", &index)
				d.ScanArgs(t, "term", &term)

				truncState := &kvserverpb.RaftTruncatedState{
					Index: kvpb.RaftIndex(index),
					Term:  kvpb.RaftTerm(term),
				}

				require.NoError(t, loader.SetRaftTruncatedState(ctx, eng, truncState))
				return ""

			case "handle":
				var buf bytes.Buffer
				var index, term uint64
				d.ScanArgs(t, "index", &index)
				d.ScanArgs(t, "term", &term)

				suggestedTruncatedState := &kvserverpb.RaftTruncatedState{
					Index: kvpb.RaftIndex(index),
					Term:  kvpb.RaftTerm(term),
				}
				currentTruncatedState, err := loader.LoadRaftTruncatedState(ctx, eng)
				require.NoError(t, err)

				// Write log entries at start, middle, end, and above the truncated interval.
				if suggestedTruncatedState.Index > currentTruncatedState.Index {
					indexes := []kvpb.RaftIndex{
						currentTruncatedState.Index + 1,                                       // start
						(suggestedTruncatedState.Index + currentTruncatedState.Index + 1) / 2, // middle
						suggestedTruncatedState.Index,                                         // end
						suggestedTruncatedState.Index + 1,                                     // new head
					}
					for _, idx := range indexes {
						meta := enginepb.MVCCMetadata{RawBytes: make([]byte, 8)}
						binary.BigEndian.PutUint64(meta.RawBytes, uint64(idx))
						value, err := protoutil.Marshal(&meta)
						require.NoError(t, err)
						require.NoError(t, eng.PutUnversioned(prefixBuf.RaftLogKey(idx), value))
					}
				}

				// Apply truncation.
				apply, err := handleTruncatedStateBelowRaftPreApply(ctx, currentTruncatedState, suggestedTruncatedState, loader, eng)
				require.NoError(t, err)
				fmt.Fprintf(&buf, "apply: %t\n", apply)

				// Check the truncated state.
				key := keys.RaftTruncatedStateKey(rangeID)
				var truncatedState kvserverpb.RaftTruncatedState
				ok, err := storage.MVCCGetProto(ctx, eng, key, hlc.Timestamp{}, &truncatedState, storage.MVCCGetOptions{})
				require.NoError(t, err)
				require.True(t, ok)
				fmt.Fprintf(&buf, "state: %s -> index=%d term=%d\n", key, truncatedState.Index, truncatedState.Term)

				// Find the first untruncated log entry (the log head).
				res, err := storage.MVCCScan(ctx, eng,
					prefixBuf.RaftLogPrefix().Clone(),
					prefixBuf.RaftLogPrefix().PrefixEnd(),
					hlc.Timestamp{},
					storage.MVCCScanOptions{MaxKeys: 1})
				require.NoError(t, err)
				var head roachpb.Key
				if len(res.KVs) > 0 {
					head = res.KVs[0].Key
				}
				fmt.Fprintf(&buf, "head: %s\n", head)

				return buf.String()

			default:
				return fmt.Sprintf("unsupported: %s", d.Cmd)
			}
		})
	})
}
