// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package logstore

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftentry"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftlog"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/stretchr/testify/require"
)

type discardBatch struct {
	storage.Batch
}

func (b *discardBatch) Commit(bool) error {
	return nil
}

type noopSyncCallback struct{}

func (noopSyncCallback) OnLogSync(context.Context, MsgStorageAppendDone, storage.BatchCommitStats) {}

func BenchmarkLogStore_StoreEntries(b *testing.B) {
	defer log.Scope(b).Close(b)
	const kb = 1 << 10
	const mb = 1 << 20
	for _, bytes := range []int64{1 * kb, 256 * kb, 512 * kb, 1 * mb, 2 * mb} {
		b.Run(fmt.Sprintf("bytes=%s", humanizeutil.IBytes(bytes)), func(b *testing.B) {
			runBenchmarkLogStore_StoreEntries(b, bytes)
		})
	}
}

func runBenchmarkLogStore_StoreEntries(b *testing.B, bytes int64) {
	ctx := context.Background()
	const tenMB = 10 * 1 << 20
	ec := raftentry.NewCache(tenMB)
	const rangeID = 1
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	st := cluster.MakeTestingClusterSettings()
	enableNonBlockingRaftLogSync.Override(ctx, &st.SV, false)
	s := LogStore{
		RangeID:     rangeID,
		Engine:      eng,
		StateLoader: NewStateLoader(rangeID),
		EntryCache:  ec,
		Settings:    st,
		Metrics: Metrics{
			RaftLogCommitLatency: metric.NewHistogram(metric.HistogramOptions{
				Mode:         metric.HistogramModePrometheus,
				Metadata:     metric.Metadata{},
				Duration:     10 * time.Second,
				BucketConfig: metric.IOLatencyBuckets,
			}),
		},
	}

	rs := RaftState{
		LastTerm: 1,
		ByteSize: 0,
	}
	var ents []raftpb.Entry
	data := make([]byte, bytes)
	rand.New(rand.NewSource(0)).Read(data)
	ents = append(ents, raftpb.Entry{
		Term:  1,
		Index: 1,
		Type:  raftpb.EntryNormal,
		Data: raftlog.EncodeCommandBytes(
			raftlog.EntryEncodingStandardWithoutAC, "deadbeef", data, 0 /* pri */),
	})
	stats := &AppendStats{}

	b.ReportAllocs()
	b.ResetTimer()
	// Use a batch that ignores Commit() so that we can run as many iterations as
	// we like without building up large amounts of data in the Engine. This hides
	// some allocations that would occur down in pebble but that's fine, we're not
	// here to measure those.
	batch := &discardBatch{}
	for i := 0; i < b.N; i++ {
		batch.Batch = newStoreEntriesBatch(eng)
		m := MsgStorageAppend{Entries: ents}
		cb := noopSyncCallback{}
		var err error
		rs, err = s.storeEntriesAndCommitBatch(ctx, rs, m, cb, stats, batch)
		if err != nil {
			b.Fatal(err)
		}
		ents[0].Index++
	}
	require.EqualValues(b, b.N, rs.LastIndex)
}
