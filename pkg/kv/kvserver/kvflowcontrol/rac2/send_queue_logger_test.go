// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"
	"math"
	"regexp"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestSendQueueLoggerTest(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	defer s.Close(t)

	testStartTs := timeutil.Now()
	rss11_100 := ReplicaSendStreamStats{
		Stream: kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(1),
			StoreID:  1,
		},
		ReplicaSendQueueStats: ReplicaSendQueueStats{
			SendQueueBytes: 100,
		},
	}
	rss22_200 := ReplicaSendStreamStats{
		Stream: kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(2),
			StoreID:  2,
		},
		ReplicaSendQueueStats: ReplicaSendQueueStats{
			SendQueueBytes: 200,
		},
	}
	rss33_50 := ReplicaSendStreamStats{
		Stream: kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(3),
			StoreID:  3,
		},
		ReplicaSendQueueStats: ReplicaSendQueueStats{
			SendQueueBytes: 50,
		},
	}
	rss44_25 := ReplicaSendStreamStats{
		Stream: kvflowcontrol.Stream{
			TenantID: roachpb.MustMakeTenantID(4),
			StoreID:  4,
		},
		ReplicaSendQueueStats: ReplicaSendQueueStats{
			SendQueueBytes: 25,
		},
	}
	sql := NewSendQueueLogger(2)

	require.True(t, sql.TryStartLog())
	// Empty stats are ok.
	stats := RangeSendStreamStats{}
	sql.ObserveRangeStats(&stats)
	stats = RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{rss22_200, rss33_50},
	}
	sql.ObserveRangeStats(&stats)
	stats = RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{rss11_100, rss22_200},
	}
	sql.ObserveRangeStats(&stats)
	stats = RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{rss11_100},
	}
	sql.ObserveRangeStats(&stats)
	stats = RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{rss33_50, rss44_25},
	}
	sql.ObserveRangeStats(&stats)
	sql.FinishLog(context.Background())

	// Cannot log again immediately.
	require.False(t, sql.TryStartLog())
	// Call FinishLog again to ensure no logging happens.
	sql.FinishLog(context.Background())

	log.FlushFiles()
	entries, err := log.FetchEntriesFromFiles(testStartTs.UnixNano(),
		math.MaxInt64, 10,
		regexp.MustCompile(`send_queue_logger\.go`),
		log.WithMarkedSensitiveData)
	require.NoError(t, err)
	require.Equal(t, 1, len(entries))
	require.Contains(t, entries[0].Message,
		"send queues: t2/s2: 400 B, t1/s1: 200 B, + 125 B more bytes across 2 streams")
}
