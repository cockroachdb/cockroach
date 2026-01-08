// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestSendQueueLogger(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s := log.ScopeWithoutShowLogs(t)
	defer s.Close(t)

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
	var output redact.RedactableString
	sql.testingLog = func(ctx context.Context, buf *redact.StringBuilder) {
		if len(output) > 0 {
			output += "\n"
		}
		output += redact.Sprintf(sendQueueLogFormat, buf)
	}
	ctx := context.Background()
	coll, ok := sql.TryStartLog()

	require.True(t, ok)
	// Empty stats are ok.
	stats := RangeSendStreamStats{}
	coll.ObserveRangeStats(&stats)
	stats = RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{rss22_200, rss33_50},
	}
	coll.ObserveRangeStats(&stats)
	stats = RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{rss11_100, rss22_200},
	}
	coll.ObserveRangeStats(&stats)
	stats = RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{rss11_100},
	}
	coll.ObserveRangeStats(&stats)
	stats = RangeSendStreamStats{
		internal: []ReplicaSendStreamStats{rss33_50, rss44_25},
	}
	coll.ObserveRangeStats(&stats)
	sql.FinishLog(ctx, coll)

	// Cannot log again immediately.
	_, ok = sql.TryStartLog()
	require.False(t, ok)
	// Call FinishLog again to ensure no logging happens.
	sql.FinishLog(ctx, SendQueueLoggerCollector{})

	echotest.Require(t, string(output), datapathutils.TestDataPath(t, t.Name()+".txt"))
}
