// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestBatchManager(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	bm := makeBatchManager()

	var runningSize int
	kv := roachpb.KeyValue{Key: roachpb.Key{'1'}}
	runningSize += kv.Size()
	bm.addKV(&kv)
	require.Equal(t, 1, len(bm.batch.KeyValues))
	require.Equal(t, runningSize, bm.getSize())

	delRange := kvpb.RangeFeedDeleteRange{Span: roachpb.Span{Key: roachpb.KeyMin}, Timestamp: hlc.Timestamp{}}
	runningSize += delRange.Size()
	bm.addDelRange(&delRange)
	require.Equal(t, 1, len(bm.batch.DelRanges))
	require.Equal(t, runningSize, bm.getSize())

	sst := replicationtestutils.SSTMaker(t, []roachpb.KeyValue{kv})
	runningSize += sst.Size()
	bm.addSST(&sst)
	require.Equal(t, 1, len(bm.batch.Ssts))
	require.Equal(t, runningSize, bm.getSize())

	bm.reset()
	require.Equal(t, 0, bm.getSize())
	require.Equal(t, 0, len(bm.batch.KeyValues))
	require.Equal(t, 0, len(bm.batch.Ssts))
	require.Equal(t, 0, len(bm.batch.DelRanges))
}
