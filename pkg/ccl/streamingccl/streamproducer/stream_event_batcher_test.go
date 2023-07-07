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

func TestStreamEventBatcher(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	seb := makeStreamEventBatcher()

	var runningSize int
	kv := roachpb.KeyValue{Key: roachpb.Key{'1'}}
	runningSize += kv.Size()
	seb.addKV(&kv)
	require.Equal(t, 1, len(seb.batch.KeyValues))
	require.Equal(t, runningSize, seb.getSize())

	delRange := kvpb.RangeFeedDeleteRange{Span: roachpb.Span{Key: roachpb.KeyMin}, Timestamp: hlc.Timestamp{}}
	runningSize += delRange.Size()
	seb.addDelRange(&delRange)
	require.Equal(t, 1, len(seb.batch.DelRanges))
	require.Equal(t, runningSize, seb.getSize())

	sst := replicationtestutils.SSTMaker(t, []roachpb.KeyValue{kv})
	runningSize += sst.Size()
	seb.addSST(&sst)
	require.Equal(t, 1, len(seb.batch.Ssts))
	require.Equal(t, runningSize, seb.getSize())

	seb.reset()
	require.Equal(t, 0, seb.getSize())
	require.Equal(t, 0, len(seb.batch.KeyValues))
	require.Equal(t, 0, len(seb.batch.Ssts))
	require.Equal(t, 0, len(seb.batch.DelRanges))
}
