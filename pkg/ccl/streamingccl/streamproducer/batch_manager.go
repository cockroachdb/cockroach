// Copyright 2023 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamproducer

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type batchManager struct {
	batch streampb.StreamEvent_Batch
	size  int
}

func makeBatchManager() *batchManager {
	return &batchManager{
		batch: streampb.StreamEvent_Batch{},
	}
}

func (bm *batchManager) reset() {
	bm.size = 0
	bm.batch.KeyValues = bm.batch.KeyValues[:0]
	bm.batch.Ssts = bm.batch.Ssts[:0]
	bm.batch.DelRanges = bm.batch.DelRanges[:0]
}

func (bm *batchManager) addSST(sst *kvpb.RangeFeedSSTable) {
	bm.batch.Ssts = append(bm.batch.Ssts, *sst)
	bm.size += sst.Size()
}

func (bm *batchManager) addKV(kv *roachpb.KeyValue) {
	bm.batch.KeyValues = append(bm.batch.KeyValues, *kv)
	bm.size += kv.Size()
}

func (bm *batchManager) addDelRange(d *kvpb.RangeFeedDeleteRange) {
	// DelRange's span is already trimmed to enclosed within
	// the subscribed span, just emit it.
	bm.batch.DelRanges = append(bm.batch.DelRanges, *d)
	bm.size += d.Size()
}

func (bm *batchManager) getSize() int {
	return bm.size
}
