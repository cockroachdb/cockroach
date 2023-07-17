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

type streamEventBatcher struct {
	batch streampb.StreamEvent_Batch
	size  int
}

func makeStreamEventBatcher() *streamEventBatcher {
	return &streamEventBatcher{
		batch: streampb.StreamEvent_Batch{},
	}
}

func (seb *streamEventBatcher) reset() {
	seb.size = 0
	seb.batch.KeyValues = seb.batch.KeyValues[:0]
	seb.batch.Ssts = seb.batch.Ssts[:0]
	seb.batch.DelRanges = seb.batch.DelRanges[:0]
}

func (seb *streamEventBatcher) addSST(sst *kvpb.RangeFeedSSTable) {
	seb.batch.Ssts = append(seb.batch.Ssts, *sst)
	seb.size += sst.Size()
}

func (seb *streamEventBatcher) addKV(kv *roachpb.KeyValue) {
	seb.batch.KeyValues = append(seb.batch.KeyValues, *kv)
	seb.size += kv.Size()
}

func (seb *streamEventBatcher) addDelRange(d *kvpb.RangeFeedDeleteRange) {
	// DelRange's span is already trimmed to enclosed within
	// the subscribed span, just emit it.
	seb.batch.DelRanges = append(seb.batch.DelRanges, *d)
	seb.size += d.Size()
}

func (seb *streamEventBatcher) getSize() int {
	return seb.size
}
