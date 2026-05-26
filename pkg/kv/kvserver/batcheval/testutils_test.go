// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
)

type kvs = storageutils.KVs

var (
	pointKV            = storageutils.PointKV
	pointKVWithLocalTS = storageutils.PointKVWithLocalTS
	rangeKey           = storageutils.RangeKey
	rangeKV            = storageutils.RangeKV
	rangeKVWithLocalTS = storageutils.RangeKVWithLocalTS
	wallTS             = storageutils.WallTS
	scanEngine         = storageutils.ScanEngine
)

type wrappedBatch struct {
	storage.Batch
	clearIterCount int
}

func (wb *wrappedBatch) ClearMVCCIteratorRange(
	start, end roachpb.Key, pointKeys, rangeKeys bool,
) error {
	wb.clearIterCount++
	return wb.Batch.ClearMVCCIteratorRange(start, end, pointKeys, rangeKeys)
}
