// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
