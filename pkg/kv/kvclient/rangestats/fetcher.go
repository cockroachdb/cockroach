// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package rangestats contains code to fetch range stats.
package rangestats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

// Fetcher implements eval.RangeStatsFetcher.
type Fetcher struct {
	db *kv.DB
}

var _ eval.RangeStatsFetcher = (*Fetcher)(nil)

// NewFetcher constructs a new Fetcher.
func NewFetcher(db *kv.DB) *Fetcher {
	return &Fetcher{db: db}
}

// RangeStats is part of the eval.RangeStatsFetcher interface.
func (f Fetcher) RangeStats(
	ctx context.Context, keys ...roachpb.Key,
) ([]*roachpb.RangeStatsResponse, error) {
	var ba kv.Batch
	reqs := make([]roachpb.RangeStatsRequest, len(keys))
	for i, k := range keys {
		reqs[i].Key = k
		ba.AddRawRequest(&reqs[i])
	}
	if err := f.db.Run(ctx, &ba); err != nil {
		return nil, err
	}
	resps := make([]*roachpb.RangeStatsResponse, len(keys))
	for i, r := range ba.RawResponse().Responses {
		resps[i] = r.GetInner().(*roachpb.RangeStatsResponse)
	}
	return resps, nil
}
