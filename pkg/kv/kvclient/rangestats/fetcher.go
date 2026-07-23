// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package rangestats contains code to fetch range stats.
package rangestats

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
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
) ([]*kvpb.RangeStatsResponse, error) {
	var ba kv.Batch
	reqs := make([]kvpb.RangeStatsRequest, len(keys))
	for i, k := range keys {
		reqs[i].Key = k
		ba.AddRawRequest(&reqs[i])
	}
	if err := f.db.Run(ctx, &ba); err != nil {
		return nil, err
	}
	resps := make([]*kvpb.RangeStatsResponse, len(keys))
	for i, r := range ba.RawResponse().Responses {
		resps[i] = r.GetInner().(*kvpb.RangeStatsResponse)
	}
	return resps, nil
}
