// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/internal/client/requestbatcher"
	"github.com/cockroachdb/cockroach/pkg/kv/kvbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// TxnHeartbeatBatcher XXX:
type TxnHeartbeatBatcher struct {
	cache   kvbase.RangeDescriptorCache
	clock   *hlc.Clock
	batcher *requestbatcher.RequestBatcher
}

// NewTxnHeartbeatBatcher XXX:
func NewTxnHeartbeatBatcher(cache kvbase.RangeDescriptorCache, clock *hlc.Clock, batcherCfg requestbatcher.Config) *TxnHeartbeatBatcher {
	batcher := requestbatcher.New(batcherCfg)
	return &TxnHeartbeatBatcher{
		cache:   cache,
		clock:   clock,
		batcher: batcher,
	}
}

func (t *TxnHeartbeatBatcher) Send(ctx context.Context, txn *roachpb.Transaction) (*roachpb.Transaction, *roachpb.TransactionAbortedError, error) {
	rangeCacheEntry, err := t.cache.Lookup(ctx, txn.Key)
	if err != nil {
		return nil, nil, err
	}
	// Add HeartbeatTxnRequest to the heartbeat batcher.
	req := &roachpb.HeartbeatTxnRequest{
		RequestHeader: roachpb.RequestHeader{
			Key: txn.Key,
		},
		Now:          t.clock.Now(),
		HeartbeatTxn: txn,
	}
	resp, err := t.batcher.Send(ctx, rangeCacheEntry.Desc().RangeID, req)
	if err != nil {
		return nil, nil, err
	}

	heartbeatResp := resp.(*roachpb.HeartbeatTxnResponse)
	return heartbeatResp.HeartbeatTxn, heartbeatResp.Error, nil
}
