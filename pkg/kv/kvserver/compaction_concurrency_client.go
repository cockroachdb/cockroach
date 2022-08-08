// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/errors"
)

// CompactionConcurrencyClient is used to request compaction for a span
// of data on a store.
type CompactionConcurrencyClient struct {
	nd *nodedialer.Dialer
}

// NewCompactionConcurrencyClient constructs a new CompactionConcurrencyClient.
func NewCompactionConcurrencyClient(nd *nodedialer.Dialer) *CompactionConcurrencyClient {
	return &CompactionConcurrencyClient{nd: nd}
}

// SetCompactionConcurrency is a tree.CompactionConcurrencyFunc.
func (c *CompactionConcurrencyClient) SetCompactionConcurrency(
	ctx context.Context, nodeID, storeID int32, compactionConcurrency uint64,
) (uint64, error) {
	conn, err := c.nd.Dial(ctx, roachpb.NodeID(nodeID), rpc.DefaultClass)
	if err != nil {
		return 0, errors.Wrapf(err, "could not dial node ID %d", nodeID)
	}
	client := NewPerStoreClient(conn)
	req := &CompactionConcurrencyRequest{
		StoreRequestHeader: StoreRequestHeader{
			NodeID:  roachpb.NodeID(nodeID),
			StoreID: roachpb.StoreID(storeID),
		},
		CompactionConcurrency: compactionConcurrency,
	}
	resp, err := client.SetCompactionConcurrency(ctx, req)
	return resp.OldCompactionConcurrency, err
}
