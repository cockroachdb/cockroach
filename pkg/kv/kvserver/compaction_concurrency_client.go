// Copyright 2022 The Cockroach Authors.
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

// CompactionConcurrencyClient is used to temporarily modify the
// compaction concurrency of a client until the request is cancelled.
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
) error {
	conn, err := c.nd.Dial(ctx, roachpb.NodeID(nodeID), rpc.DefaultClass)
	if err != nil {
		return errors.Wrapf(err, "could not dial node ID %d", nodeID)
	}
	client := NewPerStoreClient(conn)
	req := &CompactionConcurrencyRequest{
		StoreRequestHeader: StoreRequestHeader{
			NodeID:  roachpb.NodeID(nodeID),
			StoreID: roachpb.StoreID(storeID),
		},
		CompactionConcurrency: compactionConcurrency,
	}
	_, err = client.SetCompactionConcurrency(ctx, req)
	if err != nil {
		return err
	}
	return nil
}
