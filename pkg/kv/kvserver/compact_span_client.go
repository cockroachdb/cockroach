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

// CompactEngineSpanClient is used to request compaction for a span
// of data on a store.
type CompactEngineSpanClient struct {
	nd *nodedialer.Dialer
}

// NewCompactEngineSpanClient constructs a new CompactEngineSpanClient.
func NewCompactEngineSpanClient(nd *nodedialer.Dialer) *CompactEngineSpanClient {
	return &CompactEngineSpanClient{nd: nd}
}

// CompactEngineSpan is a tree.CompactEngineSpanFunc.
func (c *CompactEngineSpanClient) CompactEngineSpan(
	ctx context.Context, nodeID, storeID int32, startKey, endKey []byte,
) error {
	conn, err := c.nd.Dial(ctx, roachpb.NodeID(nodeID), rpc.DefaultClass)
	if err != nil {
		return errors.Wrapf(err, "could not dial node ID %d", nodeID)
	}
	client := NewPerStoreClient(conn)
	req := &CompactEngineSpanRequest{
		StoreRequestHeader: StoreRequestHeader{
			NodeID:  roachpb.NodeID(nodeID),
			StoreID: roachpb.StoreID(storeID),
		},
		Span: roachpb.Span{Key: roachpb.Key(startKey), EndKey: roachpb.Key(endKey)},
	}
	_, err = client.CompactEngineSpan(ctx, req)
	return err
}
