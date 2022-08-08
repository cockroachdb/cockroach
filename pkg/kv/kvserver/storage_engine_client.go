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

// StorageEngineClient is used to connect and make requests to a store.
type StorageEngineClient struct {
	nd *nodedialer.Dialer
}

// NewStorageEngineClient constructs a new StorageEngineClient.
func NewStorageEngineClient(nd *nodedialer.Dialer) *StorageEngineClient {
	return &StorageEngineClient{nd: nd}
}

// CompactEngineSpan is a tree.CompactEngineSpanFunc.
func (c *StorageEngineClient) CompactEngineSpan(
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

// SetCompactionConcurrency is a tree.CompactionConcurrencyFunc.
func (c *StorageEngineClient) SetCompactionConcurrency(
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
