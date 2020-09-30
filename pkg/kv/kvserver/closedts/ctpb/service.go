// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ctpb

import (
	"context"

	"google.golang.org/grpc"
)

// ClosedTimestampClient192 is like ClosedTimestampClient, except its Get192()
// method uses the RPC service that 19.2 had, not the 20.1 one. In 20.1 we've
// changed the name of the RPC service.
type ClosedTimestampClient192 interface {
	// Get192 calls Get() on the RPC service exposed by 19.2 nodes.
	Get192(ctx context.Context, opts ...grpc.CallOption) (ClosedTimestamp_GetClient, error)
}

func (c *closedTimestampClient) Get192(
	ctx context.Context, opts ...grpc.CallOption,
) (ClosedTimestamp_GetClient, error) {
	// Instead of "/cockroach.kv.kvserver.ctupdate.ClosedTimestamp/Get".
	stream, err := c.cc.NewStream(ctx, &_ClosedTimestamp_serviceDesc.Streams[0],
		"/cockroach.storage.ctupdate.ClosedTimestamp/Get", opts...)
	if err != nil {
		return nil, err
	}
	x := &closedTimestampGetClient{stream}
	return x, nil
}
