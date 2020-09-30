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

// _ClosedTimestampServiceDesc192 is like _ClosedTimestamp_serviceDesc, except
// it uses the service name that 19.2 nodes were using. We've changed the
// service name in 20.1 by mistake.
var _ClosedTimestampServiceDesc192 = grpc.ServiceDesc{
	// Instead of "cockroach.kv.kvserver.ctupdate.ClosedTimestamp".
	ServiceName: "cockroach.storage.ctupdate.ClosedTimestamp",
	HandlerType: (*ClosedTimestampServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Get",
			Handler:       _ClosedTimestamp_Get_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "kv/kvserver/closedts/ctpb/service.proto",
}

// RegisterClosedTimestampServerUnder192Name is like
// RegisterClosedTimestampServer, except it uses a different service name - the
// old name that 19.2 nodes are using.
func RegisterClosedTimestampServerUnder192Name(s *grpc.Server, srv ClosedTimestampServer) {
	s.RegisterService(&_ClosedTimestampServiceDesc192, srv)
}
