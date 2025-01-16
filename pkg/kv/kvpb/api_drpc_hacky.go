// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This file was manually generated with the DRPC protogen plugin using a dummy
// `api.proto` that includes a subset of relevant service methods.
//
// For instance, to generate this file, following proto file was used:
//
// -- api.proto -- begin --
// 	syntax = "proto3";
// 	package cockroach.kv.kvpb;
// 	option go_package = "github.com/cockroachdb/cockroach/pkg/kv/kvpb";
// 	service Batch {
//    	rpc Batch (BatchRequest) returns (BatchResponse) {}
//    	rpc BatchStream (stream BatchRequest) returns (stream BatchResponse) {}
// 	}
// 	message BatchRequest{}
// 	message BatchResponse{}
// -- api.proto -- end --
//
// NB: The use of empty BatchRequest and BatchResponse messages is a deliberate
// decision to avoid dependencies.
//
//
// To generate this file using DRPC protogen plugin from the dummy `api.proto`
// defined above, use the following command:
//
// ```
// protoc --gogo_out=paths=source_relative:. \
//	--go-drpc_out=paths=source_relative,protolib=github.com/gogo/protobuf:. \
//	api.proto
// ```
//
// NB: Make sure you have `protoc` installed and `protoc-gen-gogoroach` is
// built from $COCKROACH_SRC/pkg/cmd/protoc-gen-gogoroach.
//
// This code-gen should be automated as part of productionizing drpc.

package kvpb

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"storj.io/drpc"
	"storj.io/drpc/drpcerr"
)

type drpcEncoding_File_api_proto struct{}

func (drpcEncoding_File_api_proto) Marshal(msg drpc.Message) ([]byte, error) {
	return protoutil.Marshal(msg.(protoutil.Message))
}

func (drpcEncoding_File_api_proto) Unmarshal(buf []byte, msg drpc.Message) error {
	return protoutil.Unmarshal(buf, msg.(protoutil.Message))
}

type DRPCBatchClient interface {
	DRPCConn() drpc.Conn

	Batch(ctx context.Context, in *BatchRequest) (*BatchResponse, error)
	BatchStream(ctx context.Context) (DRPCBatch_BatchStreamClient, error)
}

type drpcBatchClient struct {
	cc drpc.Conn
}

func NewDRPCBatchClient(cc drpc.Conn) DRPCBatchClient {
	return &drpcBatchClient{cc}
}

func (c *drpcBatchClient) DRPCConn() drpc.Conn { return c.cc }

func (c *drpcBatchClient) Batch(ctx context.Context, in *BatchRequest) (*BatchResponse, error) {
	out := new(BatchResponse)
	err := c.cc.Invoke(ctx, "/cockroach.kv.kvpb.Batch/Batch", drpcEncoding_File_api_proto{}, in, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *drpcBatchClient) BatchStream(ctx context.Context) (DRPCBatch_BatchStreamClient, error) {
	stream, err := c.cc.NewStream(ctx, "/cockroach.kv.kvpb.Batch/BatchStream", drpcEncoding_File_api_proto{})
	if err != nil {
		return nil, err
	}
	x := &drpcBatch_BatchStreamClient{stream}
	return x, nil
}

type DRPCBatch_BatchStreamClient interface {
	drpc.Stream
	Send(*BatchRequest) error
	Recv() (*BatchResponse, error)
}

type drpcBatch_BatchStreamClient struct {
	drpc.Stream
}

func (x *drpcBatch_BatchStreamClient) GetStream() drpc.Stream {
	return x.Stream
}

func (x *drpcBatch_BatchStreamClient) Send(m *BatchRequest) error {
	return x.MsgSend(m, drpcEncoding_File_api_proto{})
}

func (x *drpcBatch_BatchStreamClient) Recv() (*BatchResponse, error) {
	m := new(BatchResponse)
	if err := x.MsgRecv(m, drpcEncoding_File_api_proto{}); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *drpcBatch_BatchStreamClient) RecvMsg(m *BatchResponse) error {
	return x.MsgRecv(m, drpcEncoding_File_api_proto{})
}

type DRPCBatchServer interface {
	Batch(context.Context, *BatchRequest) (*BatchResponse, error)
	BatchStream(DRPCBatch_BatchStreamStream) error
}

type DRPCBatchUnimplementedServer struct{}

func (s *DRPCBatchUnimplementedServer) Batch(
	context.Context, *BatchRequest,
) (*BatchResponse, error) {
	return nil, drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

func (s *DRPCBatchUnimplementedServer) BatchStream(DRPCBatch_BatchStreamStream) error {
	return drpcerr.WithCode(errors.New("Unimplemented"), drpcerr.Unimplemented)
}

type DRPCBatchDescription struct{}

func (DRPCBatchDescription) NumMethods() int { return 2 }

func (DRPCBatchDescription) Method(
	n int,
) (string, drpc.Encoding, drpc.Receiver, interface{}, bool) {
	switch n {
	case 0:
		return "/cockroach.kv.kvpb.Batch/Batch", drpcEncoding_File_api_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return srv.(DRPCBatchServer).
					Batch(
						ctx,
						in1.(*BatchRequest),
					)
			}, DRPCBatchServer.Batch, true
	case 1:
		return "/cockroach.kv.kvpb.Batch/BatchStream", drpcEncoding_File_api_proto{},
			func(srv interface{}, ctx context.Context, in1, in2 interface{}) (drpc.Message, error) {
				return nil, srv.(DRPCBatchServer).
					BatchStream(
						&drpcBatch_BatchStreamStream{in1.(drpc.Stream)},
					)
			}, DRPCBatchServer.BatchStream, true
	default:
		return "", nil, nil, nil, false
	}
}

func DRPCRegisterBatch(mux drpc.Mux, impl DRPCBatchServer) error {
	return mux.Register(impl, DRPCBatchDescription{})
}

type DRPCBatch_BatchStream interface {
	drpc.Stream
	SendAndClose(*BatchResponse) error
}

type DRPCBatch_BatchStreamStream interface {
	drpc.Stream
	Send(*BatchResponse) error
	Recv() (*BatchRequest, error)
}

type drpcBatch_BatchStreamStream struct {
	drpc.Stream
}

func (x *drpcBatch_BatchStreamStream) Send(m *BatchResponse) error {
	return x.MsgSend(m, drpcEncoding_File_api_proto{})
}

func (x *drpcBatch_BatchStreamStream) Recv() (*BatchRequest, error) {
	m := new(BatchRequest)
	if err := x.MsgRecv(m, drpcEncoding_File_api_proto{}); err != nil {
		return nil, err
	}
	return m, nil
}

func (x *drpcBatch_BatchStreamStream) RecvMsg(m *BatchRequest) error {
	return x.MsgRecv(m, drpcEncoding_File_api_proto{})
}
