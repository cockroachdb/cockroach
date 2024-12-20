// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcutil

import (
	"context"
	"unsafe"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

// RPCInfo exports grpc.rpcInfo.
type RPCInfo struct {
	FailFast      bool
	PreloaderInfo *CompressorInfo
}

// CompressorInfo exports grpc.compressorInfo.
type CompressorInfo struct {
	Codec encoding.Codec
	Cp    grpc.Compressor
	Comp  encoding.Compressor
}

// From runtime/runtime2.go:eface
type eface struct {
	typ, data unsafe.Pointer
}

// RPCInfoFromContext extracts the RPCInfo from the context.
func RPCInfoFromContext(ctx context.Context) (*RPCInfo, bool) {
	v := ctx.Value(grpcInfoContextKeyObj)
	if v == nil {
		return nil, false
	}
	return (*RPCInfo)((*eface)(unsafe.Pointer(&v)).data), true
}

// grpcInfoContextKeyObj is a copy of a value with the Go type
// `grpc.rpcInfoContextKey{}`. We cannot construct an object of that type
// directly, but we can "steal" it by forcing the grpc to give it to us:
// `grpc.PreparedMsg.Encode` gives an instance of this object as parameter to
// the `Value` method of the context you give it as argument. We use a custom
// implementation of that to "steal" the argument of type `rpcInfoContextKey{}`
// given to us that way.
//
// This is the same trick that we pull with grpcIncomingKeyObj.
var grpcInfoContextKeyObj = func() interface{} {
	var s fakeStream
	_ = (*grpc.PreparedMsg)(nil).Encode(&s, nil)
	if s.recordedKey == nil {
		panic("PreparedMsg.Encode did not request a key")
	}
	return s.recordedKey
}()

type fakeStream struct {
	fakeContext
}

var _ grpc.Stream = (*fakeStream)(nil)

func (s *fakeStream) Context() context.Context { return &s.fakeContext }

func (*fakeStream) SendMsg(interface{}) error { panic("unused") }
func (*fakeStream) RecvMsg(interface{}) error { panic("unused") }
