// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package grpcutil

import (
	"bytes"
	"encoding/binary"
	"math"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/gogo/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/encoding"
)

// PreparedMsg is responsible for creating a Marshalled and Compressed object.
//
// The type is a copy of the grpc.PreparedMsg struct, but with the the ability
// to recycle memory buffers across calls to Encode.
type PreparedMsg struct {
	// Fields that mirror the grpc.PreparedMsg struct.
	encodedData []byte
	hdr         []byte
	payload     []byte

	// Fields for memory reuse.
	encodeBuf   []byte
	compressBuf bytes.Buffer
	hdrBuf      [headerLen]byte
}

// AsGrpc returns the PreparedMsg as a *grpc.PreparedMsg.
//
// The returned value is only valid until the next call to Encode.
func (p *PreparedMsg) AsGrpc() *grpc.PreparedMsg {
	return (*grpc.PreparedMsg)(unsafe.Pointer(p))
}

// Encode marshals and compresses the message using the codec and compressor for
// the stream.
//
// Mirrors the logic in grpc.PreparedMsg.Encode, but with the ability to recycle
// memory buffers across calls.
func (p *PreparedMsg) Encode(s grpc.Stream, msg interface{}) error {
	defer p.discardLargeBuffers()

	ctx := s.Context()
	rpcInfo, ok := RPCInfoFromContext(ctx)
	if !ok {
		return status.Errorf(codes.Internal, "grpc: unable to get rpcInfo")
	}

	// Check if the context has the relevant information to prepareMsg.
	if rpcInfo.PreloaderInfo == nil {
		return status.Errorf(codes.Internal, "grpc: rpcInfo.preloaderInfo is nil")
	}
	if rpcInfo.PreloaderInfo.Codec == nil {
		return status.Errorf(codes.Internal, "grpc: rpcInfo.preloaderInfo.codec is nil")
	}

	// Prepare the msg.
	data, err := p.encode(rpcInfo.PreloaderInfo.Codec, msg)
	if err != nil {
		return err
	}
	p.encodedData = data
	compData, err := p.compress(data, rpcInfo.PreloaderInfo.Cp, rpcInfo.PreloaderInfo.Comp)
	if err != nil {
		return err
	}
	p.hdr, p.payload = p.msgHeader(data, compData)
	return nil
}

// encode serializes msg and returns a buffer containing the message, or an
// error if it is too large to be transmitted by grpc. If msg is nil, it
// generates an empty message.
func (p *PreparedMsg) encode(c encoding.Codec, msg interface{}) ([]byte, error) {
	if msg == nil {
		return nil, nil
	}
	// WIP: assume that the codec wants to use the protobuf encoding.
	_ = c
	pb, ok := msg.(protoutil.Message)
	if !ok {
		return nil, status.Errorf(codes.Internal, "expected a protoutil.Message, got %T", msg)
	}
	size := pb.Size()
	if uint(size) > math.MaxUint32 {
		return nil, status.Errorf(codes.ResourceExhausted, "grpc: message too large (%d bytes)", size)
	}
	if cap(p.encodeBuf) < size {
		p.encodeBuf = make([]byte, size)
	} else {
		p.encodeBuf = p.encodeBuf[:size]
	}
	_, err := protoutil.MarshalToSizedBuffer(pb, p.encodeBuf)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "grpc: error while marshaling: %v", err.Error())
	}
	return p.encodeBuf, nil
}

// compress returns the input bytes compressed by compressor or cp. If both
// compressors are nil, returns nil.
func (p *PreparedMsg) compress(
	in []byte, cp grpc.Compressor, compressor encoding.Compressor,
) ([]byte, error) {
	if compressor == nil && cp == nil {
		return nil, nil
	}
	if cp != nil {
		return nil, status.Errorf(codes.Internal, "expected a encoding.Compressor, got %T", cp)
	}
	wrapErr := func(err error) error {
		return status.Errorf(codes.Internal, "grpc: error while compressing: %v", err.Error())
	}
	buf := &p.compressBuf
	z, err := compressor.Compress(buf)
	if err != nil {
		return nil, wrapErr(err)
	}
	if _, err := z.Write(in); err != nil {
		return nil, wrapErr(err)
	}
	if err := z.Close(); err != nil {
		return nil, wrapErr(err)
	}
	return buf.Bytes(), nil
}

// The format of the payload: compressed or not?
type payloadFormat uint8

const (
	compressionNone payloadFormat = 0 // no compression
	compressionMade payloadFormat = 1 // compressed
)

const (
	payloadLen = 1
	sizeLen    = 4
	headerLen  = payloadLen + sizeLen
)

// msgHeader returns a 5-byte header for the message being transmitted and the
// payload, which is compData if non-nil or data otherwise.
func (p *PreparedMsg) msgHeader(data, compData []byte) (hdr []byte, payload []byte) {
	hdr = p.hdrBuf[:]
	if compData != nil {
		hdr[0] = byte(compressionMade)
		data = compData
	} else {
		hdr[0] = byte(compressionNone)
	}

	// Write length of payload into buf.
	binary.BigEndian.PutUint32(hdr[payloadLen:], uint32(len(data)))
	return hdr, data
}

// discardLargeBuffers resets the PreparedMsg for reuse. This prevents the
// PreparedMsg from holding onto excessively large buffers across calls to
// Encode.
func (p *PreparedMsg) discardLargeBuffers() {
	const maxRecycleSize = 1 << 16 /* 64KB */
	if cap(p.encodeBuf) > maxRecycleSize {
		p.encodeBuf = nil
	} else {
		p.encodeBuf = p.encodeBuf[:0]
	}
	if p.compressBuf.Cap() > maxRecycleSize {
		p.compressBuf = bytes.Buffer{}
	} else {
		p.compressBuf.Reset()
	}
}
