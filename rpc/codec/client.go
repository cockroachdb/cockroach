// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter.mattis@gmail.com)

// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package codec

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"net/rpc"
	"time"

	wire "github.com/cockroachdb/cockroach/rpc/codec/wire.pb"
	"github.com/gogo/protobuf/proto"
)

type clientCodec struct {
	baseConn

	methods map[string]int32

	// temporary work space
	reqBodyBuf   bytes.Buffer
	reqHeaderBuf bytes.Buffer
	reqHeader    wire.RequestHeader
	respHeader   wire.ResponseHeader
}

// NewClientCodec returns a new rpc.ClientCodec using Protobuf-RPC on conn.
func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return &clientCodec{
		baseConn: baseConn{
			r: bufio.NewReader(conn),
			w: bufio.NewWriter(conn),
			c: conn,
		},
		methods: make(map[string]int32),
	}
}

func (c *clientCodec) WriteRequest(r *rpc.Request, param interface{}) error {
	var request proto.Message
	if param != nil {
		var ok bool
		if request, ok = param.(proto.Message); !ok {
			return fmt.Errorf(
				"protorpc.ClientCodec.WriteRequest: %T does not implement proto.Message",
				param,
			)
		}
	}

	if err := c.writeRequest(r, request); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *clientCodec) ReadResponseHeader(r *rpc.Response) error {
	if err := c.readResponseHeader(&c.respHeader); err != nil {
		return err
	}

	r.Seq = c.respHeader.Id
	r.ServiceMethod = c.respHeader.GetMethod()
	r.Error = c.respHeader.Error
	return nil
}

func (c *clientCodec) ReadResponseBody(x interface{}) error {
	var response proto.Message
	if x != nil {
		var ok bool
		response, ok = x.(proto.Message)
		if !ok {
			return fmt.Errorf(
				"protorpc.ClientCodec.ReadResponseBody: %T does not implement proto.Message",
				x,
			)
		}
	}

	err := c.readResponseBody(&c.respHeader, response)
	if err != nil {
		return err
	}

	c.respHeader.Reset()
	return nil
}

func (c *clientCodec) writeRequest(r *rpc.Request, request proto.Message) error {
	// marshal request
	pbRequest, err := marshal(&c.reqBodyBuf, request)
	if err != nil {
		return err
	}

	// generate header
	header := &c.reqHeader
	*header = wire.RequestHeader{
		Id:               r.Seq,
		Compression:      compressionType,
		UncompressedSize: uint32(len(pbRequest)),
	}
	if mid, ok := c.methods[r.ServiceMethod]; ok {
		header.MethodId = mid
	} else {
		header.Method = &r.ServiceMethod
		header.MethodId = int32(len(c.methods))
		c.methods[r.ServiceMethod] = header.MethodId
	}

	// marshal header
	pbHeader, err := marshal(&c.reqHeaderBuf, header)
	if err != nil {
		return err
	}

	// send header (more)
	if err := c.sendFrame(pbHeader); err != nil {
		return err
	}

	// send body (end)
	if compressionType == wire.CompressionType_SNAPPY {
		return snappyEncode(pbRequest, c.sendFrame)
	} else if compressionType == wire.CompressionType_LZ4 {
		return lz4Encode(pbRequest, c.sendFrame)
	}
	return c.sendFrame(pbRequest)
}

func (c *clientCodec) readResponseHeader(header *wire.ResponseHeader) error {
	return c.recvProto(header, 0, protoUnmarshal)
}

func (c *clientCodec) readResponseBody(header *wire.ResponseHeader,
	response proto.Message) error {
	return c.recvProto(response, header.UncompressedSize, decompressors[header.Compression])
}

// NewClient returns a new rpc.Client to handle requests to the
// set of services at the other end of the connection.
func NewClient(conn io.ReadWriteCloser) *rpc.Client {
	return rpc.NewClientWithCodec(NewClientCodec(conn))
}

// Dial connects to a Protobuf-RPC server at the specified network address.
func Dial(network, address string) (*rpc.Client, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), err
}

// DialTimeout connects to a Protobuf-RPC server at the specified network address.
func DialTimeout(network, address string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}
	return NewClient(conn), err
}
