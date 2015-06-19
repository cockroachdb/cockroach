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
// Author: Peter Mattis (peter@cockroachlabs.com)

// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package codec

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net/rpc"

	"github.com/cockroachdb/cockroach/rpc/codec/wire"
	"github.com/gogo/protobuf/proto"
)

type serverCodec struct {
	baseConn

	methods []string

	// Post body-decoding hook. May be nil in tests.
	requestBodyHook func(proto.Message) error

	// temporary work space
	respBodyBuf   bytes.Buffer
	respHeaderBuf bytes.Buffer
	respHeader    wire.ResponseHeader
	reqHeader     wire.RequestHeader
}

// NewServerCodec returns a serverCodec that communicates with the ClientCodec
// on the other end of the given conn.
func NewServerCodec(conn io.ReadWriteCloser, requestBodyHook func(proto.Message) error) rpc.ServerCodec {
	return &serverCodec{
		baseConn: baseConn{
			r: bufio.NewReader(conn),
			w: bufio.NewWriter(conn),
			c: conn,
		},
		requestBodyHook: requestBodyHook,
	}
}

func (c *serverCodec) ReadRequestHeader(r *rpc.Request) error {
	err := c.readRequestHeader(c.r, &c.reqHeader)
	if err != nil {
		return err
	}

	r.Seq = c.reqHeader.Id
	if c.reqHeader.Method == nil {
		if int(c.reqHeader.MethodId) >= len(c.methods) {
			return fmt.Errorf("unexpected method-id: %d >= %d",
				c.reqHeader.MethodId, len(c.methods))
		}
		r.ServiceMethod = c.methods[c.reqHeader.MethodId]
	} else if int(c.reqHeader.MethodId) > len(c.methods) {
		return fmt.Errorf("unexpected method-id: %d > %d",
			c.reqHeader.MethodId, len(c.methods))
	} else if int(c.reqHeader.MethodId) == len(c.methods) {
		c.methods = append(c.methods, *c.reqHeader.Method)
		r.ServiceMethod = *c.reqHeader.Method
	}
	return nil
}

// UserRequest is an interface for RPC requests that have a "requested user".
type UserRequest interface {
	// GetUser returns the user from the request.
	GetUser() string
}

func (c *serverCodec) ReadRequestBody(x interface{}) error {
	if x == nil {
		return nil
	}
	request, ok := x.(proto.Message)
	if !ok {
		return fmt.Errorf(
			"protorpc.ServerCodec.ReadRequestBody: %T does not implement proto.Message",
			x,
		)
	}

	err := c.readRequestBody(c.r, &c.reqHeader, request)
	if err != nil {
		return err
	}
	c.reqHeader.Reset()

	if c.requestBodyHook == nil {
		return nil
	}
	return c.requestBodyHook(request)
}

func (c *serverCodec) WriteResponse(r *rpc.Response, x interface{}) error {
	var response proto.Message
	if x != nil {
		var ok bool
		if response, ok = x.(proto.Message); !ok {
			if _, ok = x.(struct{}); !ok {
				return fmt.Errorf(
					"protorpc.ServerCodec.WriteResponse: %T does not implement proto.Message",
					x,
				)
			}
		}
	}

	if err := c.writeResponse(r, response); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *serverCodec) writeResponse(r *rpc.Response, response proto.Message) error {
	// clear response if error
	if r.Error != "" {
		response = nil
	}

	// marshal response
	var pbResponse []byte
	if response != nil {
		var err error
		pbResponse, err = marshal(&c.respBodyBuf, response)
		if err != nil {
			return err
		}
	}

	// generate header
	header := &c.respHeader
	*header = wire.ResponseHeader{
		Id: r.Seq,
		// The net/rpc interface asks for the Response.ServiceMethod to be
		// returned from the server, but it is never used.
		//
		// Method: r.ServiceMethod,
		Error:            r.Error,
		Compression:      compressionType,
		UncompressedSize: uint32(len(pbResponse)),
	}

	// marshal header
	pbHeader, err := marshal(&c.respHeaderBuf, header)
	if err != nil {
		return err
	}

	// send header (more)
	if err := c.sendFrame(pbHeader); err != nil {
		return err
	}

	// send body (end)
	if compressionType == wire.CompressionType_SNAPPY {
		return snappyEncode(pbResponse, c.sendFrame)
	} else if compressionType == wire.CompressionType_LZ4 {
		return lz4Encode(pbResponse, c.sendFrame)
	}
	return c.sendFrame(pbResponse)
}

func (c *serverCodec) readRequestHeader(r *bufio.Reader, header *wire.RequestHeader) error {
	return c.recvProto(header, 0, protoUnmarshal)
}

func (c *serverCodec) readRequestBody(r *bufio.Reader, header *wire.RequestHeader,
	request proto.Message) error {
	return c.recvProto(request, header.UncompressedSize, decompressors[header.Compression])
}

type marshalTo interface {
	Size() int
	MarshalTo([]byte) (int, error)
}

func marshal(buf *bytes.Buffer, m proto.Message) ([]byte, error) {
	if m == nil {
		return nil, nil
	}
	if mt, ok := m.(marshalTo); ok {
		buf.Reset()
		size := mt.Size()
		buf.Grow(size)
		b := buf.Bytes()[:size]
		n, err := mt.MarshalTo(b)
		return b[:n], err
	}
	return proto.Marshal(m)
}
