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
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util"
	"github.com/gogo/protobuf/proto"
)

type serverCodec struct {
	baseConn

	methods []string

	// Server/Connection settings determined at connection time.
	insecureMode    bool
	certificateUser string

	// temporary work space
	respBodyBuf   bytes.Buffer
	respHeaderBuf bytes.Buffer
	respHeader    wire.ResponseHeader
	reqHeader     wire.RequestHeader
}

// NewServerCodec returns a serverCodec that communicates with the ClientCodec
// on the other end of the given conn.
func NewServerCodec(conn io.ReadWriteCloser, insecureMode bool, certificateUser string) rpc.ServerCodec {
	return &serverCodec{
		baseConn: baseConn{
			r: bufio.NewReader(conn),
			w: bufio.NewWriter(conn),
			c: conn,
		},
		insecureMode:    insecureMode,
		certificateUser: certificateUser,
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

// authenticateRequest takes a request proto and attempts to authenticate it.
// Requests need to implement UserRequest.
// We compare the header.User against the client certificate Subject.CommonName.
func (c *serverCodec) authenticateRequest(request proto.Message) error {
	// UserRequest must be implemented.
	requestWithUser, ok := request.(UserRequest)
	if !ok {
		return util.Errorf("unknown request type: %T", request)
	}

	// Extract user and verify.
	// TODO(marc): we may eventually need stricter user syntax rules.
	requestedUser := requestWithUser.GetUser()
	if len(requestedUser) == 0 {
		return util.Errorf("missing User in request: %+v", request)
	}

	if c.insecureMode {
		// Insecure mode: trust the user in the header.
		return nil
	}

	// The node user can do anything.
	// TODO(marc): it would be nice to pass around the fact that we came in as "node".
	if c.certificateUser == security.NodeUser {
		return nil
	}

	// Check that users match.
	if c.certificateUser != requestedUser {
		return util.Errorf("requested user is %s, but certificate is for %s",
			requestedUser, c.certificateUser)
	}

	return nil
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

	return c.authenticateRequest(request)
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
