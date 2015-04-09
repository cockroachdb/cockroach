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

	wire "github.com/cockroachdb/cockroach/rpc/codec/wire.pb"
	"github.com/gogo/protobuf/proto"
)

type serverCodec struct {
	baseConn

	// temporary work space
	respBuf    bytes.Buffer
	respHeader wire.ResponseHeader
	reqHeader  wire.RequestHeader
}

// NewServerCodec returns a serverCodec that communicates with the ClientCodec
// on the other end of the given conn.
func NewServerCodec(conn io.ReadWriteCloser) rpc.ServerCodec {
	return &serverCodec{
		baseConn: baseConn{
			r: bufio.NewReader(conn),
			w: bufio.NewWriter(conn),
			c: conn,
		},
	}
}

func (c *serverCodec) ReadRequestHeader(r *rpc.Request) error {
	err := c.readRequestHeader(c.r, &c.reqHeader)
	if err != nil {
		return err
	}

	r.Seq = c.reqHeader.GetId()
	r.ServiceMethod = c.reqHeader.GetMethod()
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
		return nil
	}

	c.reqHeader.Reset()
	return nil
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

	// generate header
	header := &c.respHeader
	*header = wire.ResponseHeader{
		Id: r.Seq,
		// The net/rpc interface asks for the Response.ServiceMethod to be
		// returned from the server, but it is never used.
		//
		// Method: r.ServiceMethod,
		Error:       r.Error,
		Compression: wire.CompressionType_NONE,
	}
	if enableSnappy {
		header.Compression = wire.CompressionType_SNAPPY
	}

	// marshal header
	pbHeader, err := marshal(&c.respBuf, header)
	if err != nil {
		return err
	}

	// send header (more)
	if err := c.sendFrame(pbHeader); err != nil {
		return err
	}

	// marshal response
	pbResponse, err := marshal(&c.respBuf, response)
	if err != nil {
		return err
	}

	// send body (end)
	if enableSnappy {
		return snappyEncode(pbResponse, c.sendFrame)
	}
	return c.sendFrame(pbResponse)
}

func (c *serverCodec) readRequestHeader(r *bufio.Reader, header *wire.RequestHeader) error {
	return c.recvProto(header, proto.Unmarshal)
}

func (c *serverCodec) readRequestBody(r *bufio.Reader, header *wire.RequestHeader,
	request proto.Message) error {
	return c.recvProto(request, decompressors[header.Compression])
}

// ServeConn runs the Protobuf-RPC server on a single connection.
// ServeConn blocks, serving the connection until the client hangs up.
// The caller typically invokes ServeConn in a go statement.
func ServeConn(conn io.ReadWriteCloser) {
	rpc.ServeCodec(NewServerCodec(conn))
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
