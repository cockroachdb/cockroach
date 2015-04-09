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

	// temporary work space
	reqBuf     bytes.Buffer
	reqHeader  wire.RequestHeader
	respHeader wire.ResponseHeader
}

// NewClientCodec returns a new rpc.ClientCodec using Protobuf-RPC on conn.
func NewClientCodec(conn io.ReadWriteCloser) rpc.ClientCodec {
	return &clientCodec{
		baseConn: baseConn{
			r: bufio.NewReader(conn),
			w: bufio.NewWriter(conn),
			c: conn,
		},
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

	if err := c.writeRequest(r.Seq, r.ServiceMethod, request); err != nil {
		return err
	}
	return c.w.Flush()
}

func (c *clientCodec) ReadResponseHeader(r *rpc.Response) error {
	if err := c.readResponseHeader(&c.respHeader); err != nil {
		return err
	}

	r.Seq = c.respHeader.GetId()
	r.ServiceMethod = c.respHeader.GetMethod()
	r.Error = c.respHeader.GetError()
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
		return nil
	}

	c.respHeader.Reset()
	return nil
}

func (c *clientCodec) writeRequest(id uint64, method string, request proto.Message) error {
	// generate header
	header := &c.reqHeader
	*header = wire.RequestHeader{
		Id:          id,
		Method:      method,
		Compression: wire.CompressionType_NONE,
	}
	if enableSnappy {
		header.Compression = wire.CompressionType_SNAPPY
	}

	// marshal header
	pbHeader, err := marshal(&c.reqBuf, header)
	if err != nil {
		return err
	}

	// send header (more)
	if err := c.sendFrame(pbHeader); err != nil {
		return err
	}

	// marshal request
	pbRequest, err := marshal(&c.reqBuf, request)
	if err != nil {
		return err
	}

	// send body (end)
	if enableSnappy {
		return snappyEncode(pbRequest, c.sendFrame)
	}
	return c.sendFrame(pbRequest)
}

func (c *clientCodec) readResponseHeader(header *wire.ResponseHeader) error {
	return c.recvProto(header, proto.Unmarshal)
}

func (c *clientCodec) readResponseBody(header *wire.ResponseHeader,
	response proto.Message) error {
	return c.recvProto(response, decompressors[header.Compression])
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
