// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package codec

import (
	"bufio"
	"io"
	"sync"

	wire "github.com/cockroachdb/cockroach/rpc/codec/wire.pb"
	"github.com/gogo/protobuf/proto"
)

type reqHeaderBuf struct {
	header wire.RequestHeader
	data   [256]byte
}

type respHeaderBuf struct {
	header wire.ResponseHeader
	data   [256]byte
}

var (
	reqHeaderPool = sync.Pool{
		New: func() interface{} {
			return &reqHeaderBuf{}
		},
	}
	respHeaderPool = sync.Pool{
		New: func() interface{} {
			return &respHeaderBuf{}
		},
	}
)

func writeRequest(w io.Writer, id uint64, method string, request proto.Message) error {
	// marshal request
	pbRequest := []byte(nil)
	if request != nil {
		var err error
		pbRequest, err = proto.Marshal(request)
		if err != nil {
			return err
		}
	}

	// TODO(pmattis): Snappy compression using go-snappy benchmarks as a
	// loss. We've got the C++ snappy library linked in to the
	// library. Should benchmark whether it is significantly faster than
	// the go version.

	// generate header
	buf := reqHeaderPool.Get().(*reqHeaderBuf)
	header := &buf.header
	*header = wire.RequestHeader{
		Id:     id,
		Method: method,
	}

	size := header.Size()
	var pbHeader []byte
	if size <= len(buf.data) {
		pbHeader = buf.data[:size]
	} else {
		pbHeader = make([]byte, size)
	}

	// check header size
	_, err := header.MarshalTo(pbHeader)
	if err != nil {
		reqHeaderPool.Put(buf)
		return err
	}

	// send header (more)
	err = sendFrame(w, pbHeader)
	reqHeaderPool.Put(buf)
	if err != nil {
		return err
	}

	// send body (end)
	if err := sendFrame(w, pbRequest); err != nil {
		return err
	}

	return nil
}

func readRequestHeader(r *bufio.Reader, header *wire.RequestHeader) error {
	return recvProto(r, header)
}

func readRequestBody(r *bufio.Reader, request proto.Message) error {
	return recvProto(r, request)
}

func writeResponse(w io.Writer, id uint64, serr string, response proto.Message) error {
	// clean response if error
	if serr != "" {
		response = nil
	}

	// marshal response
	pbResponse := []byte(nil)
	if response != nil {
		var err error
		pbResponse, err = proto.Marshal(response)
		if err != nil {
			return err
		}
	}

	// generate header
	buf := respHeaderPool.Get().(*respHeaderBuf)
	header := &buf.header
	*header = wire.ResponseHeader{
		Id:    id,
		Error: serr,
	}

	size := header.Size()
	var pbHeader []byte
	if size <= len(buf.data) {
		pbHeader = buf.data[:size]
	} else {
		pbHeader = make([]byte, size)
	}

	// check header size
	_, err := header.MarshalTo(pbHeader)
	if err != nil {
		respHeaderPool.Put(buf)
		return err
	}

	// send header (more)
	err = sendFrame(w, pbHeader)
	respHeaderPool.Put(buf)
	if err != nil {
		return err
	}

	// send body (end)
	if err = sendFrame(w, pbResponse); err != nil {
		return err
	}

	return nil
}

func readResponseHeader(r *bufio.Reader, header *wire.ResponseHeader) error {
	return recvProto(r, header)
}

func readResponseBody(r *bufio.Reader, response proto.Message) error {
	return recvProto(r, response)
}
