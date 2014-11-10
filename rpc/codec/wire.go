// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package codec

import (
	"fmt"
	"hash/crc32"
	"io"

	"code.google.com/p/gogoprotobuf/proto"
	"code.google.com/p/snappy-go/snappy"
	wire "github.com/cockroachdb/cockroach/rpc/codec/wire.pb"
)

func writeRequest(w io.Writer, id uint64, method string, request proto.Message) error {
	// marshal request
	pbRequest := []byte{}
	if request != nil {
		var err error
		pbRequest, err = proto.Marshal(request)
		if err != nil {
			return err
		}
	}

	// compress serialized proto data
	compressedPbRequest, err := snappy.Encode(nil, pbRequest)
	if err != nil {
		return err
	}

	// generate header
	header := &wire.RequestHeader{
		Id:                         id,
		Method:                     method,
		RawRequestLen:              uint32(len(pbRequest)),
		SnappyCompressedRequestLen: uint32(len(compressedPbRequest)),
		Checksum:                   crc32.ChecksumIEEE(compressedPbRequest),
	}

	// check header size
	pbHeader, err := proto.Marshal(header)
	if err != err {
		return err
	}
	if uint32(len(pbHeader)) > wire.Default_Const_MaxHeaderLen {
		return fmt.Errorf("protorpc.writeRequest: header larger than max_header_len: %d.", len(pbHeader))
	}

	// send header (more)
	if err := sendFrame(w, pbHeader); err != nil {
		return err
	}

	// send body (end)
	if err := sendFrame(w, compressedPbRequest); err != nil {
		return err
	}

	return nil
}

func readRequestHeader(r io.Reader, header *wire.RequestHeader) (err error) {
	// recv header (more)
	pbHeader, err := recvFrame(r)
	if err != nil {
		return err
	}

	// Unmarshal Header
	err = proto.Unmarshal(pbHeader, header)
	if err != nil {
		return err
	}

	return nil
}

func readRequestBody(r io.Reader, header *wire.RequestHeader, request proto.Message) error {
	// recv body (end)
	compressedPbRequest, err := recvFrame(r)
	if err != nil {
		return err
	}

	// checksum
	if crc32.ChecksumIEEE(compressedPbRequest) != header.GetChecksum() {
		return fmt.Errorf("protorpc.readRequestBody: unexpected checksum.")
	}

	// decode the compressed data
	pbRequest, err := snappy.Decode(nil, compressedPbRequest)
	if err != nil {
		return err
	}
	// check wire header: rawMsgLen
	if uint32(len(pbRequest)) != header.GetRawRequestLen() {
		return fmt.Errorf("protorpc.readRequestBody: Unexcpeted header.RawRequestLen.")
	}

	// Unmarshal to proto message
	if request != nil {
		err = proto.Unmarshal(pbRequest, request)
		if err != nil {
			return err
		}
	}

	return nil
}

func writeResponse(w io.Writer, id uint64, serr string, response proto.Message) (err error) {
	// clean response if error
	if serr != "" {
		response = nil
	}

	// marshal response
	pbResponse := []byte{}
	if response != nil {
		pbResponse, err = proto.Marshal(response)
		if err != nil {
			return err
		}
	}

	// compress serialized proto data
	compressedPbResponse, err := snappy.Encode(nil, pbResponse)
	if err != nil {
		return err
	}

	// generate header
	header := &wire.ResponseHeader{
		Id:                          id,
		Error:                       serr,
		RawResponseLen:              uint32(len(pbResponse)),
		SnappyCompressedResponseLen: uint32(len(compressedPbResponse)),
		Checksum:                    crc32.ChecksumIEEE(compressedPbResponse),
	}

	// check header size
	pbHeader, err := proto.Marshal(header)
	if err != err {
		return
	}
	if uint32(len(pbHeader)) > wire.Default_Const_MaxHeaderLen {
		return fmt.Errorf("protorpc.writeResponse: header larger than max_header_len: %d.",
			len(pbHeader),
		)
	}

	// send header (more)
	if err = sendFrame(w, pbHeader); err != nil {
		return
	}

	// send body (end)
	if err = sendFrame(w, compressedPbResponse); err != nil {
		return
	}

	return nil
}

func readResponseHeader(r io.Reader, header *wire.ResponseHeader) error {
	// recv header (more)
	pbHeader, err := recvFrame(r)
	if err != nil {
		return err
	}

	// Marshal Header
	err = proto.Unmarshal(pbHeader, header)
	if err != nil {
		return err
	}

	return nil
}

func readResponseBody(r io.Reader, header *wire.ResponseHeader, response proto.Message) error {
	// recv body (end)
	compressedPbResponse, err := recvFrame(r)
	if err != nil {
		return err
	}

	// checksum
	if crc32.ChecksumIEEE(compressedPbResponse) != header.GetChecksum() {
		return fmt.Errorf("protorpc.readResponseBody: unexpected checksum.")
	}

	// decode the compressed data
	pbResponse, err := snappy.Decode(nil, compressedPbResponse)
	if err != nil {
		return err
	}
	// check wire header: rawMsgLen
	if uint32(len(pbResponse)) != header.GetRawResponseLen() {
		return fmt.Errorf("protorpc.readResponseBody: Unexcpeted header.RawResponseLen.")
	}

	// Unmarshal to proto message
	if response != nil {
		err = proto.Unmarshal(pbResponse, response)
		if err != nil {
			return err
		}
	}

	return nil
}
