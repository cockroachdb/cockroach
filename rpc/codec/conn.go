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
	"encoding/binary"
	"io"
	"net"

	"github.com/cockroachdb/cockroach/rpc/codec/wire"
	"github.com/gogo/protobuf/proto"
)

// LZ4 benchmarks slightly faster than Snappy for pure-RPC benchmarks,
// but slightly slower than Snappy on higher level benchmarks like the
// ones for the Cockroach client.
const compressionType = wire.CompressionType_SNAPPY

type decompressFunc func(src []byte, uncompressedSize uint32, m proto.Message) error

var decompressors = [...]decompressFunc{
	wire.CompressionType_NONE:   protoUnmarshal,
	wire.CompressionType_SNAPPY: snappyDecode,
	wire.CompressionType_LZ4:    lz4Decode,
}

type baseConn struct {
	w        *bufio.Writer
	r        *bufio.Reader
	c        io.Closer
	frameBuf [binary.MaxVarintLen64]byte
}

// Close closes the underlying connection.
func (c *baseConn) Close() error {
	return c.c.Close()
}

func (c *baseConn) sendFrame(data []byte) error {
	// Allocate enough space for the biggest uvarint
	size := c.frameBuf[:]

	if data == nil || len(data) == 0 {
		n := binary.PutUvarint(size, uint64(0))
		return c.write(c.w, size[:n])
	}

	// Write the size and data
	n := binary.PutUvarint(size, uint64(len(data)))
	if err := c.write(c.w, size[:n]); err != nil {
		return err
	}
	return c.write(c.w, data)
}

func (c *baseConn) write(w io.Writer, data []byte) error {
	for index := 0; index < len(data); {
		n, err := w.Write(data[index:])
		if err != nil {
			if nerr, ok := err.(net.Error); !ok || !nerr.Temporary() {
				return err
			}
		}
		index += n
	}
	return nil
}

func (c *baseConn) recvProto(m proto.Message,
	uncompressedSize uint32, decompressor decompressFunc) error {
	size, err := binary.ReadUvarint(c.r)
	if err != nil {
		return err
	}
	if size == 0 {
		return nil
	}
	if c.r.Buffered() >= int(size) {
		// Parse proto directly from the buffered data.
		data, err := c.r.Peek(int(size))
		if err != nil {
			return err
		}
		if err := decompressor(data, uncompressedSize, m); err != nil {
			return err
		}
		// TODO(pmattis): This is a hack to advance the bufio pointer by
		// reading into the same slice that bufio.Reader.Peek
		// returned. In Go 1.5 we'll be able to use
		// bufio.Reader.Discard.
		_, err = io.ReadFull(c.r, data)
		return err
	}

	data := make([]byte, size)
	if _, err := io.ReadFull(c.r, data); err != nil {
		return err
	}
	return decompressor(data, uncompressedSize, m)
}

func protoUnmarshal(src []byte, uncompressedSize uint32, msg proto.Message) error {
	return proto.Unmarshal(src, msg)
}
