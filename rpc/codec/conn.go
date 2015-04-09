// Copyright 2013 <chaishushan{AT}gmail.com>. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package codec

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"

	"code.google.com/p/snappy-go/snappy"

	"github.com/gogo/protobuf/proto"
)

type frameBuf struct {
	size [binary.MaxVarintLen64]byte
}

var framePool = sync.Pool{
	New: func() interface{} {
		return &frameBuf{}
	},
}

func sendFrame(w io.Writer, data []byte) (err error) {
	// Allocate enough space for the biggest uvarint
	buf := framePool.Get().(*frameBuf)
	size := buf.size[:]

	if data == nil || len(data) == 0 {
		n := binary.PutUvarint(size, uint64(0))
		err = write(w, size[:n])
		framePool.Put(buf)
		return
	}

	// Write the size and data
	n := binary.PutUvarint(size, uint64(len(data)))
	err = write(w, size[:n])
	framePool.Put(buf)
	if err != nil {
		return
	}
	err = write(w, data)
	return
}

func recvProto(r *bufio.Reader, m proto.Message, compressed bool) (err error) {
	size, err := binary.ReadUvarint(r)
	if err != nil {
		return err
	}
	if size != 0 {
		if r.Buffered() >= int(size) {
			// Parse proto directly from the buffered data.
			data, err := r.Peek(int(size))
			if err != nil {
				return err
			}
			uncompressedData := data
			if compressed {
				uncompressedData, err = snappy.Decode(nil, data)
				if err != nil {
					return err
				}
			}
			if err := proto.Unmarshal(uncompressedData, m); err != nil {
				return err
			}
			// TODO(pmattis): This is a hack to advance the bufio pointer by
			// reading into the same slice that bufio.Reader.Peek
			// returned. In Go 1.5 we'll be able to use
			// bufio.Reader.Discard.
			_, err = io.ReadFull(r, data)
			return err
		}

		data := make([]byte, size)
		if _, err := io.ReadFull(r, data); err != nil {
			return err
		}
		if compressed {
			data, err = snappy.Decode(nil, data)
			if err != nil {
				return err
			}
		}
		if err := proto.Unmarshal(data, m); err != nil {
			return err
		}
	}
	return nil
}

func write(w io.Writer, data []byte) error {
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
