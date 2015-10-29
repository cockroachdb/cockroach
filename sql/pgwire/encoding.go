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
// Author: Ben Darnell

package pgwire

import (
	"encoding/binary"
	"io"

	"github.com/cockroachdb/cockroach/util"
)

const maxMessageSize = 1 << 24

// bufferedReader is implemented by bufio.Reader and bytes.Buffer.
type bufferedReader interface {
	io.Reader
	ReadString(delim byte) (string, error)
	ReadByte() (byte, error)
}

// readMsg reads a length-prefixed message. It is only used directly
// during the authentication phase of the protocol; readTypedMsg is
// used at all other times.
func readUntypedMsg(rd io.Reader) ([]byte, error) {
	var size int32
	if err := binary.Read(rd, binary.BigEndian, &size); err != nil {
		return nil, err
	}
	// size includes itself.
	size -= 4
	if size > maxMessageSize || size < 0 {
		return nil, util.Errorf("message size %d out of bounds (0..%d)",
			size, maxMessageSize)
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(rd, data); err != nil {
		return nil, err
	}
	return data, nil
}

// readTypedMsg reads a message, returning its type code and body.
func readTypedMsg(rd bufferedReader) (messageType, []byte, error) {
	typ, err := rd.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	msg, err := readUntypedMsg(rd)
	return messageType(typ), msg, err
}

// writeTypedMsg writes a message with the given type.
func writeTypedMsg(w io.Writer, typ messageType, msg []byte) error {
	if _, err := w.Write([]byte{byte(typ)}); err != nil {
		return err
	}
	if err := binary.Write(w, binary.BigEndian, int32(len(msg)+4)); err != nil {
		return nil
	}
	_, err := w.Write(msg)
	return err
}

// readString reads a null-terminated string.
func readString(rd bufferedReader) (string, error) {
	s, err := rd.ReadString(0)
	if err != nil {
		return "", err
	}
	// Chop off the trailing delimiter.
	return s[:len(s)-1], nil
}

// writeString writes a null-terminated string.
func writeString(w io.Writer, s string) error {
	if _, err := io.WriteString(w, s); err != nil {
		return err
	}
	if _, err := w.Write([]byte{0}); err != nil {
		return err
	}
	return nil
}
