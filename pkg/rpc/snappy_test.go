// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

func TestSnappyCompressorCompressDecompress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var c snappyCompressor
	for _, tt := range []struct {
		lenPrefix bool
		in        []byte // uncompressed
		expOut    []byte // compressed
	}{
		// Without length prefixing enabled.
		{false, []byte{}, nil},
		{false, []byte("A"), []byte{0xff, 0x6, 0x0, 0x0, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59, 0x1, 0x5, 0x0, 0x0, 0xb3, 0xad, 0x60, 0x3e, 0x41}},
		{false, []byte("ABC"), []byte{0xff, 0x6, 0x0, 0x0, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59, 0x1, 0x7, 0x0, 0x0, 0x4b, 0xfb, 0x81, 0xf5, 0x41, 0x42, 0x43}},
		// With length prefixing enabled.
		{true, []byte{}, []byte{0xfd, 0x0}},
		{true, []byte("A"), []byte{0xfd, 0x1, 0xff, 0x6, 0x0, 0x0, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59, 0x1, 0x5, 0x0, 0x0, 0xb3, 0xad, 0x60, 0x3e, 0x41}},
		{true, []byte("ABC"), []byte{0xfd, 0x3, 0xff, 0x6, 0x0, 0x0, 0x73, 0x4e, 0x61, 0x50, 0x70, 0x59, 0x1, 0x7, 0x0, 0x0, 0x4b, 0xfb, 0x81, 0xf5, 0x41, 0x42, 0x43}},
	} {
		name := fmt.Sprintf("lenPrefix=%t/input=%v", tt.lenPrefix, tt.in)
		t.Run(name, func(t *testing.T) {
			c.setLengthPrefixingEnabled(tt.lenPrefix)

			// Compress.
			buf := &bytes.Buffer{}
			wc, err := c.Compress(buf)
			require.NoError(t, err)

			_, err = wc.Write(tt.in)
			require.NoError(t, err)
			err = wc.Close()
			require.NoError(t, err)
			out := buf.Bytes()
			require.Equal(t, tt.expOut, out)

			// Decompress.
			n := c.DecompressedSize(out)
			if tt.lenPrefix {
				require.Equal(t, len(tt.in), n)
			} else {
				require.Equal(t, -1, n)
			}

			r, err := c.Decompress(bytes.NewReader(out))
			require.NoError(t, err)
			in, err := io.ReadAll(r)
			require.NoError(t, err)
			require.EqualValues(t, tt.in, in)
		})
	}
}

func TestSnappyCompressorDecompressError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var c snappyCompressor
	for _, tt := range []struct {
		in     []byte // compressed
		expErr error
	}{
		{[]byte{}, nil},
		{[]byte{0x00}, snappy.ErrCorrupt},
		{[]byte{0xfd}, errCorruptLenPrefix},
		{[]byte{0xfd, 0xff}, errCorruptLenPrefix},
		{[]byte{0xfe}, snappy.ErrCorrupt},
		{[]byte{0xff}, snappy.ErrCorrupt},
	} {
		name := fmt.Sprintf("input=%v", tt.in)
		t.Run(name, func(t *testing.T) {
			r, err := c.Decompress(bytes.NewReader(tt.in))
			require.NoError(t, err)
			in, err := io.ReadAll(r)
			require.ErrorIs(t, tt.expErr, err)
			require.Equal(t, []byte{}, in)
		})
	}
}
