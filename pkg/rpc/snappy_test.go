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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/golang/snappy"
	"github.com/stretchr/testify/require"
)

func TestSnappyCompressorCompressDecompress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var c snappyCompressor
	w := echotest.NewWalker(t, testutils.TestDataPath(t, t.Name()))
	for _, tt := range []struct {
		lenPrefix bool
		in        []byte // uncompressed
	}{
		// Without length prefixing enabled.
		{false, []byte{}},
		{false, []byte("A")},
		{false, []byte("ABC")},
		// With length prefixing enabled.
		{true, []byte{}},
		{true, []byte("A")},
		{true, []byte("ABC")},
	} {
		name := string(tt.in)
		if name == "" {
			name = "empty"
		}
		if tt.lenPrefix {
			name += "_lenprefix"
		} else {
			name += "_noprefix"
		}
		t.Run(name, w.Run(t, name, func(t *testing.T) string {
			var s strings.Builder

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
			fmt.Fprintf(&s, "input:\n%x (%s)\n", tt.in, tt.in)
			fmt.Fprintf(&s, "compressed:\n%x\n", out)

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
			require.Equal(t, tt.in, in)
			return s.String()
		}))
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
