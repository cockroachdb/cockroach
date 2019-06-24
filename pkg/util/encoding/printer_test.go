// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package encoding_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

func TestUndoPrefixEnd(t *testing.T) {
	for _, tc := range []struct {
		in  []byte
		out []byte
	}{
		{[]byte{0x00, 0x01}, []byte{0x00, 0x00}},
		{[]byte{0x01, 0x02, 0x03, 0x05}, []byte{0x01, 0x02, 0x03, 0x04}},
		{[]byte{0xff, 0xff}, []byte{0xff, 0xfe}},
		{[]byte{0xff}, []byte{0xfe}},

		// Invalid keys
		{[]byte{0x00}, nil},
		{[]byte{0x01, 0x00}, nil},
	} {
		t.Run(fmt.Sprintf("undo-prefix/key=%q", tc.in), func(t *testing.T) {
			result, ok := encoding.UndoPrefixEnd(tc.in)
			if !ok {
				result = nil
			}
			if !bytes.Equal(tc.out, result) {
				t.Errorf("expected %q but got %q", tc.out, result)
			}
		})
	}

	for _, k := range [][]byte{
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x01, 0x00, 0xff, 0x00},
		{0x00, 0x00, 0x00, 0x00},
		{0x01, 0x02, 0x03, 0x04},
		// Keys that end in 0xff do not roundtrip.
	} {
		t.Run(fmt.Sprintf("roundtrip/key=%q", k), func(t *testing.T) {
			if r, ok := encoding.UndoPrefixEnd(roachpb.Key(k).PrefixEnd()); !ok || !bytes.Equal(k, r) {
				t.Errorf("roundtripping resulted in %q", r)
			}
		})
	}

}
