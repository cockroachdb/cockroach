// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License.

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
		{[]byte{0x00}, []byte{0x00}},
		{[]byte{0x00, 0x00}, []byte{0x00, 0x00}},
		{[]byte{0x00, 0x01}, []byte{0x00, 0x00}},
		{[]byte{0x01, 0x00, 0x00, 0x00}, []byte{0x00, 0xff, 0xff, 0xff}},
		{[]byte{0xff, 0xff}, []byte{0xff, 0xfe}},
	} {
		t.Run(fmt.Sprintf("undo-prefix/key=%q", tc.in), func(t *testing.T) {
			if e, a := tc.out, encoding.UndoPrefixEnd(tc.in); !bytes.Equal(e, a) {
				t.Errorf("expected %q but got %q", e, a)
			}
		})
	}

	for _, k := range [][]byte{
		{0x00},
		{0x00, 0x00},
		{0x00, 0x01},
		{0x00, 0xff, 0xff, 0xff},
		{0x01, 0x00, 0x00, 0x00},
	} {
		t.Run(fmt.Sprintf("roundtrip/key=%q", k), func(t *testing.T) {
			if r := encoding.UndoPrefixEnd(roachpb.Key(k).PrefixEnd()); !bytes.Equal(k, r) {
				t.Errorf("roundtripping resulted in %q", r)
			}
		})
	}

}
