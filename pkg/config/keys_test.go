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
// permissions and limitations under the License.

package config_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestDecodeObjectID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		key       roachpb.RKey
		keySuffix []byte
		success   bool
		id        uint32
	}{
		// Before the structured span.
		{roachpb.RKeyMin, nil, false, 0},

		// Boundaries of structured span.
		{roachpb.RKeyMax, nil, false, 0},

		// Valid, even if there are things after the ID.
		{testutils.MakeKey(keys.MakeTablePrefix(42), roachpb.RKey("\xff")), []byte{'\xff'}, true, 42},
		{keys.MakeTablePrefix(0), []byte{}, true, 0},
		{keys.MakeTablePrefix(999), []byte{}, true, 999},
	}

	for tcNum, tc := range testCases {
		id, keySuffix, success := config.DecodeObjectID(tc.key)
		if success != tc.success {
			t.Errorf("#%d: expected success=%t", tcNum, tc.success)
			continue
		}
		if id != tc.id {
			t.Errorf("#%d: expected id=%d, got %d", tcNum, tc.id, id)
		}
		if !bytes.Equal(keySuffix, tc.keySuffix) {
			t.Errorf("#%d: expected suffix=%q, got %q", tcNum, tc.keySuffix, keySuffix)
		}
	}
}
