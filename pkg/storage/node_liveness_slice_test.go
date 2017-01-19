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
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage

import (
	"bytes"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestLivenessSlice(t *testing.T) {
	defer leaktest.AfterTest(t)()

	l1 := Liveness{
		NodeID:     1,
		Epoch:      1,
		Expiration: makeTS(1, 1),
	}
	l2 := Liveness{
		NodeID:     2,
		Epoch:      2,
		Expiration: makeTS(2, 2),
	}
	l3 := Liveness{
		NodeID:     3,
		Epoch:      3,
		Expiration: makeTS(3, 3),
	}

	// Verify sorting.
	ls := livenessSlice{l2, l3, l1}
	sort.Sort(ls)
	if expLS := (livenessSlice{l1, l2, l3}); !reflect.DeepEqual(expLS, ls) {
		t.Errorf("expected order %+v; got %+v", expLS, ls)
	}

	// Verify Hash() is order independent.
	h1 := (livenessSlice{l1, l2, l3}).Hash()
	if h2 := (livenessSlice{l3, l1, l2}).Hash(); !bytes.Equal(h1, h2) {
		t.Error("expected order independence in Hash()")
	}

	// Modify a value and verify hash notices.
	l1.Epoch = 3
	if h2 := (livenessSlice{l1, l2, l3}).Hash(); bytes.Equal(h1, h2) {
		t.Error("expected hash to change with incremented epoch")
	}
}
