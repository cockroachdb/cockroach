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
// Author: Bram Gruneir (bram.gruneir@gmail.com)

package storage

import (
	"testing"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

// TestIsRed ensures that the isRed function is correct.
func TestIsRed(t *testing.T) {
	defer leaktest.AfterTest(t)
	testCases := []struct {
		node     *proto.RangeTreeNode
		expected bool
	}{
		// normal black node
		{&proto.RangeTreeNode{Black: true}, false},
		// normal red node
		{&proto.RangeTreeNode{Black: false}, true},
		// nil
		{nil, false},
	}
	for i, test := range testCases {
		node := test.node
		actual := isRed(node)
		if actual != test.expected {
			t.Errorf("%d: %+v expect %v; got %v", i, node, test.expected, actual)
		}
	}
}
