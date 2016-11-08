// Copyright 2016 The Cockroach Authors.
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

package gossip

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNodeIDFromKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		key     string
		nodeID  roachpb.NodeID
		success bool
	}{
		{MakeNodeIDKey(0), 0, true},
		{MakeNodeIDKey(1), 1, true},
		{MakeNodeIDKey(123), 123, true},
		{MakeNodeIDKey(123) + "foo", 0, false},
		{"foo" + MakeNodeIDKey(123), 0, false},
		{KeyNodeIDPrefix, 0, false},
		{KeyNodeIDPrefix + ":", 0, false},
		{KeyNodeIDPrefix + ":foo", 0, false},
		{"123", 0, false},
		{MakePrefixPattern(KeyNodeIDPrefix), 0, false},
		{MakeNodeLivenessKey(1), 0, false},
		{MakeStoreKey(1), 0, false},
		{MakeDeadReplicasKey(1), 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			nodeID, err := NodeIDFromKey(tc.key)
			if err != nil {
				if tc.success {
					t.Errorf("expected success, got error: %s", err)
				}
			} else if !tc.success {
				t.Errorf("expected failure, got nodeID %d", nodeID)
			} else if nodeID != tc.nodeID {
				t.Errorf("expected NodeID=%d, got %d", tc.nodeID, nodeID)
			}
		})
	}
}
