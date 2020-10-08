// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			nodeID, err := NodeIDFromKey(tc.key, KeyNodeIDPrefix)
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

func TestStoreIDFromKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		key     string
		storeID roachpb.StoreID
		success bool
	}{
		{MakeStoreKey(0), 0, true},
		{MakeStoreKey(1), 1, true},
		{MakeStoreKey(123), 123, true},
		{MakeStoreKey(123) + "foo", 0, false},
		{"foo" + MakeStoreKey(123), 0, false},
		{KeyStorePrefix, 0, false},
		{"123", 0, false},
		{MakePrefixPattern(KeyStorePrefix), 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			storeID, err := StoreIDFromKey(tc.key)
			if err != nil {
				if tc.success {
					t.Errorf("expected success, got error: %s", err)
				}
			} else if !tc.success {
				t.Errorf("expected failure, got storeID %d", storeID)
			} else if storeID != tc.storeID {
				t.Errorf("expected StoreID=%d, got %d", tc.storeID, storeID)
			}
		})
	}
}
