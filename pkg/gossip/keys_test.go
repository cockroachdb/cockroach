// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
		{KeyNodeDescPrefix, 0, false},
		{KeyNodeDescPrefix + ":", 0, false},
		{KeyNodeDescPrefix + ":foo", 0, false},
		{"123", 0, false},
		{MakePrefixPattern(KeyNodeDescPrefix), 0, false},
		{MakeNodeLivenessKey(1), 0, false},
		{MakeStoreDescKey(1), 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			nodeID, err := DecodeNodeDescKey(tc.key, KeyNodeDescPrefix)
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
		{MakeStoreDescKey(0), 0, true},
		{MakeStoreDescKey(1), 1, true},
		{MakeStoreDescKey(123), 123, true},
		{MakeStoreDescKey(123) + "foo", 0, false},
		{"foo" + MakeStoreDescKey(123), 0, false},
		{KeyStoreDescPrefix, 0, false},
		{"123", 0, false},
		{MakePrefixPattern(KeyStoreDescPrefix), 0, false},
	}

	for _, tc := range testCases {
		t.Run(tc.key, func(t *testing.T) {
			storeID, err := DecodeStoreDescKey(tc.key)
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
