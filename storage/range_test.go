// Copyright 2014 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"encoding/gob"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/gossip"
)

var (
	testRangeDescriptor = RangeDescriptor{
		StartKey: KeyMin,
		Replicas: []Replica{
			{
				NodeID:  1,
				StoreID: 1,
				RangeID: 1,
				Attrs:   Attributes([]string{"dc1", "mem"}),
			},
			{
				NodeID:  2,
				StoreID: 1,
				RangeID: 1,
				Attrs:   Attributes([]string{"dc2", "mem"}),
			},
		},
	}
	testDefaultAcctConfig = AcctConfig{}
	testDefaultPermConfig = PermConfig{
		Perms: []Permission{
			{Read: true, Write: true},
		},
	}
	testDefaultZoneConfig = ZoneConfig{
		Replicas: []Attributes{
			Attributes([]string{"dc1", "mem"}),
			Attributes([]string{"dc2", "mem"}),
		},
	}
)

// createTestEngine creates an in-memory engine and initializes some
// default configuration settings.
func createTestEngine(t *testing.T) Engine {
	engine := NewInMem(Attributes([]string{"dc1", "mem"}), 1<<20)
	if err := putI(engine, KeyConfigAccountingPrefix, testDefaultAcctConfig); err != nil {
		t.Fatal(err)
	}
	if err := putI(engine, KeyConfigPermissionPrefix, testDefaultPermConfig); err != nil {
		t.Fatal(err)
	}
	if err := putI(engine, KeyConfigZonePrefix, testDefaultZoneConfig); err != nil {
		t.Fatal(err)
	}
	return engine
}

// createTestRange creates a new range initialized to the full extent
// of the keyspace. The gossip instance is also returned for testing.
func createTestRange(engine Engine, t *testing.T) (*Range, *gossip.Gossip) {
	rm := RangeMetadata{
		RangeID:  0,
		StartKey: KeyMin,
		EndKey:   KeyMax,
		Replicas: testRangeDescriptor,
	}
	g := gossip.New()
	r := NewRange(rm, engine, nil, g)
	r.Start()
	return r, g
}

// TestRangeGossipFirstRange verifies that the first range gossips its location.
func TestRangeGossipFirstRange(t *testing.T) {
	r, g := createTestRange(createTestEngine(t), t)
	defer r.Stop()
	info, err := g.GetInfo(gossip.KeyFirstRangeMetadata)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info.(RangeDescriptor), testRangeDescriptor) {
		t.Errorf("expected gossipped range locations to be equal: %s vs %s", info.(RangeDescriptor), testRangeDescriptor)
	}
}

// TestRangeGossipAllConfigs verifies that all config types are
// gossipped.
func TestRangeGossipAllConfigs(t *testing.T) {
	r, g := createTestRange(createTestEngine(t), t)
	defer r.Stop()
	testData := []struct {
		gossipKey string
		configs   []*prefixConfig
	}{
		{gossip.KeyConfigAccounting, []*prefixConfig{&prefixConfig{KeyMin, &testDefaultAcctConfig}}},
		{gossip.KeyConfigPermission, []*prefixConfig{&prefixConfig{KeyMin, &testDefaultPermConfig}}},
		{gossip.KeyConfigZone, []*prefixConfig{&prefixConfig{KeyMin, &testDefaultZoneConfig}}},
	}
	for _, test := range testData {
		info, err := g.GetInfo(test.gossipKey)
		if err != nil {
			t.Fatal(err)
		}
		configs := info.([]*prefixConfig)
		if !reflect.DeepEqual(configs, test.configs) {
			t.Errorf("expected gossiped configs to be equal %s vs %s", configs, test.configs)
		}
	}
}

// TestRangeGossipConfigWithMultipleKeyPrefixes verifies that multiple
// key prefixes for a config are gossipped.
func TestRangeGossipConfigWithMultipleKeyPrefixes(t *testing.T) {
	engine := createTestEngine(t)
	// Add a permission for a new key prefix.
	db1Perm := PermConfig{
		Perms: []Permission{
			{Users: []string{"spencer"}, Read: true, Write: true, Priority: 100.0},
			{Users: []string{"foo", "bar", "baz"}, Read: true, Write: false, Priority: 10.0},
		},
	}
	key := MakeKey(KeyConfigPermissionPrefix, Key("/db1"))
	if err := putI(engine, key, db1Perm); err != nil {
		t.Fatal(err)
	}
	r, g := createTestRange(engine, t)
	defer r.Stop()

	info, err := g.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configs := info.([]*prefixConfig)
	expConfigs := []*prefixConfig{
		&prefixConfig{KeyMin, &testDefaultPermConfig},
		&prefixConfig{Key("/db1"), &db1Perm},
	}
	if !reflect.DeepEqual(configs, expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configs, expConfigs)
	}
}

// TestRangeGossipConfigUpdates verifies that writes to the
// permissions cause the updated configs to be re-gossipped.
func TestRangeGossipConfigUpdates(t *testing.T) {
	r, g := createTestRange(createTestEngine(t), t)
	defer r.Stop()
	// Add a permission for a new key prefix.
	db1Perm := PermConfig{
		Perms: []Permission{
			{Users: []string{"spencer"}, Read: true, Write: true, Priority: 100.0},
		},
	}
	key := MakeKey(KeyConfigPermissionPrefix, Key("/db1"))
	reply := &PutResponse{}

	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(db1Perm); err != nil {
		t.Fatal(err)
	}
	r.Put(&PutRequest{Key: key, Value: Value{Bytes: buf.Bytes()}}, reply)
	if reply.Error != nil {
		t.Fatal(reply.Error)
	}

	info, err := g.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configs := info.([]*prefixConfig)
	expConfigs := []*prefixConfig{
		&prefixConfig{KeyMin, &testDefaultPermConfig},
		&prefixConfig{Key("/db1"), &db1Perm},
	}
	if !reflect.DeepEqual(configs, expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configs, expConfigs)
	}
}
