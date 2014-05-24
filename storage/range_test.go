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
	"net"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/rpc"
	yaml "gopkg.in/yaml.v1"
)

var (
	testRangeLocations = RangeLocations{
		StartKey: KeyMin,
		Replicas: []Replica{
			{
				NodeID:     1,
				StoreID:    1,
				RangeID:    1,
				Datacenter: "dc1",
				DiskType:   MEM,
			},
			{
				NodeID:     2,
				StoreID:    1,
				RangeID:    1,
				Datacenter: "dc2",
				DiskType:   MEM,
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
		Replicas: map[string][]string{
			"dc1": []string{"MEM"},
			"dc2": []string{"MEM"},
		},
	}
)

// marshalConfig marshals the specified configI interface into yaml
// bytes.
func marshalConfig(configI interface{}, t *testing.T) []byte {
	bytes, err := yaml.Marshal(configI)
	if err != nil {
		t.Fatal(err)
	}
	return bytes
}

// createTestEngine creates an in-memory engine and initializes some
// default configuration settings.
func createTestEngine(t *testing.T) Engine {
	engine := NewInMem(1 << 20)
	if err := engine.put(KeyConfigAccountingPrefix, Value{Bytes: marshalConfig(testDefaultAcctConfig, t)}); err != nil {
		t.Fatal(err)
	}
	if err := engine.put(KeyConfigPermissionPrefix, Value{Bytes: marshalConfig(testDefaultPermConfig, t)}); err != nil {
		t.Fatal(err)
	}
	if err := engine.put(KeyConfigZonePrefix, Value{Bytes: marshalConfig(testDefaultZoneConfig, t)}); err != nil {
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
		Replicas: testRangeLocations,
	}
	g := gossip.New(rpc.NewServer(&net.UnixAddr{"fake", "unix"}))
	return NewRange(rm, engine, nil, g), g
}

// TestRangeGossipFirstRange verifies that the first range gossips its location.
func TestRangeGossipFirstRange(t *testing.T) {
	_, g := createTestRange(createTestEngine(t), t)
	info, err := g.GetInfo(gossip.KeyFirstRangeMetadata)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(info.(RangeLocations), testRangeLocations) {
		t.Errorf("expected gossipped range locations to be equal: %s vs %s", info.(RangeLocations), testRangeLocations)
	}
}

// TestRangeGossipAllConfigs verifies that all config types are
// gossipped.
func TestRangeGossipAllConfigs(t *testing.T) {
	_, g := createTestRange(createTestEngine(t), t)
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
		configMap := info.(*prefixConfigMap)
		if !reflect.DeepEqual(configMap.configs, test.configs) {
			t.Errorf("expected gossiped configs to be equal %s vs %s", configMap.configs, test.configs)
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
	if err := engine.put(key, Value{Bytes: marshalConfig(db1Perm, t)}); err != nil {
		t.Fatal(err)
	}
	_, g := createTestRange(engine, t)

	info, err := g.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(*prefixConfigMap)
	expConfigs := []*prefixConfig{
		&prefixConfig{KeyMin, &testDefaultPermConfig},
		&prefixConfig{Key("/db1"), &db1Perm},
		&prefixConfig{PrefixEndKey(Key("/db1")), &testDefaultPermConfig},
	}
	if !reflect.DeepEqual(configMap.configs, expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configMap.configs, expConfigs)
	}
}

// TestRangeGossipConfigUpdates verifies that writes to the
// permissions cause the updated configs to be re-gossipped.
func TestRangeGossipConfigUpdates(t *testing.T) {
	r, g := createTestRange(createTestEngine(t), t)
	// Add a permission for a new key prefix.
	db1Perm := PermConfig{
		Perms: []Permission{
			{Users: []string{"spencer"}, Read: true, Write: true, Priority: 100.0},
		},
	}
	key := MakeKey(KeyConfigPermissionPrefix, Key("/db1"))
	reply := &PutResponse{}
	r.Put(&PutRequest{Key: key, Value: Value{Bytes: marshalConfig(db1Perm, t)}}, reply)
	if reply.Error != nil {
		t.Fatal(reply.Error)
	}

	info, err := g.GetInfo(gossip.KeyConfigPermission)
	if err != nil {
		t.Fatal(err)
	}
	configMap := info.(*prefixConfigMap)
	expConfigs := []*prefixConfig{
		&prefixConfig{KeyMin, &testDefaultPermConfig},
		&prefixConfig{Key("/db1"), &db1Perm},
		&prefixConfig{PrefixEndKey(Key("/db1")), &testDefaultPermConfig},
	}
	if !reflect.DeepEqual(configMap.configs, expConfigs) {
		t.Errorf("expected gossiped configs to be equal %s vs %s", configMap.configs, expConfigs)
	}
}
