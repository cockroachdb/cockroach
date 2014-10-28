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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Kathy Spradlin (kathyspradlin@gmail.com)
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"bytes"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
)

func TestGetFirstRangeDescriptor(t *testing.T) {
	n := gossip.NewSimulationNetwork(3, "unix", gossip.DefaultTestGossipInterval)
	ds := NewDistSender(n.Nodes[0].Gossip)
	if _, err := ds.getFirstRangeDescriptor(); err == nil {
		t.Errorf("expected not to find first range descriptor")
	}
	expectedDesc := &proto.RangeDescriptor{}
	expectedDesc.StartKey = proto.Key("a")
	expectedDesc.EndKey = proto.Key("c")

	// Add first RangeDescriptor to a node different from the node for
	// this dist sender and ensure that this dist sender has the
	// information within a given time.
	n.Nodes[1].Gossip.AddInfo(
		gossip.KeyFirstRangeDescriptor, *expectedDesc, time.Hour)
	maxCycles := 10
	n.SimulateNetwork(func(cycle int, network *gossip.SimulationNetwork) bool {
		desc, err := ds.getFirstRangeDescriptor()
		if err != nil {
			if cycle >= maxCycles {
				t.Errorf("could not get range descriptor after %d cycles", cycle)
				return false
			}
			return true
		}
		if !bytes.Equal(desc.StartKey, expectedDesc.StartKey) ||
			!bytes.Equal(desc.EndKey, expectedDesc.EndKey) {
			t.Errorf("expected first range descriptor %v, instead was %v",
				expectedDesc, desc)
		}
		return false
	})
	n.Stop()
}

func TestVerifyPermissions(t *testing.T) {
	n := gossip.NewSimulationNetwork(1, "unix", gossip.DefaultTestGossipInterval)
	ds := NewDistSender(n.Nodes[0].Gossip)
	config1 := &proto.PermConfig{
		Read:  []string{"read1", "readAll", "rw", "rwAll"},
		Write: []string{"write1", "writeAll", "rw", "rwAll"}}
	config2 := &proto.PermConfig{
		Read:  []string{"read2", "readAll", "rw2", "rwAll"},
		Write: []string{"write2", "writeAll", "rw2", "rwAll"}}
	configs := []*storage.PrefixConfig{
		{engine.KeyMin, nil, config1},
		{proto.Key("a"), nil, config2},
	}
	configMap, err := storage.NewPrefixConfigMap(configs)
	if err != nil {
		t.Fatalf("failed to make prefix config map, err: %s", err.Error())
	}
	ds.gossip.AddInfo(gossip.KeyConfigPermission, configMap, time.Hour)

	readMethods := proto.ReadMethods
	writeMethods := proto.WriteMethods
	readOnlyMethods := make([]string, 0, len(readMethods))
	writeOnlyMethods := make([]string, 0, len(writeMethods))
	readWriteMethods := make([]string, 0, len(readMethods)+len(writeMethods))
	for _, readM := range readMethods {
		if proto.IsReadOnly(readM) {
			readOnlyMethods = append(readOnlyMethods, readM)
		} else {
			readWriteMethods = append(readWriteMethods, readM)
		}
	}
	for _, writeM := range writeMethods {
		if !proto.NeedReadPerm(writeM) {
			writeOnlyMethods = append(writeOnlyMethods, writeM)
		}
	}

	testData := []struct {
		// Permission-based db methods from the storage package.
		methods          []string
		user             string
		startKey, endKey proto.Key
		hasPermission    bool
	}{
		// Test permissions within a single range
		{readOnlyMethods, "read1", engine.KeyMin, engine.KeyMin, true},
		{readOnlyMethods, "rw", engine.KeyMin, engine.KeyMin, true},
		{readOnlyMethods, "write1", engine.KeyMin, engine.KeyMin, false},
		{readOnlyMethods, "random", engine.KeyMin, engine.KeyMin, false},
		{readWriteMethods, "rw", engine.KeyMin, engine.KeyMin, true},
		{readWriteMethods, "read1", engine.KeyMin, engine.KeyMin, false},
		{readWriteMethods, "write1", engine.KeyMin, engine.KeyMin, false},
		{writeOnlyMethods, "write1", engine.KeyMin, engine.KeyMin, true},
		{writeOnlyMethods, "rw", engine.KeyMin, engine.KeyMin, true},
		{writeOnlyMethods, "read1", engine.KeyMin, engine.KeyMin, false},
		{writeOnlyMethods, "random", engine.KeyMin, engine.KeyMin, false},
		// Test permissions across both ranges
		{readOnlyMethods, "readAll", engine.KeyMin, proto.Key("b"), true},
		{readOnlyMethods, "read1", engine.KeyMin, proto.Key("b"), false},
		{readOnlyMethods, "read2", engine.KeyMin, proto.Key("b"), false},
		{readOnlyMethods, "random", engine.KeyMin, proto.Key("b"), false},
		{readWriteMethods, "rwAll", engine.KeyMin, proto.Key("b"), true},
		{readWriteMethods, "rw", engine.KeyMin, proto.Key("b"), false},
		{readWriteMethods, "random", engine.KeyMin, proto.Key("b"), false},
		{writeOnlyMethods, "writeAll", engine.KeyMin, proto.Key("b"), true},
		{writeOnlyMethods, "write1", engine.KeyMin, proto.Key("b"), false},
		{writeOnlyMethods, "write2", engine.KeyMin, proto.Key("b"), false},
		{writeOnlyMethods, "random", engine.KeyMin, proto.Key("b"), false},
		// Test permissions within and around the boundaries of a range,
		// representatively using rw methods.
		{readWriteMethods, "rw2", proto.Key("a"), proto.Key("b"), true},
		{readWriteMethods, "rwAll", proto.Key("a"), proto.Key("b"), true},
		{readWriteMethods, "rw2", proto.Key("a"), proto.Key("a"), true},
		{readWriteMethods, "rw2", proto.Key("a"), proto.Key("a1"), true},
		{readWriteMethods, "rw2", proto.Key("a"), proto.Key("b1"), false},
		{readWriteMethods, "rw2", proto.Key("a3"), proto.Key("a4"), true},
		{readWriteMethods, "rw2", proto.Key("a3"), proto.Key("b1"), false},
	}

	for _, test := range testData {
		for _, method := range test.methods {
			err := ds.verifyPermissions(
				method,
				&proto.RequestHeader{
					User: test.user, Key: test.startKey, EndKey: test.endKey})
			if err != nil && test.hasPermission {
				t.Errorf("user: %s should have had permission to %s, err: %s",
					test.user, method, err.Error())
			} else if err == nil && !test.hasPermission {
				t.Errorf("user: %s should not have had permission to %s",
					test.user, method)
			}
		}
	}
	n.Stop()
}
