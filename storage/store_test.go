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

import "testing"

var testIdent = StoreIdent{
	ClusterID: "cluster",
	NodeID:    1,
	StoreID:   1,
}

// TestStoreInitAndBootstrap verifies store initialization and
// bootstrap.
func TestStoreInitAndBootstrap(t *testing.T) {
	engine := NewInMem(1 << 20)
	store := NewStore(engine, nil)

	// Can't init as haven't bootstrapped.
	if err := store.Init(); err == nil {
		t.Error("expected failure init'ing un-bootstrapped store")
	}

	// Bootstrap with a fake ident.
	if err := store.Bootstrap(testIdent); err != nil {
		t.Errorf("error bootstrapping store: %v", err)
	}

	// Try to get 1st range--non-existent.
	if _, err := store.GetRange(1); err == nil {
		t.Error("expected error fetching non-existent range")
	}

	// Create range and fetch.
	if _, err := store.CreateRange(KeyMin, KeyMax); err != nil {
		t.Errorf("failure to create first range: %v", err)
	}
	if _, err := store.GetRange(1); err != nil {
		t.Errorf("failure fetching 1st range: %v", err)
	}

	// Now, attempt to initialize a store with a now-bootstrapped engine.
	store = NewStore(engine, nil)
	if err := store.Init(); err != nil {
		t.Errorf("failure initializing bootstrapped store: %v", err)
	}
	// 1st range should be available.
	if _, err := store.GetRange(1); err != nil {
		t.Errorf("failure fetching 1st range: %v", err)
	}
}

// TestBootstrapOfNonEmptyStore verifies bootstrap failure if engine
// is not empty.
func TestBootstrapOfNonEmptyStore(t *testing.T) {
	engine := NewInMem(1 << 20)

	// Put some random garbage into the engine.
	if err := engine.put(Key("foo"), Value{Bytes: []byte("bar")}); err != nil {
		t.Errorf("failure putting key foo into engine: %v", err)
	}
	store := NewStore(engine, nil)

	// Can't init as haven't bootstrapped.
	if err := store.Init(); err == nil {
		t.Error("expected failure init'ing un-bootstrapped store")
	}

	// Bootstrap should fail on non-empty engine.
	if err := store.Bootstrap(testIdent); err == nil {
		t.Error("expected bootstrap error on non-empty store")
	}
}
