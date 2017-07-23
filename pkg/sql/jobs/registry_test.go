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
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package jobs

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// mockNodeLiveness allows simulating liveness failures without the full
// storage.NodeLiveness machinery.
type mockNodeLiveness struct {
	syncutil.Mutex
	ch       chan struct{}
	clock    *hlc.Clock
	liveness storage.Liveness
}

func newMockNodeLiveness(clock *hlc.Clock) *mockNodeLiveness {
	return &mockNodeLiveness{
		ch:    make(chan struct{}),
		clock: clock,
		liveness: storage.Liveness{
			NodeID:     1,
			Epoch:      1,
			Expiration: hlc.MaxTimestamp,
		},
	}
}

// Self implements the nodeLiveness interface. On every call, a nonblocking send
// is performed over nl.ch to allow tests to execute a callback.
func (nl *mockNodeLiveness) Self() (*storage.Liveness, error) {
	select {
	case nl.ch <- struct{}{}:
	default:
	}
	nl.Lock()
	defer nl.Unlock()
	return &nl.liveness, nil
}

func (nl *mockNodeLiveness) incrementEpoch() {
	nl.Lock()
	defer nl.Unlock()
	nl.liveness.Epoch++
}

func (nl *mockNodeLiveness) setExpiration(ts hlc.Timestamp) {
	nl.Lock()
	defer nl.Unlock()
	nl.liveness.Expiration = ts
}
func TestRegistryCancelation(t *testing.T) {
	ctx, stopper := context.Background(), stop.NewStopper()
	defer stopper.Stop(ctx)

	var db *client.DB
	var ex sqlutil.InternalExecutor
	clock := hlc.NewClock(hlc.UnixNano, time.Nanosecond)

	registry := MakeRegistry(clock, db, ex)
	nodeLiveness := newMockNodeLiveness(clock)

	// Poll liveness as fast as the scheduler will allow to keep this test fast.
	if err := registry.WatchLiveness(stopper, nodeLiveness, time.Nanosecond); err != nil {
		t.Fatal(err)
	}

	wait := func() {
		// Every turn of the registry's liveness poll loop will generate exactly one
		// call to nodeLiveness.Self. Only after we've witnessed two calls can we be
		// sure that the first turn of the registry's loop has completed.
		//
		// Waiting for only the first call to nodeLiveness.Self is racy, as we'd
		// perform our assertions concurrently with the registry loop's observation
		// of our injected liveness failure, if any.
		<-nodeLiveness.ch
		<-nodeLiveness.ch
	}

	cancelCount := 0
	cancel := func() { cancelCount++ }

	register := func(id int64, cancel func()) {
		if err := registry.register(id, cancel); err != nil {
			t.Fatal(err)
		}
	}

	// Jobs that complete while the node is live should not be canceled.
	register(1, cancel)
	wait()
	registry.unregister(1)
	wait()
	if e, a := 0, cancelCount; e != a {
		t.Fatalf("expected cancelCount of %d, but got %d", e, a)
	}

	// Jobs that are in-progress when the liveness epoch is incremented should be
	// canceled.
	register(2, cancel)
	nodeLiveness.incrementEpoch()
	wait()
	if e, a := 1, cancelCount; e != a {
		t.Fatalf("expected cancelCount of %d, but got %d", e, a)
	}

	// Jobs started in the new epoch that complete while the new epoch is live
	// should not be canceled.
	register(3, cancel)
	wait()
	registry.unregister(3)
	wait()
	if e, a := 1, cancelCount; e != a {
		t.Fatalf("expected cancelCount of %d, but got %d", e, a)
	}

	// Jobs that are in-progress when the liveness lease expires should be
	// canceled.
	register(4, cancel)
	nodeLiveness.setExpiration(hlc.MinTimestamp)
	wait()
	if e, a := 2, cancelCount; e != a {
		t.Fatalf("expected cancelCount of %d, but got %d", e, a)
	}

	// Jobs that are started while the liveness lease is expired should be
	// canceled.
	register(5, cancel)
	wait()
	if e, a := 3, cancelCount; e != a {
		t.Fatalf("expected cancelCount of %d, but got %d", e, a)
	}
}

func TestRegistryRegister(t *testing.T) {
	var db *client.DB
	var ex sqlutil.InternalExecutor

	registry := MakeRegistry(hlc.NewClock(hlc.UnixNano, time.Nanosecond), db, ex)

	if err := registry.register(42, func() {}); err != nil {
		t.Fatal(err)
	}

	if err := registry.register(42, func() {}); !testutils.IsError(err, "already tracking job ID") {
		t.Fatalf("expected 'already tracking job ID', but got '%s'", err)
	}

	// Unregistering the same ID multiple times is not an error.
	registry.unregister(42)
	registry.unregister(42)
}
