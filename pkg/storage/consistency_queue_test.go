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
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package storage_test

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// TestConsistencyQueueRequiresLive verifies the queue will not
// process ranges whose replicas are not all live.
func TestConsistencyQueueRequiresLive(t *testing.T) {
	defer leaktest.AfterTest(t)()
	sc := storage.TestStoreConfig(nil)
	mtc := &multiTestContext{storeConfig: &sc}
	defer mtc.Stop()
	mtc.Start(t, 3)

	// Replicate the range to three nodes.
	repl := mtc.stores[0].LookupReplica(roachpb.RKeyMin, nil)
	rangeID := repl.RangeID
	mtc.replicateRange(rangeID, 1, 2)

	// Verify that queueing is immediately possible.
	if shouldQ, priority := mtc.stores[0].ConsistencyQueueShouldQueue(
		context.TODO(), mtc.clock.Now(), repl, config.SystemConfig{}); !shouldQ {
		t.Fatalf("expected shouldQ true; got %t, %f", shouldQ, priority)
	}

	// Stop a node and expire leases.
	mtc.stopStore(2)
	mtc.advanceClock(context.TODO())

	if shouldQ, priority := mtc.stores[0].ConsistencyQueueShouldQueue(
		context.TODO(), mtc.clock.Now(), repl, config.SystemConfig{}); shouldQ {
		t.Fatalf("expected shouldQ false; got %t, %f", shouldQ, priority)
	}
}
