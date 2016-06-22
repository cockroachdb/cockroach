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
// Author: Peter Mattis (peter@cockroachlabs.com)

package storage_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/server"
	"github.com/cockroachdb/cockroach/util/leaktest"
)

func TestEagerReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, stopper, _ := createTestStore(t)
	defer stopper.Stop()

	// Disable the replica scanner so that we rely on the eager replication code
	// path that occurs after splits.
	store.SetReplicaScannerDisabled(true)

	if err := server.WaitForInitialSplits(store.DB()); err != nil {
		t.Fatal(err)
	}

	// After the initial splits have been performed, all of the resulting ranges
	// should be present in replicate queue purgatory (because we only have a
	// single store in the test and thus replication cannot succeed).
	expected := server.ExpectedInitialRangeCount()
	if n := store.ReplicateQueuePurgatoryLength(); expected != n {
		t.Fatalf("expected %d replicas in purgatory, but found %d",
			expected, n)
	}
}
