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
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/pkg/errors"
)

func TestEagerReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	store, stopper, _ := createTestStore(t)
	defer stopper.Stop()

	// Disable the replica scanner so that we rely on the eager replication code
	// path that occurs after splits.
	store.SetReplicaScannerActive(false)

	if err := server.WaitForInitialSplits(store.DB()); err != nil {
		t.Fatal(err)
	}

	// WaitForInitialSplits will return as soon as the meta2 span contains the
	// expected number of descriptors. But the addition of replicas to the
	// replicateQueue after a split occurs happens after the update of the
	// descriptors in meta2 leaving a tiny window of time in which the newly
	// split replica will not have been added to purgatory. Thus we loop.
	util.SucceedsSoon(t, func() error {
		// After the initial splits have been performed, all of the resulting ranges
		// should be present in replicate queue purgatory (because we only have a
		// single store in the test and thus replication cannot succeed).
		expected := server.ExpectedInitialRangeCount()
		if n := store.ReplicateQueuePurgatoryLength(); expected != n {
			return errors.Errorf("expected %d replicas in purgatory, but found %d", expected, n)
		}
		return nil
	})
}
