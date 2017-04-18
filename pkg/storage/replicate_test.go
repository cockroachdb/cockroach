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

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func TestEagerReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Start with the split queue disabled.
	storeCfg := storage.TestStoreConfig(nil)
	storeCfg.TestingKnobs.DisableSplitQueue = true
	stopper := stop.NewStopper()
	defer stopper.Stop(context.TODO())
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// Disable the replica scanner so that we rely on the eager replication code
	// path that occurs after splits.
	store.SetReplicaScannerActive(false)
	// Enable the split queue and force a scan and process.
	store.SetSplitQueueActive(true)
	store.ForceSplitScanAndProcess()

	// The addition of replicas to the replicateQueue after a split
	// occurs happens after the update of the descriptors in meta2
	// leaving a tiny window of time in which the newly split replica
	// will not have been added to purgatory. Thus we loop.
	testutils.SucceedsSoon(t, func() error {
		// After the initial splits have been performed, all of the resulting ranges
		// should be present in replicate queue purgatory (because we only have a
		// single store in the test and thus replication cannot succeed).
		expected, err := server.ExpectedInitialRangeCount(store.DB())
		if err != nil {
			return err
		}
		if n := store.ReplicateQueuePurgatoryLength(); expected != n {
			return errors.Errorf("expected %d replicas in purgatory, but found %d", expected, n)
		}
		return nil
	})
}
