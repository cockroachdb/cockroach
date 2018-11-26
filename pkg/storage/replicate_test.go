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

package storage_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
)

func TestEagerReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	storeCfg := storage.TestStoreConfig(nil /* clock */)
	// Disable the replica scanner so that we rely on the eager replication code
	// path that occurs after splits.
	storeCfg.TestingKnobs.DisableScanner = true
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)
	store := createTestStoreWithConfig(t, stopper, storeCfg)

	// After bootstrap, all of the system ranges should be present in replicate
	// queue purgatory (because we only have a single store in the test and thus
	// replication cannot succeed).
	purgatoryStartCount := store.ReplicateQueuePurgatoryLength()

	// Perform a split and check that there's one more range in the purgatory.

	key := roachpb.Key("a")
	args := adminSplitArgs(key)
	_, pErr := client.SendWrapped(ctx, store.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// The addition of replicas to the replicateQueue after a split
	// occurs happens after the update of the descriptors in meta2
	// leaving a tiny window of time in which the newly split replica
	// will not have been added to purgatory. Thus we loop.
	testutils.SucceedsSoon(t, func() error {
		expected := purgatoryStartCount + 1
		if n := store.ReplicateQueuePurgatoryLength(); expected != n {
			return errors.Errorf("expected %d replicas in purgatory, but found %d", expected, n)
		}
		return nil
	})
}
