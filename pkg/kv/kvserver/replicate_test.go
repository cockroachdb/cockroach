// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestEagerReplication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.WithIssue(t, 54646, "flaky test")
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	serv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				// Disable the replica scanner so that we rely on the eager replication code
				// path that occurs after splits.
				DisableScanner: true,
			},
		},
	})
	s := serv.(*server.TestServer)
	defer s.Stopper().Stop(ctx)
	store, err := s.Stores().GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	// After bootstrap, all of the system ranges should be present in replicate
	// queue purgatory (because we only have a single store in the test and thus
	// replication cannot succeed).
	purgatoryStartCount := store.ReplicateQueuePurgatoryLength()

	t.Logf("purgatory start count is %d", purgatoryStartCount)
	// Perform a split and check that there's one more range in the purgatory.

	key := roachpb.Key("a")
	args := adminSplitArgs(key)
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), args)
	if pErr != nil {
		t.Fatal(pErr)
	}

	// The addition of replicas to the replicateQueue after a split
	// occurs happens after the update of the descriptors in meta2
	// leaving a tiny window of time in which the newly split replicas
	// will not have been added to purgatory. Thus we loop.
	testutils.SucceedsSoon(t, func() error {
		expected := purgatoryStartCount + 2
		if n := store.ReplicateQueuePurgatoryLength(); expected != n {
			return errors.Errorf("expected %d replicas in purgatory, but found %d", expected, n)
		}
		return nil
	})
}
