// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstreamer

import (
	"context"
	"math"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestStreamerLimitations verifies that the streamer panics or encounters
// errors in currently unsupported or invalid scenarios.
func TestStreamerLimitations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	getStreamer := func() *Streamer {
		return NewStreamer(
			s.DistSenderI().(*kvcoord.DistSender),
			s.Stopper(),
			kv.NewTxn(ctx, s.DB(), s.NodeID()),
			cluster.MakeTestingClusterSettings(),
			lock.WaitPolicy(0),
			math.MaxInt64,
			nil, /* acc */
		)
	}

	t.Run("InOrder mode unsupported", func(t *testing.T) {
		require.Panics(t, func() {
			streamer := getStreamer()
			streamer.Init(InOrder, Hints{UniqueRequests: true})
		})
	})

	t.Run("non-unique requests unsupported", func(t *testing.T) {
		require.Panics(t, func() {
			streamer := getStreamer()
			streamer.Init(OutOfOrder, Hints{UniqueRequests: false})
		})
	})

	t.Run("invalid enqueueKeys", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Cancel()
		streamer.Init(OutOfOrder, Hints{UniqueRequests: true})
		// Use a single request but two keys which is invalid.
		reqs := []roachpb.RequestUnion{{Value: &roachpb.RequestUnion_Get{}}}
		enqueueKeys := []int{0, 1}
		require.Error(t, streamer.Enqueue(ctx, reqs, enqueueKeys))
	})

	t.Run("pipelining unsupported", func(t *testing.T) {
		streamer := getStreamer()
		defer streamer.Cancel()
		streamer.Init(OutOfOrder, Hints{UniqueRequests: true})
		get := roachpb.NewGet(roachpb.Key("key"), false /* forUpdate */)
		reqs := []roachpb.RequestUnion{{
			Value: &roachpb.RequestUnion_Get{
				Get: get.(*roachpb.GetRequest),
			},
		}}
		require.NoError(t, streamer.Enqueue(ctx, reqs, nil /* enqueueKeys */))
		// It is invalid to enqueue more requests before the previous have been
		// responded to.
		require.Error(t, streamer.Enqueue(ctx, reqs, nil /* enqueueKeys */))
	})
}

// TestLargeKeys verifies that the Streamer successfully completes the queries
// when the keys to lookup are large (i.e. the enqueued requests themselves have
// large memory footprint).
func TestLargeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	// Lower the distsql_workmem limit so that we can operate with smaller
	// blobs. Note that the joinReader in the row-by-row engine will override
	// the limit if it is lower than 8MiB, so we cannot go lower than that here.
	_, err := db.Exec("SET distsql_workmem='8MiB'")
	require.NoError(t, err)
	// We set up such a table that contains two large columns, one of them being
	// the primary key. The idea is that the query below will first read from
	// the secondary index which would include only the PK blob, and that will
	// be used to construct index join lookups (i.e. the PK blobs will be the
	// enqueued requests for the Streamer) whereas the other blob will be part
	// of the response.
	_, err = db.Exec("CREATE TABLE foo (pk_blob STRING PRIMARY KEY, attribute INT, blob TEXT, INDEX(attribute))")
	require.NoError(t, err)

	// Insert a handful of large rows.
	rng, _ := randutil.NewTestRand()
	numRows := rng.Intn(3) + 3
	// In both engines, the index joiner buffers input rows up to 4MiB in size,
	// so we have a couple of interesting options for the blob size:
	// - 3000000 is interesting because it doesn't exceed the buffer size, yet
	// two rows with such blobs do exceed it. The index joiners are expected to
	// to process each row on its own.
	// - 5000000 is interesting because a single row already exceeds the buffer
	// size.
	blobSize := []int{3000000, 5000000}[rng.Intn(2)]
	for i := 0; i < numRows; i++ {
		letter := string(byte('a') + byte(i))
		_, err = db.Exec("INSERT INTO foo SELECT repeat($1, $2), 1, repeat($1, $2)", letter, blobSize)
		require.NoError(t, err)
	}

	// Perform an index join so that the Streamer API is used.
	query := "SELECT * FROM foo@foo_attribute_idx WHERE attribute=1"
	testutils.RunTrueAndFalse(t, "vectorize", func(t *testing.T, vectorize bool) {
		vectorizeMode := "off"
		if vectorize {
			vectorizeMode = "on"
		}
		_, err = db.Exec("SET vectorize = " + vectorizeMode)
		require.NoError(t, err)
		_, err = db.Exec(query)
		require.NoError(t, err)
	})
}
