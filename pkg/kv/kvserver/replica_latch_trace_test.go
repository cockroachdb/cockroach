// Copyright 2021 The Cockroach Authors.
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
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReplicaTraceForSlowLatchAcquisition(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	knobs := &kvserver.StoreTestingKnobs{}

	errIntercepted := errors.New("intercepted")

	blocked := struct {
		sync.Once
		ch chan struct{}
	}{
		ch: make(chan struct{}),
	}
	knobs.EvalKnobs.TestingEvalFilter = func(args kvserverbase.FilterArgs) *roachpb.Error {
		put, ok := args.Req.(*roachpb.PutRequest)
		if !ok {
			return nil
		}
		if !put.Key.Equal(keys.ScratchRangeMin) {
			return nil
		}
		time.Sleep(5 * time.Second)
		blocked.Do(func() {
			close(blocked.ch)
		})
		return roachpb.NewError(errIntercepted)
	}
	var args base.TestClusterArgs
	args.ServerArgs.Knobs.Store = knobs

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	db := tc.Server(0).DB()
	require.NoError(t, tc.Stopper().RunAsyncTask(ctx, "slow-write", func(ctx context.Context) {
		err := db.Put(ctx, keys.ScratchRangeMin, "hello")
		assert.True(t, errors.Is(err, errIntercepted), "%+v", err)
	}))
	select {
	case <-blocked.ch:
	case <-time.After(testutils.DefaultSucceedsSoonDuration):
		t.Fatal("timed out")
	}
	//tr := tc.Server(0).DistSenderI().(*kvcoord.DistSender).Tracer
	tr := tc.Server(0).Stopper().Tracer()
	// tr := tc.Server(0).TracerI().(*tracing.Tracer)
	ctx, finishAndGetRecording := tracing.ContextWithRecordingSpan(ctx, tr, "get")
	defer finishAndGetRecording()
	_, err := db.Get(ctx, keys.ScratchRangeMin)
	require.NoError(t, err)
	rec := finishAndGetRecording()
	t.Log(rec.String())
}
