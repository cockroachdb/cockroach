// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

// jobPersisterTestRig spins up a single test server, creates an
// adoptable job, and returns a jobPersister bound to its ID together
// with the InternalDB the test will read/write through.
type jobPersisterTestRig struct {
	srv       serverutils.TestServerInterface
	persister revlogjob.Persister
	jobID     jobspb.JobID
	db        isql.DB
}

func newJobPersisterTestRig(t *testing.T) *jobPersisterTestRig {
	t.Helper()
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	t.Cleanup(func() { srv.Stopper().Stop(ctx) })

	app := srv.ApplicationLayer()
	internalDB := app.InternalDB().(isql.DB)
	registry := app.JobRegistry().(*jobs.Registry)

	jobID := registry.MakeJobID()
	rec := jobs.Record{
		Description: "revlogjob persister test",
		Details:     jobspb.BackupDetails{},
		Progress:    jobspb.BackupProgress{},
		Username:    username.RootUserName(),
	}
	require.NoError(t, internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, jobID, txn)
		return err
	}))

	return &jobPersisterTestRig{
		srv:       srv,
		persister: revlogjob.NewJobPersisterForTesting(jobID, internalDB),
		jobID:     jobID,
		db:        internalDB,
	}
}

// TestJobPersisterLoadFirstRun verifies the persister returns
// found=false for a freshly-created job that has never been
// checkpointed (Load on a job with no InfoStorage entry).
func TestJobPersisterLoadFirstRun(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rig := newJobPersisterTestRig(t)
	state, found, err := rig.persister.Load(ctx)
	require.NoError(t, err)
	require.False(t, found, "first-run Load must report not-found")
	require.Equal(t, hlc.Timestamp{}, state.HighWater)
	require.Empty(t, state.OpenTicks)
	require.Nil(t, state.Frontier)
}

// TestJobPersisterRoundTrip verifies that Store followed by Load on
// the same persister returns equivalent state — the three storage
// pieces (HighWater, Frontier, OpenTicks) all survive the txn round
// trip.
func TestJobPersisterRoundTrip(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rig := newJobPersisterTestRig(t)

	sp := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}
	frontier, err := span.MakeFrontierAt(hlc.Timestamp{WallTime: 150}, sp)
	require.NoError(t, err)
	defer frontier.Release()

	openTickEnd := hlc.Timestamp{WallTime: 160}
	want := revlogjob.State{
		HighWater: hlc.Timestamp{WallTime: 140},
		Frontier:  frontier,
		OpenTicks: map[hlc.Timestamp][]revlogpb.File{
			openTickEnd: {
				{FileID: 1, FlushOrder: 0},
				{FileID: 2, FlushOrder: 1},
			},
		},
	}
	require.NoError(t, rig.persister.Store(ctx, want))

	got, found, err := rig.persister.Load(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, want.HighWater, got.HighWater)
	require.Equal(t, want.OpenTicks, got.OpenTicks)
	require.NotNil(t, got.Frontier)
	require.Equal(t, want.Frontier.Frontier(), got.Frontier.Frontier())
}

// TestJobPersisterStoreOverwrites verifies a second Store fully
// replaces the prior persisted state — no residual fields leak from
// the earlier checkpoint.
func TestJobPersisterStoreOverwrites(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rig := newJobPersisterTestRig(t)

	sp := roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}

	frontier1, err := span.MakeFrontierAt(hlc.Timestamp{WallTime: 100}, sp)
	require.NoError(t, err)
	defer frontier1.Release()
	require.NoError(t, rig.persister.Store(ctx, revlogjob.State{
		HighWater: hlc.Timestamp{WallTime: 90},
		Frontier:  frontier1,
		OpenTicks: map[hlc.Timestamp][]revlogpb.File{
			{WallTime: 110}: {{FileID: 1, FlushOrder: 0}},
		},
	}))

	frontier2, err := span.MakeFrontierAt(hlc.Timestamp{WallTime: 200}, sp)
	require.NoError(t, err)
	defer frontier2.Release()
	require.NoError(t, rig.persister.Store(ctx, revlogjob.State{
		HighWater: hlc.Timestamp{WallTime: 190},
		Frontier:  frontier2,
		OpenTicks: map[hlc.Timestamp][]revlogpb.File{
			{WallTime: 210}: {{FileID: 7, FlushOrder: 5}},
		},
	}))

	got, found, err := rig.persister.Load(ctx)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, hlc.Timestamp{WallTime: 190}, got.HighWater)
	require.Equal(t, hlc.Timestamp{WallTime: 200}, got.Frontier.Frontier())
	require.Equal(t, map[hlc.Timestamp][]revlogpb.File{
		{WallTime: 210}: {{FileID: 7, FlushOrder: 5}},
	}, got.OpenTicks)
}
