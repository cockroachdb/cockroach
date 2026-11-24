// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob_test

import (
	"context"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogjob"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// ptsTestRig stands up a test server, creates a real BACKUP job,
// and returns a PTSManagerForTesting bound to that job's PTS
// provider — the minimum scaffolding for exercising install /
// advance against the real protectedts subsystem.
type ptsTestRig struct {
	srv     serverutils.TestServerInterface
	job     *jobs.Job
	mgr     *revlogjob.PTSManagerForTesting
	startTS hlc.Timestamp
}

func newPTSTestRig(t *testing.T, startTS hlc.Timestamp) *ptsTestRig {
	t.Helper()
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	t.Cleanup(func() { srv.Stopper().Stop(ctx) })

	app := srv.SystemLayer()
	internalDB := app.InternalDB().(isql.DB)
	registry := app.JobRegistry().(*jobs.Registry)
	execCfg := app.ExecutorConfig().(sql.ExecutorConfig)

	jobID := registry.MakeJobID()
	rec := jobs.Record{
		Description: "revlog pts test",
		Details:     jobspb.BackupDetails{},
		Progress:    jobspb.BackupProgress{},
		Username:    username.RootUserName(),
	}
	require.NoError(t, internalDB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		_, err := registry.CreateAdoptableJobWithTxn(ctx, rec, jobID, txn)
		return err
	}))
	job, err := registry.LoadJob(ctx, jobID)
	require.NoError(t, err)

	target := ptpb.MakeClusterTarget()
	mgr := revlogjob.NewPTSManagerForTesting(
		job, execCfg.ProtectedTimestampProvider, internalDB, target, startTS,
	)
	return &ptsTestRig{srv: srv, job: job, mgr: mgr, startTS: startTS}
}

// TestPTSManagerInstallAllocatesRecord verifies install allocates a
// fresh PTS record at startHLC and stashes its UUID on the job's
// BackupDetails — the contract that lets the existing BACKUP
// OnFailOrCancel release the record on teardown.
func TestPTSManagerInstallAllocatesRecord(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	startTS := hlc.Timestamp{WallTime: int64(1000 * time.Second)}
	rig := newPTSTestRig(t, startTS)

	require.NoError(t, rig.mgr.Install(ctx))

	id := rig.mgr.RecordIDForTesting()
	require.NotEqual(t, uuid.UUID{}, id, "install must allocate a record UUID")
	require.Equal(t, startTS, rig.mgr.RecordTSForTesting())

	// The job's BackupDetails should now carry the same UUID.
	loaded, err := rig.srv.SystemLayer().JobRegistry().(*jobs.Registry).
		LoadJob(ctx, rig.job.ID())
	require.NoError(t, err)
	details := loaded.Details().(jobspb.BackupDetails)
	require.NotNil(t, details.ProtectedTimestampRecord)
	require.Equal(t, id, *details.ProtectedTimestampRecord)
}

// TestPTSManagerInstallReusesExistingRecord verifies install is
// idempotent: a second install on a job whose details already carry
// a UUID looks up the existing record rather than allocating a new
// one. This is the resume-after-crash invariant — without it we'd
// orphan the prior record.
func TestPTSManagerInstallReusesExistingRecord(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	startTS := hlc.Timestamp{WallTime: int64(1000 * time.Second)}
	rig := newPTSTestRig(t, startTS)

	require.NoError(t, rig.mgr.Install(ctx))
	first := rig.mgr.RecordIDForTesting()

	// Reload the job and build a second manager pointed at the same
	// job. Install should pick up the existing UUID.
	app := rig.srv.SystemLayer()
	registry := app.JobRegistry().(*jobs.Registry)
	job2, err := registry.LoadJob(ctx, rig.job.ID())
	require.NoError(t, err)
	mgr2 := revlogjob.NewPTSManagerForTesting(
		job2, app.ExecutorConfig().(sql.ExecutorConfig).ProtectedTimestampProvider,
		app.InternalDB().(isql.DB),
		ptpb.MakeClusterTarget(), startTS,
	)
	require.NoError(t, mgr2.Install(ctx))
	require.Equal(t, first, mgr2.RecordIDForTesting(),
		"second install must reuse the existing record")
}

// TestPTSManagerAdvanceMovesRecordTimestamp verifies that advance
// updates the record's timestamp when the frontier has moved past
// the threshold, and is a no-op otherwise. The PTS is held back
// from the frontier by ptsTargetLag (10m) and only advances when
// the new candidate is more than ptsAdvanceThreshold (1m) past the
// current timestamp.
func TestPTSManagerAdvanceMovesRecordTimestamp(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Pick a startHLC well past zero so the initial PTS isn't pinned
	// against the wall-clock floor.
	startTS := hlc.Timestamp{WallTime: int64(1 * time.Hour)}
	rig := newPTSTestRig(t, startTS)
	require.NoError(t, rig.mgr.Install(ctx))
	originalTS := rig.mgr.RecordTSForTesting()

	// Frontier just past startHLC: candidate = frontier - 10m, which
	// is BEFORE startHLC. Advance is a no-op.
	rig.mgr.Advance(ctx, startTS.AddDuration(time.Second))
	require.Equal(t, originalTS, rig.mgr.RecordTSForTesting(),
		"frontier within ptsTargetLag of startHLC: PTS must not advance")

	// Frontier far past startHLC: candidate is comfortably past
	// startHLC and exceeds ptsAdvanceThreshold over the current
	// recordTS. Advance should issue an UpdateTimestamp.
	farFrontier := startTS.AddDuration(2 * time.Hour)
	rig.mgr.Advance(ctx, farFrontier)
	advanced := rig.mgr.RecordTSForTesting()
	require.True(t, originalTS.Less(advanced),
		"after advance with far frontier, record ts should have moved (was %s, got %s)",
		originalTS, advanced)
	require.Equal(t, farFrontier.AddDuration(-10*time.Minute), advanced,
		"advanced to candidate = frontier - ptsTargetLag (10m)")
}

// TestPTSManagerAdvanceWithoutInstallIsNoop verifies that calling
// advance before install is harmless rather than panicking — the
// orchestration layer wires the hook before install completes, so
// concurrent frontier advances during the install txn would
// otherwise race.
func TestPTSManagerAdvanceWithoutInstallIsNoop(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	rig := newPTSTestRig(t, hlc.Timestamp{WallTime: int64(time.Hour)})
	rig.mgr.Advance(ctx, hlc.Timestamp{WallTime: int64(2 * time.Hour)})
	require.Equal(t, hlc.Timestamp{}, rig.mgr.RecordTSForTesting(),
		"advance before install must not affect record ts")
}
