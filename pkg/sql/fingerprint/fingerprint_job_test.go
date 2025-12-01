// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fingerprint

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestPersist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	registry := s.ApplicationLayer().JobRegistry().(*jobs.Registry)

	// Create a fingerprint job to get a valid job ID.
	record := jobs.Record{
		Details:  jobspb.FingerprintDetails{},
		Progress: jobspb.FingerprintProgress{},
		Username: username.TestUserName(),
	}
	job, err := registry.CreateJobWithTxn(ctx, record, registry.MakeJobID(), nil)
	require.NoError(t, err)

	// Create persist with the actual ExecCfg wrapped in FakeJobExecContext.
	execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	p := &persist{
		id:      job.ID(),
		execCtx: &sql.FakeJobExecContext{ExecutorConfig: &execCfg},
	}

	t.Run("emptyLoad", func(t *testing.T) {
		state, found, err := p.load(ctx)
		require.NoError(t, err)
		require.False(t, found)
		require.Equal(t, uint64(0), state.fingerprint)
	})

	t.Run("storeAndLoad", func(t *testing.T) {
		spans := []roachpb.Span{
			{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
			{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
		}
		doneFrontier, err := span.MakeFrontier(spans...)
		require.NoError(t, err)
		// Forward all spans to non-empty timestamps so the frontier is considered non-empty.
		_, _ = doneFrontier.Forward(spans[0], hlc.Timestamp{WallTime: 1})
		_, _ = doneFrontier.Forward(spans[1], hlc.Timestamp{WallTime: 1})

		state := checkpointState{
			fingerprint: 12345,
			done:        doneFrontier,
		}

		err = p.store(ctx, state, 0.5)
		require.NoError(t, err)

		loaded, found, err := p.load(ctx)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, uint64(12345), loaded.fingerprint)
		// Frontiers are stored and loaded.
		require.NotNil(t, loaded.done)
	})

	t.Run("overwrite", func(t *testing.T) {
		spans := []roachpb.Span{{Key: roachpb.Key("x"), EndKey: roachpb.Key("z")}}
		doneFrontier, err := span.MakeFrontier(spans...)
		require.NoError(t, err)

		state := checkpointState{
			fingerprint: 99999,
			done:        doneFrontier,
		}
		err = p.store(ctx, state, 1.0)
		require.NoError(t, err)

		loaded, found, err := p.load(ctx)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, uint64(99999), loaded.fingerprint)
	})
}

func TestKVFingerprinter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Create a table with some data.
	runner.Exec(t, `CREATE TABLE test_fp (id INT PRIMARY KEY, val STRING)`)
	runner.Exec(t, `INSERT INTO test_fp VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	// Get the table's span.
	var tableID uint32
	runner.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'test_fp'`).Scan(&tableID)

	// Use actual key encoding for the table.
	tableSpan := roachpb.Span{
		Key:    s.Codec().TablePrefix(tableID),
		EndKey: s.Codec().TablePrefix(tableID).PrefixEnd(),
	}

	fp := kvFingerprinter{
		sender:   s.DB().NonTransactionalSender(),
		asOf:     s.Clock().Now(),
		stripped: true,
	}

	result, err := fp.fingerprintSpan(ctx, tableSpan)
	require.NoError(t, err)

	require.Equal(t, uint64(0x2249712bb6b0388a), result.fingerprint)
}

// TestJobE2E tests that a fingerprint job can be created as adoptable and
// successfully runs through the job system.
func TestJobE2E(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	runner := sqlutils.MakeSQLRunner(db)

	// Create a table with some data to fingerprint.
	runner.Exec(t, `CREATE TABLE test_job (id INT PRIMARY KEY, val STRING)`)
	runner.Exec(t, `INSERT INTO test_job VALUES (1, 'a'), (2, 'b'), (3, 'c')`)

	// Get the table's span.
	var tableID uint32
	runner.QueryRow(t, `SELECT id FROM system.namespace WHERE name = 'test_job'`).Scan(&tableID)

	tableSpan := roachpb.Span{
		Key:    s.Codec().TablePrefix(tableID),
		EndKey: s.Codec().TablePrefix(tableID).PrefixEnd(),
	}

	registry := s.ApplicationLayer().JobRegistry().(*jobs.Registry)

	// Create an adoptable fingerprint job.
	record := jobs.Record{
		Details: jobspb.FingerprintDetails{
			Spans:    []roachpb.Span{tableSpan},
			AsOf:     s.Clock().Now(),
			Start:    hlc.Timestamp{},
			Stripped: false,
		},
		Progress: jobspb.FingerprintProgress{},
		Username: username.TestUserName(),
	}

	job, err := registry.CreateAdoptableJobWithTxn(ctx, record, registry.MakeJobID(), nil /* txn */)
	require.NoError(t, err)

	// Wait for the job to be adopted and succeed.
	testutils.SucceedsSoon(t, func() error {
		// Nudge the adoption queue to pick up the job immediately.
		registry.TestingNudgeAdoptionQueue()

		var status string
		runner.QueryRow(t, `SELECT status FROM system.jobs WHERE id = $1`, job.ID()).Scan(&status)
		if status == string(jobs.StateSucceeded) {
			return nil
		}
		if status == string(jobs.StateFailed) || status == string(jobs.StateCanceled) {
			return errors.Errorf("job failed with status %s", status)
		}
		return errors.Errorf("job not yet succeeded, current status: %s", status)
	})

	// Verify that the job completed and has a fingerprint stored.
	execCfg := s.ApplicationLayer().ExecutorConfig().(sql.ExecutorConfig)
	p := &persist{
		id:      job.ID(),
		execCtx: &sql.FakeJobExecContext{ExecutorConfig: &execCfg},
	}

	state, found, err := p.load(ctx)
	require.NoError(t, err)
	require.True(t, found, "fingerprint checkpoint should exist after job completion")
	require.NotEqual(t, uint64(0), state.fingerprint, "fingerprint should be non-zero")
}
