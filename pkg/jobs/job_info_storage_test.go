// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl/backupccl" // import ccl to be able to run backups
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl"    // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestJobInfoAccessors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	idb := s.InternalDB().(isql.DB)
	r := s.JobRegistry().(*jobs.Registry)

	createJob := func(id jobspb.JobID) *jobs.Job {
		defaultRecord := jobs.Record{
			// Job does not accept an empty Details field, so arbitrarily provide
			// ImportDetails.
			Details:  jobspb.BackupDetails{},
			Progress: jobspb.BackupProgress{},
			Username: username.TestUserName(),
		}

		job, err := r.CreateJobWithTxn(ctx, defaultRecord, id, nil /* txn */)
		require.NoError(t, err)
		return job
	}

	job1 := createJob(1)
	job2 := createJob(2)
	job3 := createJob(3)
	kPrefix, kA, kB, kC, kD := "🔑", "🔑A", "🔑B", "🔑C", "🔑D"
	v1, v2, v3 := []byte("val1"), []byte("val2"), []byte("val3")

	// Key doesn't exist yet.
	getJobInfo := func(j *jobs.Job, key string) (v []byte, ok bool, err error) {
		err = idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			infoStorage := j.InfoStorage(txn)
			v, ok, err = infoStorage.Get(ctx, key)
			return err
		})
		return v, ok, err
	}
	_, ok, err := getJobInfo(job1, kA)
	require.NoError(t, err)
	require.False(t, ok)

	// Write kA = v1.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job1.InfoStorage(txn)
		return infoStorage.Write(ctx, kA, v1)
	}))
	// Write kD = v2.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Write(ctx, kD, v2)
	}))

	// Check that key is now found with value v1.
	v, ok, err := getJobInfo(job1, kA)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, v1, v)

	// Overwrite kA = v2.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job1.InfoStorage(txn)
		return infoStorage.Write(ctx, kA, v2)
	}))

	// Check that key is now v1.
	v, ok, err = getJobInfo(job1, kA)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, v2, v)

	// Verify a different is not found.
	_, ok, err = getJobInfo(job1, kB)
	require.NoError(t, err)
	require.False(t, ok)

	// Verify that the same key for a different job is not found.
	_, ok, err = getJobInfo(job2, kB)
	require.NoError(t, err)
	require.False(t, ok)

	// Write and revise some info keys a, b and c (out of order, just for fun).
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Write(ctx, kB, v2)
	}))
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Write(ctx, kA, v1)
	}))
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Write(ctx, kC, v2)
	}))
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Write(ctx, kA, v2)
	}))
	// Also delete the info key d.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Delete(ctx, kD)
	}))

	// Iterate the common prefix of a, b, c and d.
	var i int
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Iterate(ctx, kPrefix, func(key string, value []byte) error {
			i++
			switch i {
			case 1:
				require.Equal(t, key, kA)
			case 2:
				require.Equal(t, key, kB)
			case 3:
				require.Equal(t, key, kC)
			}
			require.Equal(t, v2, value)
			return nil
		})
	}))
	require.Equal(t, 3, i)

	// Add a new revision to kC.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Write(ctx, kC, v3)
	}))
	i = 0
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.GetLast(ctx, kPrefix, func(key string, value []byte) error {
			i++
			require.Equal(t, key, kC)
			require.Equal(t, v3, value)
			return nil
		})
	}))
	require.Equal(t, 1, i)

	// Iterate the specific prefix of just a.
	found := false
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Iterate(ctx, kA, func(key string, value []byte) error {
			require.Equal(t, kA, key)
			require.Equal(t, v2, value)
			found = true
			return nil
		})
	}))
	require.True(t, found)

	// Delete kA-kB.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.DeleteRange(ctx, kA, kC)
	}))
	// Verify only kC remains.
	i = 0
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.Iterate(ctx, kPrefix, func(key string, value []byte) error {
			i++
			require.Equal(t, key, kC)
			return nil
		})
	}))
	require.Equal(t, 1, i)

	// Iterate a different job.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job3.InfoStorage(txn)
		return infoStorage.Iterate(ctx, kPrefix, func(key string, value []byte) error {
			t.Fatalf("unexpected record for job 3: %v = %v", key, value)
			return nil
		})
	}))
}

func TestAccessorsWithWrongSQLLivenessSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			// Avoiding jobs to be adopted.
			JobsTestingKnobs: &jobs.TestingKnobs{
				DisableAdoptions: true,
			},
			// DisableAdoptions needs this.
			UpgradeManager: &upgradebase.TestingKnobs{
				DontUseJobs:                       true,
				SkipJobMetricsPollingJobBootstrap: true,
			},
			KeyVisualizer: &keyvisualizer.TestingKnobs{
				SkipJobBootstrap: true,
			},
		},
	}

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, args)
	defer s.Stopper().Stop(ctx)
	ief := s.InternalDB().(isql.DB)

	registry := s.JobRegistry().(*jobs.Registry)

	defaultRecord := jobs.Record{
		// Job does not accept an empty Details field, so arbitrarily provide
		// ImportDetails.
		Details:  jobspb.ImportDetails{},
		Progress: jobspb.ImportProgress{},
		Username: username.TestUserName(),
	}

	job, err := registry.CreateJobWithTxn(ctx, defaultRecord, registry.MakeJobID(), nil /* txn */)
	require.NoError(t, err)
	require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job.InfoStorage(txn)
		return infoStorage.Write(ctx, "foo", []byte("baz"))
	}))

	require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Change job's session id and check that writes are rejected.
		updateClaimStmt := `UPDATE system.jobs SET claim_session_id = $1 WHERE id = $2`
		_, err := txn.ExecEx(ctx, "update-claim", txn.KV(), sessiondata.NodeUserSessionDataOverride,
			updateClaimStmt, "!@#!@$!$@#", job.ID())
		return err
	}))

	err = ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job.InfoStorage(txn)
		return infoStorage.Write(ctx, "foo", []byte("bar"))
	})
	require.True(t, testutils.IsError(err, "expected session.*but found"))

	// A Get should still succeed even with an invalid session id.
	require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job.InfoStorage(txn)
		val, exists, err := infoStorage.Get(ctx, "foo")
		if err != nil {
			return err
		}
		require.True(t, exists)
		require.Equal(t, val, []byte("baz"))
		return nil
	}))

	// Iterate should still succeed even with an invalid session id.
	require.NoError(t, ief.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job.InfoStorage(txn)
		return infoStorage.Iterate(ctx, "foo", func(infoKey string, value []byte) error {
			require.Equal(t, value, []byte("baz"))
			return nil
		})
	}))
}
