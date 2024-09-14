// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs_test

import (
	"context"
	"math"
	"testing"
	"time"

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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	kPrefix, kA, kB, kC, kD, kE, kF, kG, kZ := "🔑", "🔑A", "🔑B", "🔑C", "🔑D", "🔑E", "🔑F", "🔑G", "🔑Z"
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

	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		count, err := infoStorage.Count(ctx, kPrefix, kZ)
		require.Equal(t, 3, count)
		return err
	}))

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
		return infoStorage.DeleteRange(ctx, kA, kC, 0)
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
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		count, err := infoStorage.Count(ctx, kPrefix, kZ)
		require.Equal(t, 1, count)
		return err
	}))

	// Write kE, kF, kG.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		for _, k := range []string{kE, kF, kG} {
			if err := infoStorage.Write(ctx, k, v2); err != nil {
				return err
			}
		}
		return nil
	}))

	// Verify we see 4 rows (c, e, f, g) in the prefix.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		count, err := infoStorage.Count(ctx, kPrefix, kZ)
		if err != nil {
			return err
		}
		require.Equal(t, 4, count)
		_, ok, err := infoStorage.Get(ctx, kC)
		if err != nil {
			return err
		}
		require.True(t, ok)
		return nil
	}))

	// Delete [k, kZ) but with a limit of 2 so just kC and kE.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		return infoStorage.DeleteRange(ctx, kC, kZ, 2)
	}))

	// Verify we see 2 rows (F, G) in the prefix and C and E are missing.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		count, err := infoStorage.Count(ctx, kPrefix, kZ)
		if err != nil {
			return err
		}
		require.Equal(t, 2, count)
		_, ok, err := infoStorage.Get(ctx, kC)
		if err != nil {
			return err
		}
		require.False(t, ok)
		_, ok, err = infoStorage.Get(ctx, kF)
		if err != nil {
			return err
		}
		require.True(t, ok)
		return nil
	}))

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

func TestJobProgressAndStatusAccessors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)

	idb := s.InternalDB().(isql.DB)
	r := s.JobRegistry().(*jobs.Registry)

	createJob := func(id jobspb.JobID) *jobs.Job {
		job, err := r.CreateJobWithTxn(ctx, jobs.Record{Details: jobspb.BackupDetails{}, Progress: jobspb.BackupProgress{}, Username: username.TestUserName()}, id, nil)
		require.NoError(t, err)
		return job
	}

	job1 := createJob(1)
	job2 := createJob(2)

	before := s.Clock().Now().GoTime()

	t.Run("status", func(t *testing.T) {
		// Write two kinds of status for j1.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.StatusStorage().Write(ctx, txn, "kind1", "one=a")
		}))
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.StatusStorage().Write(ctx, txn, "kind2", "two=x")
		}))
		// Update one of j1's kinds of status to a new message.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.StatusStorage().Write(ctx, txn, "kind1", "one=b")
		}))
		// Write a status for j2.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job2.StatusStorage().Write(ctx, txn, "kind1", "one=aa")
		}))

		after := s.Clock().Now().GoTime()

		var (
			msg  string
			kind jobs.JobStatusKind
			when time.Time
			err  error
		)

		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			msg, kind, when, err = job1.StatusStorage().Latest(ctx, txn, "kind1")
			return err
		}))
		require.Equal(t, jobs.JobStatusKind("kind1"), kind)
		require.Equal(t, "one=b", msg)
		require.True(t, before.Before(when) && after.After(when), "%s < %s < %s", before, when, after)

		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			msg, kind, when, err = job1.StatusStorage().Latest(ctx, txn, "kind2")
			return err
		}))
		require.Equal(t, jobs.JobStatusKind("kind2"), kind)
		require.Equal(t, "two=x", msg)
		require.True(t, before.Before(when) && after.After(when), "%s < %s < %s", before, when, after)

		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			msg, kind, when, err = job2.StatusStorage().Latest(ctx, txn, "kind1")
			return err
		}))
		require.Equal(t, jobs.JobStatusKind("kind1"), kind)
		require.Equal(t, "one=aa", msg)
		require.True(t, before.Before(when) && after.After(when), "%s < %s < %s", before, when, after)
	})

	t.Run("progress", func(t *testing.T) {
		// Write two fractions updates for j1.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.ProgressStorage().Write(ctx, txn, 0.2, hlc.Timestamp{})
		}))
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.ProgressStorage().Write(ctx, txn, 0.5, hlc.Timestamp{})
		}))
		// Write a ts for for j2.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job2.ProgressStorage().Write(ctx, txn, math.NaN(), hlc.Timestamp{WallTime: 100})
		}))

		after := s.Clock().Now().GoTime()

		var (
			fraction float64
			resolved hlc.Timestamp
			when     time.Time
			err      error
		)

		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			fraction, resolved, when, err = job1.ProgressStorage().Get(ctx, txn)
			return err
		}))
		require.Equal(t, 0.5, fraction)
		require.True(t, resolved.IsEmpty())
		require.True(t, before.Before(when) && after.After(when), "%s < %s < %s", before, when, after)

		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			fraction, resolved, when, err = job2.ProgressStorage().Get(ctx, txn)
			return err
		}))

		require.True(t, math.IsNaN(fraction))
		require.Equal(t, int64(100), resolved.WallTime)
		require.True(t, before.Before(when) && after.After(when), "%s < %s < %s", before, when, after)
	})

}
