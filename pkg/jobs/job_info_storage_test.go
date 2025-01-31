// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobs_test

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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
			v, ok, err = infoStorage.Get(ctx, "getJobInfo", key)
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

	const opTest = "test"

	// Verify we see 4 rows (c, e, f, g) in the prefix.
	require.NoError(t, idb.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := job2.InfoStorage(txn)
		count, err := infoStorage.Count(ctx, kPrefix, kZ)
		if err != nil {
			return err
		}
		require.Equal(t, 4, count)
		_, ok, err := infoStorage.Get(ctx, opTest, kC)
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
		_, ok, err := infoStorage.Get(ctx, opTest, kC)
		if err != nil {
			return err
		}
		require.False(t, ok)
		_, ok, err = infoStorage.Get(ctx, opTest, kF)
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
		val, exists, err := infoStorage.Get(ctx, "test", "foo")
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

	s, conn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	sql := sqlutils.MakeSQLRunner(conn)
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

	before := timeutil.Now()

	t.Run("progress", func(t *testing.T) {
		// Write two fractions updates for j1.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.ProgressStorage().Set(ctx, txn, 0.2, hlc.Timestamp{})
		}))

		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			if err := job1.ProgressStorage().Set(ctx, txn, 0.4, hlc.Timestamp{}); err != nil {
				return err
			}
			// Read our own write back in the same txn.
			got, _, _, err := job1.ProgressStorage().Get(ctx, txn)
			if err != nil {
				return err
			}
			require.Equal(t, 0.4, got)

			return job1.ProgressStorage().Set(ctx, txn, 0.5, hlc.Timestamp{})
		}))

		// Write a ts for for j2.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job2.ProgressStorage().Set(ctx, txn, math.NaN(), hlc.Timestamp{WallTime: 100})
		}))

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
		require.True(t, !before.After(when))
		sql.CheckQueryResults(t, fmt.Sprintf("SELECT fraction from system.job_progress_history where job_id = %d", job1.ID()), [][]string{{"0.5"}, {"0.2"}})

		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			fraction, resolved, when, err = job2.ProgressStorage().Get(ctx, txn)
			return err
		}))

		require.True(t, math.IsNaN(fraction))
		require.Equal(t, int64(100), resolved.WallTime)
		require.True(t, !before.After(when))
	})

	t.Run("status-and-message", func(t *testing.T) {
		// Keep track of how many messages we expect to see for j1 and j2.
		var expJ1Msg, expJ2Msg []jobs.JobMessage

		// Record a message for j1.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.Messages().Record(ctx, txn, "k1", "a")
		}))
		expJ1Msg = append(expJ1Msg, jobs.JobMessage{Kind: "k1", Message: "a"})

		beforeB := timeutil.Now()
		// Update status for j1.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			if err := job1.StatusStorage().Set(ctx, txn, "a"); err != nil {
				return err
			}
			got, _, err := job1.StatusStorage().Get(ctx, txn)
			if err != nil {
				return err
			}
			require.Equal(t, "a", got)
			if err := job1.StatusStorage().Set(ctx, txn, "b"); err != nil {
				return err
			}
			return nil
		}))

		// Even though we set the status twice, we only expect to see the last one,
		// both as the single latest row in the status table but also logged in the
		// message table; the overritten first status is overwritten not just in the
		// status table but the recorded message is overritten too since it has the
		// same timestamp and kind.
		expJ1Msg = append(expJ1Msg, jobs.JobMessage{Kind: "status", Message: "b"})

		// Update status for j2 a couple times.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job2.StatusStorage().Set(ctx, txn, "c")
		}))
		expJ2Msg = append(expJ2Msg, jobs.JobMessage{Kind: "status", Message: "c"})

		beforeD := timeutil.Now()
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job2.StatusStorage().Set(ctx, txn, "d")
		}))
		expJ2Msg = append(expJ2Msg, jobs.JobMessage{Kind: "status", Message: "d"})

		// Now we should see j1 and j2's statuses as the ones we set, and only two
		// rows in the status table -- one per job -- even though we set the status
		// more than twice.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			got, when, err := job1.StatusStorage().Get(ctx, txn)
			if err != nil {
				return err
			}
			require.Equal(t, "b", got)
			require.True(t, !beforeB.After(when), "%s <= %s", beforeB, when)

			got, when, err = job2.StatusStorage().Get(ctx, txn)
			if err != nil {
				return err
			}
			require.Equal(t, "d", got)
			require.True(t, !beforeD.After(when), "%s <= %s", beforeD, when)

			// Verify only one row per job is retained in status.
			row, err := txn.QueryRow(ctx, "test", txn.KV(),
				"select count(*) from system.job_status WHERE job_id IN ($1, $2)", job1.ID(), job2.ID(),
			)
			if err != nil {
				return err
			}
			require.Equal(t, 2, int(*row[0].(*tree.DInt)))
			return nil
		}))

		// Record a couple more messages.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.Messages().Record(ctx, txn, "k2", "b")
		}))
		expJ1Msg = append(expJ1Msg, jobs.JobMessage{Kind: "k2", Message: "b"})

		// Update one of j1's kinds of message to a new message.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.Messages().Record(ctx, txn, "k1", "c")
		}))
		expJ1Msg = append(expJ1Msg, jobs.JobMessage{Kind: "k1", Message: "c"})

		// Write a message for j2.
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job2.Messages().Record(ctx, txn, "k1", "d")
		}))
		expJ2Msg = append(expJ2Msg, jobs.JobMessage{Kind: "k1", Message: "d"})

		var j1Messages, j2Messages []jobs.JobMessage
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			var err error
			j1Messages, err = job1.Messages().Fetch(ctx, txn)
			if err != nil {
				return err
			}
			j2Messages, err = job2.Messages().Fetch(ctx, txn)
			return err
		}))

		// Reverse the order of the expected messages we accumulated them from
		// oldest to newest, but we persist and fetch entries newest-first.
		slices.Reverse(expJ1Msg)
		slices.Reverse(expJ2Msg)

		// Blank the written timestamps so we can compare to our expectation.
		for i := range j1Messages {
			j1Messages[i].Written = time.Time{}
		}
		for i := range j2Messages {
			j2Messages[i].Written = time.Time{}
		}

		require.Equal(t, expJ1Msg, j1Messages)
		require.Equal(t, expJ2Msg, j2Messages)
	})

	t.Run("progress-history-retention", func(t *testing.T) {
		sql.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = 1", "jobs.retained_progress_entries"))
		defer func() {
			sql.Exec(t, fmt.Sprintf("RESET CLUSTER SETTING %s", "jobs.retained_progress_entries"))
		}()
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.ProgressStorage().Set(ctx, txn, 0.8, hlc.Timestamp{})
		}))
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job1.ProgressStorage().Set(ctx, txn, 0.9, hlc.Timestamp{})
		}))
		sql.CheckQueryResults(t, fmt.Sprintf("SELECT fraction from system.job_progress_history where job_id = %d", job1.ID()), [][]string{{"0.9"}})
	})
	t.Run("message-retention", func(t *testing.T) {
		job3 := createJob(3)
		sql.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = 1", "jobs.retained_messages"))
		defer func() {
			sql.Exec(t, fmt.Sprintf("RESET CLUSTER SETTING %s", "jobs.retained_messages"))
		}()
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job3.Messages().Record(ctx, txn, "k1", "foo")
		}))
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job3.Messages().Record(ctx, txn, "k1", "bar")
		}))
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job3.Messages().Record(ctx, txn, "k2", "baz")
		}))
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			return job3.Messages().Record(ctx, txn, "k2", "boo")
		}))
		var j3Messages []jobs.JobMessage
		require.NoError(t, idb.Txn(ctx, func(ct context.Context, txn isql.Txn) error {
			var err error
			j3Messages, err = job3.Messages().Fetch(ctx, txn)
			if err != nil {
				return err
			}
			return nil
		}))
		// Blank the written timestamps so we can compare to our expectation.
		for i := range j3Messages {
			j3Messages[i].Written = time.Time{}
		}
		require.Equal(t, []jobs.JobMessage{{Kind: "k2", Message: "boo"}, {Kind: "k1", Message: "bar"}}, j3Messages)
	})

}
