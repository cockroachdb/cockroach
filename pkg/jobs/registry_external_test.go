// Copyright 2017 The Cockroach Authors.
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
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"regexp"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestRoundtripJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	registry := s.JobRegistry().(*jobs.Registry)
	defer s.Stopper().Stop(ctx)

	jobID := registry.MakeJobID()
	record := jobs.Record{
		Description:   "beep boop",
		Username:      security.MakeSQLUsernameFromPreNormalizedString("robot"),
		DescriptorIDs: descpb.IDs{42},
		Details:       jobspb.RestoreDetails{},
		Progress:      jobspb.RestoreProgress{},
	}
	storedJob, err := registry.CreateAdoptableJobWithTxn(ctx, record, jobID, nil /* txn */)
	require.NoError(t, err)
	retrievedJob, err := registry.LoadJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if e, a := storedJob, retrievedJob; !reflect.DeepEqual(e, a) {
		//diff := strings.Join(pretty.Diff(e, a), "\n")
		t.Fatalf("stored job did not match retrieved job:\n%+v\n%+v", e, a)
	}
}

// TestExpiringSessionsAndClaimJobsDoesNotTouchTerminalJobs will ensure that we do not
// update the claim_session_id field of jobs when expiring sessions or claiming
// jobs.
func TestExpiringSessionsAndClaimJobsDoesNotTouchTerminalJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Don't adopt, cancel rapidly.
	adopt := 10 * time.Hour
	cancel := 10 * time.Millisecond
	args := base.TestServerArgs{Knobs: base.TestingKnobs{
		JobsTestingKnobs: jobs.NewTestingKnobsWithIntervals(adopt, cancel),
	}}

	ctx := context.Background()
	s, sqlDB, _ := serverutils.StartServer(t, args)
	defer s.Stopper().Stop(ctx)

	payload, err := protoutil.Marshal(&jobspb.Payload{
		Details: jobspb.WrapPayloadDetails(jobspb.BackupDetails{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	progress, err := protoutil.Marshal(&jobspb.Progress{
		Details: jobspb.WrapProgressDetails(jobspb.BackupProgress{}),
	})
	if err != nil {
		t.Fatal(err)
	}

	tdb := sqlutils.MakeSQLRunner(sqlDB)
	const insertQuery = `
   INSERT
     INTO system.jobs (
                        status,
                        payload,
                        progress,
                        claim_session_id,
                        claim_instance_id
                      )
   VALUES ($1, $2, $3, $4, $5)
RETURNING id;
`
	// Disallow clean up of claimed jobs
	jobs.CancellationsUpdateLimitSetting.Override(ctx, &s.ClusterSettings().SV, 0)
	terminalStatuses := []jobs.Status{jobs.StatusSucceeded, jobs.StatusCanceled, jobs.StatusFailed}
	terminalIDs := make([]jobspb.JobID, len(terminalStatuses))
	terminalClaims := make([][]byte, len(terminalStatuses))
	for i, s := range terminalStatuses {
		terminalClaims[i] = uuid.MakeV4().GetBytes() // bogus claim
		tdb.QueryRow(t, insertQuery, s, payload, progress, terminalClaims[i], 42).
			Scan(&terminalIDs[i])
	}
	var nonTerminalID jobspb.JobID
	tdb.QueryRow(t, insertQuery, jobs.StatusRunning, payload, progress, uuid.MakeV4().GetBytes(), 42).
		Scan(&nonTerminalID)

	checkClaimEqual := func(id jobspb.JobID, exp []byte) error {
		const getClaimQuery = `SELECT claim_session_id FROM system.jobs WHERE id = $1`
		var claim []byte
		tdb.QueryRow(t, getClaimQuery, id).Scan(&claim)
		if !bytes.Equal(claim, exp) {
			return errors.Errorf("expected nil, got %s", hex.EncodeToString(exp))
		}
		return nil
	}

	getClaimCount := func(id jobspb.JobID) int {
		const getClaimQuery = `SELECT count(claim_session_id) FROM system.jobs WHERE id = $1`
		count := 0
		tdb.QueryRow(t, getClaimQuery, id).Scan(&count)
		return count
	}
	// Validate the claims were not cleaned up.
	claimCount := getClaimCount(nonTerminalID)
	if claimCount == 0 {
		require.FailNowf(t, "unexpected claim sessions",
			"claim session ID's were removed some how %d", claimCount)
	}
	// Allow clean up of claimed jobs
	jobs.CancellationsUpdateLimitSetting.Override(ctx, &s.ClusterSettings().SV, 1000)
	testutils.SucceedsSoon(t, func() error {
		return checkClaimEqual(nonTerminalID, nil)
	})
	for i, id := range terminalIDs {
		require.NoError(t, checkClaimEqual(id, terminalClaims[i]))
	}
	// Update the terminal jobs to set them to have a NULL claim.
	for _, id := range terminalIDs {
		tdb.Exec(t, `UPDATE system.jobs SET claim_session_id = NULL WHERE id = $1`, id)
	}
	// At this point, all of the jobs should have a NULL claim.
	// Assert that.
	for _, id := range append(terminalIDs, nonTerminalID) {
		require.NoError(t, checkClaimEqual(id, nil))
	}

	// Nudge the adoption queue and ensure that only the non-terminal job gets
	// claimed.
	s.JobRegistry().(*jobs.Registry).TestingNudgeAdoptionQueue()

	sess, err := s.SQLLivenessProvider().(sqlliveness.Provider).Session(ctx)
	require.NoError(t, err)
	testutils.SucceedsSoon(t, func() error {
		return checkClaimEqual(nonTerminalID, sess.ID().UnsafeBytes())
	})
	// Ensure that the terminal jobs still have a nil claim.
	for _, id := range terminalIDs {
		require.NoError(t, checkClaimEqual(id, nil))
	}
}

// TestRegistrySettingUpdate checks whether the cluster settings are effective
// and properly propagated through the SQL interface. The cluster settings
// change the frequency of adopt, cancel, and gc jobs run by the registry.
func TestRegistrySettingUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// The SQL queries to match to identify the type of job that is running.
	adoptQuery := jobs.AdoptQuery
	cancelQuery := jobs.CancelQuery
	gcQuery := jobs.GcQuery

	// Key strings of the cluster settings to test.
	adoptSettingKey := jobs.AdoptIntervalSettingKey
	cancelSettingKey := jobs.CancelIntervalSettingKey
	gcSettingKey := jobs.GcIntervalSettingKey
	retentionTimeSettingKey := jobs.RetentionTimeSettingKey
	baseSettingKey := jobs.IntervalBaseSettingKey

	// Default interval at the beginning of each test. The duration should be long
	// to ensure that no jobs are run in the initial phase of the tests.
	const defaultDuration = time.Hour
	// Interval to use when testing the value to go from a shorter to a longer duration.
	const longDuration = "500ms"
	// Interval to use when testing the value to go from a longer to a shorter duration.
	const shortDuration = "5ms"
	// Number of job runs to expect when the interval is set to longDuration
	const lessThan = 3
	// Number of job runs to expect when the interval is set to shortDuration.
	const moreThan = 20
	// Base multiplier to convert defaultDuration into 500ms.
	var largeValueFloat = 500.0 / float64(defaultDuration.Milliseconds())
	// Base multiplier to convert defaultDuration into 10ms: 10ms / (60min * 60sec * 1000millis).
	var smallValueFloat = 10.0 / float64(defaultDuration.Milliseconds())

	// Returns cluster settings that overrides the given setting to a long
	// defaultDuration so that the cluster setting can be tested by reducing the
	// intervals.
	clusterSettings := func(ctx context.Context, setting *settings.DurationSetting) *cluster.Settings {
		s := cluster.MakeTestingClusterSettings()
		setting.Override(ctx, &s.SV, defaultDuration)
		return s
	}

	for _, test := range [...]struct {
		name          string // Test case ID
		setting       string // Cluster setting key
		shortInterval string // Duration when expecting a large number of job runs
		longInterval  string // Duration when expecting a small number of job runs
		matchStmt     string // SQL statement to match to identify the target job
		initCount     int    // Initial number of jobs to ignore at the beginning of the test
		toOverride    *settings.DurationSetting
	}{
		{
			name:          "adopt setting",
			setting:       adoptSettingKey,
			shortInterval: shortDuration,
			longInterval:  longDuration,
			matchStmt:     adoptQuery,
			initCount:     0,
			toOverride:    jobs.AdoptIntervalSetting,
		},
		{
			name:          "adopt setting with base",
			setting:       baseSettingKey,
			shortInterval: fmt.Sprintf("%f", smallValueFloat),
			longInterval:  fmt.Sprintf("%f", largeValueFloat),
			matchStmt:     adoptQuery,
			initCount:     0,
			toOverride:    jobs.AdoptIntervalSetting,
		},
		{
			name:          "cancel setting",
			setting:       cancelSettingKey,
			shortInterval: shortDuration,
			longInterval:  longDuration,
			matchStmt:     cancelQuery,
			initCount:     1, // 1 because a cancelLoopTask is run before the job loop.
			toOverride:    jobs.CancelIntervalSetting,
		},
		{
			name:          "cancel setting with base",
			setting:       baseSettingKey,
			shortInterval: fmt.Sprintf("%f", smallValueFloat),
			longInterval:  fmt.Sprintf("%f", largeValueFloat),
			matchStmt:     cancelQuery,
			initCount:     1, // 1 because a cancelLoopTask is run before the job loop.
			toOverride:    jobs.CancelIntervalSetting,
		},
		{
			name:          "gc setting",
			setting:       gcSettingKey,
			shortInterval: shortDuration,
			longInterval:  longDuration,
			matchStmt:     gcQuery,
			initCount:     0,
			toOverride:    jobs.GcIntervalSetting,
		},
		{
			name:          "gc setting with base",
			setting:       baseSettingKey,
			shortInterval: fmt.Sprintf("%f", smallValueFloat),
			longInterval:  fmt.Sprintf("%f", largeValueFloat),
			matchStmt:     gcQuery,
			initCount:     0,
			toOverride:    jobs.GcIntervalSetting,
		},
		{
			name:          "retention-time setting",
			setting:       retentionTimeSettingKey,
			shortInterval: shortDuration,
			longInterval:  longDuration,
			matchStmt:     gcQuery,
			initCount:     0,
			toOverride:    jobs.RetentionTimeSetting,
		},
		{
			name:          "retention-time setting with base",
			setting:       baseSettingKey,
			shortInterval: fmt.Sprintf("%f", smallValueFloat),
			longInterval:  fmt.Sprintf("%f", largeValueFloat),
			matchStmt:     gcQuery,
			initCount:     0,
			toOverride:    jobs.RetentionTimeSetting,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			// Replace multiple white spaces with a single space, remove the last ';', and
			// trim leading and trailing spaces.
			matchStmt := strings.TrimSpace(regexp.MustCompile(`(\s+|;+)`).ReplaceAllString(test.matchStmt, " "))
			var seen int32
			seen = 0
			stmtFilter := func(ctxt context.Context, stmt string, err error) {
				if err != nil {
					return
				}
				if stmt == matchStmt {
					atomic.AddInt32(&seen, 1)
				}
			}

			// Override the setting to be tested and set the value to a long duration.
			// We do so to observe rapid increase in job runs in response to updating
			// the job interval to a short duration.
			cs := clusterSettings(ctx, test.toOverride)
			args := base.TestServerArgs{
				Settings: cs,
				Knobs:    base.TestingKnobs{SQLExecutor: &sql.ExecutorTestingKnobs{StatementFilter: stmtFilter}},
			}
			s, sdb, _ := serverutils.StartServer(t, args)
			defer s.Stopper().Stop(ctx)
			tdb := sqlutils.MakeSQLRunner(sdb)

			// Wait for the initial job runs to finish.
			testutils.SucceedsSoon(t, func() error {
				counted := int(atomic.LoadInt32(&seen))
				if counted == test.initCount {
					return nil
				}
				return errors.Errorf("%s: expected at least %d calls at the beginning, counted %d",
					test.name, test.initCount, counted)
			})

			// Expect no jobs to run after a short duration to ensure that the
			// long interval times are in effect.
			atomic.StoreInt32(&seen, 0)
			dur, _ := time.ParseDuration(test.shortInterval)
			time.Sleep(2 * dur)
			counted := int(atomic.LoadInt32(&seen))
			require.Equalf(t, 0, counted,
				"expected no jobs after a short duration in the beginning, found %d", counted)

			// Reduce the interval and expect a larger number of job runs in a few
			// seconds.
			tdb.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", test.setting, test.shortInterval))
			atomic.StoreInt32(&seen, 0)
			testutils.SucceedsSoon(t, func() error {
				counted = int(atomic.LoadInt32(&seen))
				if counted >= moreThan {
					return nil
				}
				return errors.Errorf("expected at least %d calls, counted %d",
					moreThan, counted)
			})

			// Increase the interval again to a moderately large value and expect a
			// few jobs to run.
			tdb.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", test.setting, test.longInterval))
			atomic.StoreInt32(&seen, 0)
			time.Sleep(time.Second)
			counted = int(atomic.LoadInt32(&seen))
			require.GreaterOrEqualf(t, lessThan, counted,
				"expected less than %d jobs when the interval is increased, found %d", lessThan, counted)
		})
	}
}
