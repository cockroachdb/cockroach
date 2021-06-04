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

// TestExpiringSessionsDoesNotTouchTerminalJobs will ensure that we do not
// update the claim_session_id field of jobs when expiring sessions or claiming
// jobs.
func TestExpiringSessionsAndClaimJobsDoesNotTouchTerminalJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Don't adopt, cancel rapidly.
	adoptInterval := 10 * time.Hour
	cancelInterval := 10 * time.Millisecond
	args := base.TestServerArgs{
		Knobs: base.TestingKnobs{JobsTestingKnobs: &jobs.TestingKnobs{
			AdoptIntervalOverride:  &adoptInterval,
			CancelIntervalOverride: &cancelInterval,
		}},
	}

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

func TestRegistrySettingUpdate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	adoptQuery := jobs.AdoptQuery()
	cancelQuery := jobs.CancelQuery()
	gcQuery := jobs.GcQuery()

	adoptSettingKey := jobs.AdoptIntervalSettingKey()
	cancelSettingKey := jobs.CancelIntervalSettingKey()
	gcSettingKey := jobs.GcIntervalSettingKey()
	baseSettingKey := jobs.IntervalBaseSettingKey()

	for _, test := range [...]struct {
		name      string
		setting   string
		value     string
		matchStmt string
		toCount   int32
	}{
		{
			name:      "adopt setting",
			setting:   adoptSettingKey,
			value:     "10ms",
			matchStmt: adoptQuery,
			toCount:   10,
		},
		{
			name:      "adopt setting with base",
			setting:   baseSettingKey,
			value:     "0.000002", //To convert an hour to a smaller value
			matchStmt: adoptQuery,
			toCount:   10,
		},
		{
			name:      "cancel setting",
			setting:   cancelSettingKey,
			value:     "10ms",
			matchStmt: cancelQuery,
			toCount:   10,
		},
		{
			name:      "cancel setting with base",
			setting:   baseSettingKey,
			value:     "0.000002", //To convert an hour to a smaller value
			matchStmt: cancelQuery,
			toCount:   10,
		},
		{
			name:      "gc setting",
			setting:   gcSettingKey,
			value:     "10ms",
			matchStmt: gcQuery,
			toCount:   10,
		},
		{
			name:      "gc setting with base",
			setting:   baseSettingKey,
			value:     "0.000002", //To convert an hour to a smaller value
			matchStmt: gcQuery,
			toCount:   10,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			// Replace multiple white spaces with a single space, remove the last ';', and
			// trim leading and trailing spaces
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

			cs := cluster.MakeTestingClusterSettings()
			jobs.AdoptIntervalSetting().Override(ctx, &cs.SV, time.Hour)
			jobs.CancelIntervalSetting().Override(ctx, &cs.SV, time.Hour)
			jobs.GcIntervalSetting().Override(ctx, &cs.SV, time.Hour)
			jobs.IntervalBaseSetting().Override(ctx, &cs.SV, 1.0)
			args := base.TestServerArgs{
				Settings: cs,
				Knobs: base.TestingKnobs{
					SQLExecutor: &sql.ExecutorTestingKnobs{
						StatementFilter: stmtFilter,
					},
				},
			}
			s, sdb, _ := serverutils.StartServer(t, args)
			defer s.Stopper().Stop(ctx)

			tdb := sqlutils.MakeSQLRunner(sdb)
			tdb.Exec(t, fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", test.setting, test.value))

			atomic.StoreInt32(&seen, 0)
			testutils.SucceedsSoon(t, func() error {
				counted := atomic.LoadInt32(&seen)
				if counted >= test.toCount {
					return nil
				}
				return errors.Errorf("Test %s: expected at least %d calls, counted %d",
					test, test.toCount, counted)
			})
		})
	}
}
