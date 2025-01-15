// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

func BenchmarkJobs(b *testing.B) { reg.Run(b) }
func init() {
	payloadBytes, err := protoutil.Marshal(&jobspb.Payload{
		Details:       jobspb.WrapPayloadDetails(jobspb.ImportDetails{}),
		UsernameProto: username.RootUserName().EncodeProto(),
	})
	if err != nil {
		panic(err)
	}

	progressBytes, err := protoutil.Marshal(&jobspb.Progress{
		Details:  jobspb.WrapProgressDetails(jobspb.ImportProgress{}),
		Progress: &jobspb.Progress_FractionCompleted{FractionCompleted: 1},
	})
	if err != nil {
		panic(err)
	}

	setupQueries := []string{
		fmt.Sprintf("INSERT INTO system.job_info(job_id, info_key, value) (SELECT id, 'legacy_progress', '\\x%s' FROM generate_series(1000, 3000) as id)",
			hex.EncodeToString(progressBytes)),

		fmt.Sprintf("INSERT INTO system.job_info(job_id, info_key, value) (SELECT id, 'legacy_payload', '\\x%s' FROM generate_series(1000, 3000) as id)",
			hex.EncodeToString(payloadBytes)),
		"INSERT INTO system.jobs(id, status, created, job_type, owner) (SELECT id, 'succeeded', now(), 'IMPORT', 'test' FROM generate_series(1000, 3000) as id)",

		// Job 3001 is a RUNNING job. We've marked it as
		// claimed and added run stats that likely prevent it
		// from being meaninfully used during the duration of
		// the test.
		fmt.Sprintf("INSERT INTO system.job_info(job_id, info_key, value) VALUES (3001, 'legacy_progress', '\\x%s')", hex.EncodeToString(progressBytes)),
		fmt.Sprintf("INSERT INTO system.job_info(job_id, info_key, value) VALUES (3001, 'legacy_payload', '\\x%s')", hex.EncodeToString(payloadBytes)),
		`INSERT INTO system.jobs(id, status, created, last_run, num_runs, job_type, owner, claim_instance_id, claim_session_id) VALUES (3001, 'running', now(), now(), 200, 'IMPORT', 'root',
(SELECT id FROM system.sql_instances WHERE session_id IS NOT NULL ORDER BY id LIMIT 1),
(SELECT session_id FROM system.sql_instances WHERE session_id IS NOT NULL ORDER BY id LIMIT 1))`,

		// Job 3002 is a PAUSED job.
		fmt.Sprintf("INSERT INTO system.job_info(job_id, info_key, value) VALUES (3002, 'legacy_progress', '\\x%s')", hex.EncodeToString(progressBytes)),
		fmt.Sprintf("INSERT INTO system.job_info(job_id, info_key, value) VALUES (3002, 'legacy_payload', '\\x%s')", hex.EncodeToString(payloadBytes)),
		`INSERT INTO system.jobs(id, status, created, last_run, num_runs, job_type, owner, claim_instance_id, claim_session_id) VALUES (3002, 'paused', now(), now(), 200, 'IMPORT', 'root',
(SELECT id FROM system.sql_instances WHERE session_id IS NOT NULL ORDER BY id LIMIT 1),
(SELECT session_id FROM system.sql_instances WHERE session_id IS NOT NULL ORDER BY id LIMIT 1))`,
		`ANALYZE system.jobs`,
		`ANALYZE system.job_info`,
	}

	cleanupQuery := "DELETE FROM system.jobs WHERE id >= 1000 AND id <= 4000; DELETE FROM system.job_info WHERE job_id >= 1000 AND job_id <= 4000"
	defaultJobsQuery := server.BuildJobQueryFromRequest(&serverpb.JobsRequest{})
	limitJobsQuery := server.BuildJobQueryFromRequest(&serverpb.JobsRequest{
		Limit: 50,
	})
	typeFilteredJobQueryNoRows := server.BuildJobQueryFromRequest(&serverpb.JobsRequest{
		Type: jobspb.TypeRestore,
	})
	typeFilteredJobQueryManyRows := server.BuildJobQueryFromRequest(&serverpb.JobsRequest{
		Type: jobspb.TypeImport,
	})
	reg.Register("Jobs", []RoundTripBenchTestCase{
		{
			SetupEx: setupQueries,
			Reset:   cleanupQuery,
			Name:    "show job",
			Stmt:    "SHOW JOB 2000",
		},
		{
			SetupEx: setupQueries,
			Reset:   cleanupQuery,
			Name:    "pause job",
			Stmt:    "PAUSE JOB 3001",
		},
		{
			SetupEx: setupQueries,
			Reset:   cleanupQuery,
			Name:    "cancel job",
			Stmt:    "CANCEL JOB 3001",
		},
		{
			SetupEx: setupQueries,
			Reset:   cleanupQuery,
			Name:    "resume job",
			Stmt:    "RESUME JOB 3002",
		},
		{
			SetupEx: setupQueries,
			Reset:   cleanupQuery,
			Name:    "show jobs",
			Stmt:    "SHOW JOBS",
		},
		{
			SetupEx:      setupQueries,
			Reset:        cleanupQuery,
			Name:         "non admin show jobs",
			Stmt:         "SHOW JOBS",
			NonAdminUser: true,
		},
		{
			SetupEx:  setupQueries,
			Reset:    cleanupQuery,
			Name:     "jobs page default",
			Stmt:     defaultJobsQuery.String(),
			StmtArgs: defaultJobsQuery.QueryArguments(),
		},
		{
			SetupEx:  setupQueries,
			Reset:    cleanupQuery,
			Name:     "jobs page latest 50",
			Stmt:     limitJobsQuery.String(),
			StmtArgs: limitJobsQuery.QueryArguments(),
		},
		{
			SetupEx:  setupQueries,
			Reset:    cleanupQuery,
			Name:     "jobs page type filtered",
			Stmt:     typeFilteredJobQueryManyRows.String(),
			StmtArgs: typeFilteredJobQueryManyRows.QueryArguments(),
		},
		{
			SetupEx:  setupQueries,
			Reset:    cleanupQuery,
			Name:     "jobs page type filtered no matches",
			Stmt:     typeFilteredJobQueryNoRows.String(),
			StmtArgs: typeFilteredJobQueryNoRows.QueryArguments(),
		},
		{
			SetupEx: setupQueries,
			Reset:   cleanupQuery,
			Name:    "crdb_internal.system_jobs",
			Stmt:    "SELECT * FROM crdb_internal.system_jobs",
		},
		{
			SetupEx:      setupQueries,
			Reset:        cleanupQuery,
			Name:         "non admin crdb_internal.system_jobs",
			Stmt:         "SELECT * FROM crdb_internal.system_jobs",
			NonAdminUser: true,
		},
	})
}
