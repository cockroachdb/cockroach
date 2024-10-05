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
		"INSERT INTO system.jobs(id, status, created, job_type) (SELECT id, 'succeeded', now(), 'IMPORT' FROM generate_series(1000, 3000) as id)",
	}

	reg.Register("Jobs", []RoundTripBenchTestCase{
		{
			SetupEx: setupQueries,
			Reset:   "DELETE FROM system.jobs WHERE id >= 1000 AND id <= 3000; DELETE FROM system.job_info WHERE job_id >= 1000 AND job_id <= 3000",
			Name:    "show job",
			Stmt:    "SHOW JOB 2000",
		},
	})
}
