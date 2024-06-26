// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

func TestCreateIndexWithStmtTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: &jobs.TestingKnobs{
				BeforeUpdate: func(orig, updated jobs.JobMetadata) error {
					if orig.Payload.Type() != jobspb.TypeNewSchemaChange {
						return nil
					}
					time.Sleep(5 * time.Second)
					return nil
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)
	tDB := sqlutils.MakeSQLRunner(sqlDB)

	// Set up
	tDB.Exec(t, `
CREATE TABLE t(
  a INT PRIMARY KEY,
  b INT
);
INSERT INTO t VALUES (1)
`,
	)

	var jobCount int
	// We don't do it earlier because migration-job-find-already-completed
	tDB.Exec(t, "SET CLUSTER SETTING sql.defaults.statement_timeout = '3s'")
	tDB.Exec(t, `CREATE UNIQUE INDEX bar ON t (b)`)
	q := `SELECT COUNT(*) 
FROM crdb_internal.jobs 
WHERE job_type = 'NEW SCHEMA CHANGE' AND status = 'succeeded'`
	tDB.QueryRow(t, q).Scan(&jobCount)
	// Assert that the job has completed
	assert.Equal(t, 1, jobCount)
}
