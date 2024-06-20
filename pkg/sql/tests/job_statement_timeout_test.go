// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

func TestBackgroundJobIgnoresStatementTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testutils.RunTrueAndFalse(t, "use-declarative-schema-changer", func(
		t *testing.T, useDeclarativeSchemaChanger bool,
	) {
		s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: &jobs.TestingKnobs{
					BeforeUpdate: func(orig, updated jobs.JobMetadata) error {
						isCreateIndex := strings.Contains(orig.Payload.Description, "CREATE UNIQUE INDEX bar")
						if orig.Payload.Type() != jobspb.TypeNewSchemaChange && !isCreateIndex {
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
		if useDeclarativeSchemaChanger {
			tDB.Exec(t, "SET use_declarative_schema_changer = on;")
		} else {
			tDB.Exec(t, "SET use_declarative_schema_changer = off;")
		}
		tDB.Exec(t, `
CREATE TABLE t(
  a INT PRIMARY KEY,
  b INT
);
INSERT INTO t VALUES (1);
`)

		var jobCount int
		// We don't set statement timeout earlier because
		// migration-job-find-already-completed will fail unless a fairly large
		// timeout is set.
		tDB.Exec(t, "SET CLUSTER SETTING sql.defaults.statement_timeout = '3s'")
		tDB.ExecSucceedsSoon(t, `CREATE UNIQUE INDEX bar ON t (b)`)
		q := `SELECT count(*) 
FROM crdb_internal.jobs 
WHERE job_type ILIKE '%SCHEMA CHANGE%' AND status = 'succeeded'
AND description ILIKE 'CREATE UNIQUE INDEX bar%'`
		tDB.QueryRow(t, q).Scan(&jobCount)
		// Assert that the job has completed
		assert.Equal(t, 1, jobCount)
	})
}
