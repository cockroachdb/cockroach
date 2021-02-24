// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemachanger_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaChangeWaitsForOtherSchemaChanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer sqltestutils.SetTestJobsAdoptInterval()()

	t.Run("wait for old-style schema changes", func(t *testing.T) {
		ctx := context.Background()

		var doOnce sync.Once
		// Closed when we enter the RunBeforeBackfill knob.
		beforeBackfillNotification := make(chan struct{})
		// Closed when we're ready to continue with the schema change.
		continueNotification := make(chan struct{})

		var kvDB *kv.DB
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLSchemaChanger: &sql.SchemaChangerTestingKnobs{
				RunBeforeBackfill: func() error {
					doOnce.Do(func() {
						close(beforeBackfillNotification)
						<-continueNotification
					})
					return nil
				},
			},
			SQLNewSchemaChanger: &scexec.NewSchemaChangerTestingKnobs{
				BeforeStage: func(phase scplan.Phase, _ *scplan.Stage) {
					// Assert that there are no mutations being prematurely queued.
					if phase != scplan.PostCommitPhase {
						return
					}
					table := catalogkv.TestingGetTableDescriptorFromSchema(
						kvDB, keys.SystemSQLCodec, "db", "public", "t")
					mutations := table.GetMutations()
					t.Logf("mutations: %+v", mutations)
					// There are 2 schema changes that should precede the one being run
					// in the new schema changer. Note that the new-style schema change
					// itself uses multiple mutation IDs.
					for i := range mutations {
						assert.Greater(t, int(mutations[i].MutationID), 2)
					}
				},
			},
		}
		var s serverutils.TestServerInterface
		var sqlDB *gosql.DB
		s, sqlDB, kvDB = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)

		g := ctxgroup.WithContext(ctx)

		// Start an index schema change, which does not use the new schema changer.
		g.GoCtx(func(ctx context.Context) error {
			_, err := sqlDB.ExecContext(ctx, `CREATE INDEX idx ON db.t(a)`)
			assert.NoError(t, err)
			return nil
		})

		<-beforeBackfillNotification

		// Start a column schema change which uses the new schema changer.
		g.GoCtx(func(ctx context.Context) error {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, `SET experimental_use_new_schema_changer = 'on'`)
			assert.NoError(t, err)
			_, err = conn.ExecContext(ctx, `ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1`)
			assert.NoError(t, err)
			return nil
		})

		// Start another index schema change, which does not use the new schema
		// changer. We sleep a bit to increase the chances of a more interesting
		// interaction with the column schema change, which will probably not "see"
		// this second index schema change when first reading the table descriptor.
		time.Sleep(100 * time.Millisecond)
		g.GoCtx(func(ctx context.Context) error {
			_, err := sqlDB.ExecContext(ctx, `CREATE INDEX idx2 ON db.t(a)`)
			assert.NoError(t, err)
			return nil
		})

		// Finally, let the first index schema change finish, which will unblock the
		// others.
		close(continueNotification)
		require.NoError(t, g.Wait())

		// Check that the new schema change job was created last.
		tdb.CheckQueryResults(t,
			fmt.Sprintf(`SELECT job_type, status, description FROM crdb_internal.jobs WHERE job_type = '%s' OR job_type = '%s' ORDER BY created`,
				jobspb.TypeSchemaChange.String(), jobspb.TypeNewSchemaChange.String(),
			),
			[][]string{
				{jobspb.TypeSchemaChange.String(), string(jobs.StatusSucceeded), `CREATE INDEX idx ON db.public.t (a)`},
				{jobspb.TypeSchemaChange.String(), string(jobs.StatusSucceeded), `CREATE INDEX idx2 ON db.public.t (a)`},
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded), `Schema change job`},
			},
		)
	})

	t.Run("wait for new-style schema changes", func(t *testing.T) {
		ctx := context.Background()

		var doOnce sync.Once
		// Closed when we enter the RunBeforeBackfill knob.
		beforeBackfillNotification := make(chan struct{})
		// Closed when we're ready to continue with the schema change.
		continueNotification := make(chan struct{})

		var kvDB *kv.DB
		params, _ := tests.CreateTestServerParams()
		params.Knobs = base.TestingKnobs{
			SQLNewSchemaChanger: &scexec.NewSchemaChangerTestingKnobs{
				BeforeStage: func(phase scplan.Phase, stage *scplan.Stage) {
					// Verify that we never queue mutations for the second schema change
					// before finishing the first one.
					if phase != scplan.PostCommitPhase {
						return
					}
					table := catalogkv.TestingGetTableDescriptorFromSchema(
						kvDB, keys.SystemSQLCodec, "db", "public", "t")
					mutations := table.GetMutations()
					if len(mutations) == 0 {
						t.Errorf("unexpected empty mutations")
						return
					}
					var idsSeen []descpb.MutationID
					for i := range mutations {
						m := &mutations[i]
						if len(idsSeen) == 0 || m.MutationID > idsSeen[len(idsSeen)-1] {
							idsSeen = append(idsSeen, m.MutationID)
						}
					}
					// Each schema change involving an index backfill has 2 consecutive
					// mutation IDs, so the first schema change is expected to have
					// mutation IDs 1 and 2, and the second one 3 and 4.
					//
					// This test would be better if we had some unique identifier for each
					// run of the schema changer.
					lowestID, highestID := idsSeen[0], idsSeen[len(idsSeen)-1]
					firstSchemaChangeExecutingAlone := highestID <= 2
					secondSchemaChangeExecutingAlone := lowestID >= 3 && highestID <= 4
					assert.Truef(t, firstSchemaChangeExecutingAlone || secondSchemaChangeExecutingAlone,
						"unexpected mutation IDs %v", idsSeen)

					if stage.Ops.Type() != scop.BackfillType {
						return
					}

					for _, op := range stage.Ops.Slice() {
						// Only block the first schema change.
						if backfillOp, ok := op.(scop.BackfillIndex); ok && backfillOp.IndexID == descpb.IndexID(2) {
							doOnce.Do(func() {
								close(beforeBackfillNotification)
								<-continueNotification
							})
						}
					}
				},
			},
		}

		var s serverutils.TestServerInterface
		var sqlDB *gosql.DB
		s, sqlDB, kvDB = serverutils.StartServer(t, params)
		defer s.Stopper().Stop(ctx)

		tdb := sqlutils.MakeSQLRunner(sqlDB)
		tdb.Exec(t, `CREATE DATABASE db`)
		tdb.Exec(t, `CREATE TABLE db.t (a INT PRIMARY KEY)`)

		g := ctxgroup.WithContext(ctx)

		g.GoCtx(func(ctx context.Context) error {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, `SET experimental_use_new_schema_changer = 'on'`)
			assert.NoError(t, err)
			_, err = conn.ExecContext(ctx, `ALTER TABLE db.t ADD COLUMN b INT DEFAULT 1`)
			assert.NoError(t, err)
			return nil
		})

		<-beforeBackfillNotification

		g.GoCtx(func(ctx context.Context) error {
			conn, err := sqlDB.Conn(ctx)
			if err != nil {
				return err
			}
			_, err = conn.ExecContext(ctx, `SET experimental_use_new_schema_changer = 'on'`)
			assert.NoError(t, err)
			_, err = conn.ExecContext(ctx, `ALTER TABLE db.t ADD COLUMN c INT DEFAULT 2`)
			assert.NoError(t, err)
			return nil
		})

		close(continueNotification)
		require.NoError(t, g.Wait())

		tdb.CheckQueryResults(t,
			fmt.Sprintf(`SELECT job_type, status FROM crdb_internal.jobs WHERE job_type = '%s' OR job_type = '%s' ORDER BY created`,
				jobspb.TypeSchemaChange.String(), jobspb.TypeNewSchemaChange.String(),
			),
			[][]string{
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded)},
				{jobspb.TypeNewSchemaChange.String(), string(jobs.StatusSucceeded)},
			},
		)
	})
}
