// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobstest"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	pbtypes "github.com/gogo/protobuf/types"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestValidateTTLScheduledJobs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testCases := []struct {
		desc          string
		setup         func(t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, s serverutils.TestServerInterface, tableDesc *tabledesc.Mutable, scheduleID int64)
		expectedErrRe func(tableID descpb.ID, scheduleID int64) string
	}{
		{
			desc: "not pointing at a valid scheduled job",
			setup: func(t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, s serverutils.TestServerInterface, tableDesc *tabledesc.Mutable, scheduleID int64) {
				require.NoError(t, sql.TestingDescsTxn(ctx, s, func(ctx context.Context, txn isql.Txn, col *descs.Collection) (err error) {
					// We need the collection to read the descriptor from storage for
					// the subsequent write to succeed.
					tableDesc, err = col.MutableByID(txn.KV()).Table(ctx, tableDesc.GetID())
					tableDesc.RowLevelTTL.ScheduleID = 0
					tableDesc.Version++
					if err != nil {
						return err
					}
					return col.WriteDesc(ctx, false /* kvBatch */, tableDesc, txn.KV())
				}))
			},
			expectedErrRe: func(tableID descpb.ID, scheduleID int64) string {
				return fmt.Sprintf(`table id %d maps to a non-existent schedule id 0`, tableID)
			},
		},
		{
			desc: "scheduled job points at an different table",
			setup: func(t *testing.T, sqlDB *gosql.DB, kvDB *kv.DB, s serverutils.TestServerInterface, tableDesc *tabledesc.Mutable, scheduleID int64) {
				db := s.InternalDB().(isql.DB)
				require.NoError(t, db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
					schedules := jobs.ScheduledJobTxn(txn)
					sj, err := schedules.Load(
						ctx,
						jobstest.NewJobSchedulerTestEnv(
							jobstest.UseSystemTables,
							timeutil.Now(),
							tree.ScheduledBackupExecutor,
						),
						scheduleID,
					)
					if err != nil {
						return err
					}
					var args catpb.ScheduledRowLevelTTLArgs
					if err := pbtypes.UnmarshalAny(sj.ExecutionArgs().Args, &args); err != nil {
						return err
					}
					args.TableID = 0
					any, err := pbtypes.MarshalAny(&args)
					if err != nil {
						return err
					}
					sj.SetExecutionDetails(sj.ExecutorType(), jobspb.ExecutionArguments{Args: any})
					return schedules.Update(ctx, sj)
				}))
			},
			expectedErrRe: func(tableID descpb.ID, scheduleID int64) string {
				return fmt.Sprintf(`schedule id %d points to table id 0 instead of table id %d`, scheduleID, tableID)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			s, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)

			_, err := sqlDB.Exec(`CREATE TABLE t () WITH (ttl_expire_after = '10 mins')`)
			require.NoError(t, err)

			tableDesc := desctestutils.TestingGetMutableExistingTableDescriptor(kvDB, keys.SystemSQLCodec, "defaultdb", "t")
			require.NotNil(t, tableDesc.GetRowLevelTTL())
			scheduleID := tableDesc.GetRowLevelTTL().ScheduleID

			tc.setup(t, sqlDB, kvDB, s, tableDesc, scheduleID)

			_, err = sqlDB.Exec(`SELECT crdb_internal.validate_ttl_scheduled_jobs()`)
			require.Error(t, err)
			require.Regexp(t, tc.expectedErrRe(tableDesc.GetID(), scheduleID), err)
			var pgxErr *pq.Error
			require.True(t, errors.As(err, &pgxErr))
			require.Regexp(
				t,
				fmt.Sprintf(`use crdb_internal.repair_ttl_table_scheduled_job\(%d\) to repair the missing job`, tableDesc.GetID()),
				pgxErr.Hint,
			)

			// Repair and check jobs are valid.
			_, err = sqlDB.Exec(`DROP SCHEDULE $1`, scheduleID)
			require.NoError(t, err)
			_, err = sqlDB.Exec(`SELECT crdb_internal.repair_ttl_table_scheduled_job($1)`, tableDesc.GetID())
			require.NoError(t, err)
			_, err = sqlDB.Exec(`SELECT crdb_internal.validate_ttl_scheduled_jobs()`)
			require.NoError(t, err)
		})
	}
}

// TestSchemaChangeValidationWithCreateOnlyPrivilege asserts that a user
// holding only CREATE on a table (not SELECT) can still validate
// constraints they legitimately add. CHECK and UNIQUE WITHOUT INDEX
// validation now runs as the schema-change issuer; the implicit SELECT
// bypass scoped to the table being scanned is what lets a CREATE-only
// issuer pass. Without that bypass, a user who adds a CHECK or UWI
// constraint via the privilege they legitimately hold would fail
// validation with a permission error.
func TestSchemaChangeValidationWithCreateOnlyPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	rootDB := sqlutils.MakeSQLRunner(sqlDB)
	rootDB.Exec(t, fmt.Sprintf(`CREATE USER %s`, username.TestUser))
	rootDB.Exec(t, fmt.Sprintf(`GRANT USAGE ON SCHEMA defaultdb.public TO %s`, username.TestUser))

	userConn := s.SQLConn(t, serverutils.User(username.TestUser))
	userRunner := sqlutils.MakeSQLRunner(userConn)
	userRunner.Exec(t, `SET experimental_enable_unique_without_index_constraints = true`)

	for _, useDeclarative := range []bool{false, true} {
		name := "legacy"
		if useDeclarative {
			name = "declarative"
		}
		t.Run(name, func(t *testing.T) {
			tableName := fmt.Sprintf("create_only_%s", name)
			rootDB.Exec(t, fmt.Sprintf(`CREATE TABLE %s (x INT, y INT)`, tableName))
			rootDB.Exec(t, fmt.Sprintf(`INSERT INTO %s VALUES (1, 10), (2, 20), (3, 30)`, tableName))
			rootDB.Exec(t, fmt.Sprintf(`GRANT CREATE ON TABLE %s TO %s`, tableName, username.TestUser))

			if useDeclarative {
				userRunner.Exec(t, `SET use_declarative_schema_changer = 'on'`)
			} else {
				userRunner.Exec(t, `SET use_declarative_schema_changer = 'off'`)
				rootDB.Exec(t, fmt.Sprintf(`ALTER TABLE %s SET (schema_locked = false)`, tableName))
			}

			// CHECK exercises validateCheckExpr; the user holds only
			// CREATE (not SELECT) on the table, so without the implicit
			// SELECT bypass the validation scan errors out.
			userRunner.Exec(t,
				fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT c_x_positive CHECK (x > 0)`, tableName))

			// UNIQUE WITHOUT INDEX exercises validateUniqueConstraint via
			// the validateUniqueWithoutIndexConstraintInTxn (legacy) /
			// ValidateConstraint (declarative) call sites.
			userRunner.Exec(t,
				fmt.Sprintf(`ALTER TABLE %s ADD CONSTRAINT u_y UNIQUE WITHOUT INDEX (y)`, tableName))
		})
	}
}
