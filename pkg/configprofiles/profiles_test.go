// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package configprofiles_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	// To enable the CCL-only functions.
	"github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/configprofiles"
	"github.com/cockroachdb/cockroach/pkg/server/autoconfig/autoconfigpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestProfilesValidSQL checks that the SQL of each static
// configuration profile can be applied successfully on a new cluster.
func TestProfilesValidSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer ccl.TestingEnableEnterprise()()

	for profileName, tasks := range configprofiles.TestingGetProfiles() {
		t.Run(profileName, func(t *testing.T) {
			defer log.Scope(t).Close(t)

			s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{
				DisableDefaultTestTenant: true,
			})
			defer s.Stopper().Stop(context.Background())
			db := sqlutils.MakeSQLRunner(sqlDB)

			for _, task := range tasks {
				t.Run(fmt.Sprintf("%d/%s", task.TaskID, task.Description), func(t *testing.T) {
					switch payload := task.GetPayload().(type) {
					case *autoconfigpb.Task_SimpleSQL:
						for _, stmt := range payload.SimpleSQL.NonTransactionalStatements {
							t.Logf("non-txn statement: %s", stmt)
							db.Exec(t, stmt)
							// Try it a second time too -- it should work since
							// non-txn statements are supposed to be idempotent
							// (because jobs can be retried).
							db.Exec(t, stmt)
						}
						db.Exec(t, "BEGIN TRANSACTION")
						for _, stmt := range payload.SimpleSQL.TransactionalStatements {
							t.Logf("txn statement: %s", stmt)
							db.Exec(t, stmt)
						}
						db.Exec(t, "COMMIT TRANSACTION")

					default:
						t.Fatalf("unsupported payload type %T", payload)
					}
				})
			}
		})
	}
}

// TestMonotonicTaskIDs checks that the task IDs of each profile
// are increasing monotonically.
func TestMonotonicTaskIDs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for profileName, tasks := range configprofiles.TestingGetProfiles() {
		t.Run(profileName, func(t *testing.T) {
			prevTaskID := autoconfigpb.TaskID(0)
			for taskNum, task := range tasks {
				if task.TaskID <= prevTaskID {
					t.Fatalf("%d: task ID %d not greater than previous task ID %d", taskNum, task.TaskID, prevTaskID)
				}
				prevTaskID = task.TaskID
			}
		})
	}
}
