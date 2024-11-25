// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package backupccl

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestBackupCompaction(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()

	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	}
	dataDir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()
	_, sqlDB, cleanupFn := backupRestoreTestSetupEmpty(
		t,
		singleNode,
		dataDir,
		InitManualReplication,
		params,
	)
	defer cleanupFn()
	fullBackupStmt := fmt.Sprintf(`BACKUP TABLE person INTO '%s'`, localFoo)
	incBackupStmt := fmt.Sprintf(`BACKUP TABLE person INTO LATEST IN '%s'`, localFoo)
	sqlDB.Exec(t, "CREATE TABLE person (id INT PRIMARY KEY, name STRING)")
	sqlDB.Exec(t, "INSERT INTO person VALUES (1, 'foo')")
	sqlDB.Exec(t, fullBackupStmt)
	sqlDB.Exec(t, "INSERT INTO person VALUES (2, 'bar')")
	sqlDB.Exec(t, incBackupStmt)
	sqlDB.Exec(t, "INSERT INTO person VALUES (3, 'bax')")
	sqlDB.Exec(t, "INSERT INTO person VALUES (4, 'qux')")
	sqlDB.Exec(t, incBackupStmt)
	sqlDB.Exec(t, "UPDATE person SET name = 'baz' WHERE id = 3")
	sqlDB.Exec(t, "DELETE FROM person WHERE id = 4")
	sqlDB.Exec(t, incBackupStmt)
	rows := sqlDB.Query(t, "SELECT * FROM person")
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			t.Fatal(err)
		}
		t.Logf("id=%d name=%s", id, name)
	}
}
