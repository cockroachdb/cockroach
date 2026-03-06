// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func (a *azureConfig) ecURI(file string, testID uint64) string {
	return fmt.Sprintf("azure-storage://%s/%s-%d?%s=%s&%s=%s&%s=%s",
		a.bucket, file, testID,
		AzureAccountKeyParam, url.QueryEscape(a.key),
		AzureAccountNameParam, url.QueryEscape(a.account),
		AzureEnvironmentKeyParam, url.QueryEscape(a.environment))
}

func TestExternalConnections(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	ts, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer ts.Stopper().Stop(context.Background())

	sqlDB := sqlutils.MakeSQLRunner(db)

	// Setup some dummy data.
	sqlDB.Exec(t, `CREATE DATABASE foo`)
	sqlDB.Exec(t, `USE foo`)
	sqlDB.Exec(t, `CREATE TABLE foo (id INT PRIMARY KEY)`)
	sqlDB.Exec(t, `INSERT INTO foo VALUES (1), (2), (3)`)

	createExternalConnection := func(externalConnectionName, uri string) {
		sqlDB.Exec(t, fmt.Sprintf(`CREATE EXTERNAL CONNECTION '%s' AS '%s'`, externalConnectionName, uri))
	}
	backupAndRestoreFromExternalConnection := func(backupExternalConnectionName string) {
		backupURI := fmt.Sprintf("external://%s", backupExternalConnectionName)
		sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE foo INTO '%s'`, backupURI))
		sqlDB.Exec(t, fmt.Sprintf(`RESTORE DATABASE foo FROM LATEST IN '%s' WITH new_db_name = bar`, backupURI))
		sqlDB.CheckQueryResults(t, `SELECT * FROM bar.foo`, [][]string{{"1"}, {"2"}, {"3"}})
		sqlDB.CheckQueryResults(t, `SELECT * FROM crdb_internal.invalid_objects`, [][]string{})
		sqlDB.Exec(t, `DROP DATABASE bar CASCADE`)
	}

	cfg, err := getAzureConfig()
	if err != nil {
		skip.IgnoreLint(t, "TestExternalConnections not configured for Azure")
		return
	}

	testID := cloudtestutils.NewTestID()
	ecName := "azure-ec"
	createExternalConnection(ecName, cfg.ecURI("backup-ec", testID))
	backupAndRestoreFromExternalConnection(ecName)
}
