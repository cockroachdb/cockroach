// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package azure

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"

	az "github.com/Azure/go-autorest/autorest/azure"
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/cloud/azure"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/externalconn/providers" // import External Connection providers.
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func (a azureConfig) URI(file string, testID uint64) string {
	return fmt.Sprintf("azure-storage://%s/%s-%d?%s=%s&%s=%s&%s=%s",
		a.bucket, file, testID,
		azure.AzureAccountKeyParam, url.QueryEscape(a.key),
		azure.AzureAccountNameParam, url.QueryEscape(a.account),
		azure.AzureEnvironmentKeyParam, url.QueryEscape(a.environment))
}

type azureConfig struct {
	account, key, bucket, environment string
}

func getAzureConfig() (azureConfig, error) {
	cfg := azureConfig{
		account:     os.Getenv("AZURE_ACCOUNT_NAME"),
		key:         os.Getenv("AZURE_ACCOUNT_KEY"),
		bucket:      os.Getenv("AZURE_CONTAINER"),
		environment: az.PublicCloud.Name,
	}
	if cfg.account == "" || cfg.key == "" || cfg.bucket == "" {
		return azureConfig{}, errors.New("AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY, AZURE_CONTAINER must all be set")
	}
	if v, ok := os.LookupEnv(azure.AzureEnvironmentKeyParam); ok {
		cfg.environment = v
	}
	return cfg, nil
}

func TestExternalConnections(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	params := base.TestClusterArgs{}
	params.ServerArgs.ExternalIODir = dir

	tc := testcluster.StartTestCluster(t, 1, params)
	defer tc.Stopper().Stop(context.Background())

	tc.WaitForNodeLiveness(t)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

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
	createExternalConnection(ecName, cfg.URI("backup-ec", testID))
	backupAndRestoreFromExternalConnection(ecName)
}
