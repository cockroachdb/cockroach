// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/pkg/ccl/LICENSE

package sqlccl

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/rlmcpherson/s3gof3r"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// The tests in this file talk to remote APIs which require credentials.
// To run these tests, you need to supply credentials via env vars (the tests
// skip themselves if they are not set). Obtain these credentials from the
// admin consoles of the various cloud providers.
// .customenv.mak (gitignored) may be a useful place to record these.
// Cockroach Labs Employees: symlink .customenv.mak to copy in `production`.

// TestBackupRestoreS3 hits the real S3 and so could occasionally be flaky. It's
// only run if the AWS_S3_BUCKET environment var is set.
func TestBackupRestoreS3(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s3Keys, err := s3gof3r.EnvKeys()
	if err != nil {
		s3Keys, err = s3gof3r.InstanceKeys()
		if err != nil {
			t.Skip("No AWS keys instance or env keys")
		}
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		// CRL uses a bucket `cockroach-backup-tests` that has a 24h TTL policy.
		t.Skip("AWS_S3_BUCKET env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	ctx, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreS3-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "s3", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(storageccl.S3AccessKeyParam, s3Keys.AccessKey)
	values.Add(storageccl.S3SecretParam, s3Keys.SecretKey)
	uri.RawQuery = values.Encode()

	backupAndRestore(ctx, t, sqlDB, uri.String(), numAccounts)
}

// TestBackupRestoreGoogleCloudStorage hits the real GCS and so could
// occasionally be flaky. It's only run if the GS_BUCKET environment var is set.
func TestBackupRestoreGoogleCloudStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	bucket := os.Getenv("GS_BUCKET")
	if bucket == "" {
		t.Skip("GS_BUCKET env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	ctx, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreGoogleCloudStorage-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "gs", Host: bucket, Path: prefix}
	backupAndRestore(ctx, t, sqlDB, uri.String(), numAccounts)
}

// TestBackupRestoreAzure hits the real Azure Blob Storage and so could
// occasionally be flaky. It's only run if the AZURE_ACCOUNT_NAME and
// AZURE_ACCOUNT_KEY environment vars are set.
func TestBackupRestoreAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	if accountName == "" || accountKey == "" {
		t.Skip("AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY env vars must be set")
	}
	bucket := os.Getenv("AZURE_CONTAINER")
	if bucket == "" {
		t.Skip("AZURE_CONTAINER env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	// TODO(dt): this prevents leaking an http conn goroutine.
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	ctx, _, _, sqlDB, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreAzure-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "azure", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(storageccl.AzureAccountNameParam, accountName)
	values.Add(storageccl.AzureAccountKeyParam, accountKey)
	uri.RawQuery = values.Encode()

	backupAndRestore(ctx, t, sqlDB, uri.String(), numAccounts)
}

func backupAndRestore(
	ctx context.Context, t *testing.T, sqlDB *sqlutils.SQLRunner, dest string, numAccounts int64,
) {
	{
		var unused string
		var dataSize int64
		sqlDB.QueryRow(fmt.Sprintf(`BACKUP DATABASE bench TO '%s'`, dest)).Scan(
			&unused, &unused, &unused, &dataSize,
		)
		approxDataSize := int64(backupRestoreRowPayloadSize) * numAccounts
		if max := approxDataSize * 2; dataSize < approxDataSize || dataSize > 2*max {
			t.Errorf("expected data size in [%d,%d] but was %d", approxDataSize, max, dataSize)
		}
	}

	// Start a new cluster to restore into.
	{
		tcRestore := testcluster.StartTestCluster(t, multiNode, base.TestClusterArgs{})
		defer tcRestore.Stopper().Stop()
		sqlDBRestore := sqlutils.MakeSQLRunner(t, tcRestore.Conns[0])

		// Restore assumes the database exists.
		sqlDBRestore.Exec(bankCreateDatabase)

		// Force the ID of the restored bank table to be different.
		sqlDBRestore.Exec(`CREATE TABLE bench.empty (a INT PRIMARY KEY)`)

		sqlDBRestore.Exec(fmt.Sprintf(`RESTORE DATABASE bench FROM '%s'`, dest))

		var rowCount int64
		sqlDBRestore.QueryRow(`SELECT COUNT(*) FROM bench.bank`).Scan(&rowCount)
		if rowCount != numAccounts {
			t.Fatalf("expected %d rows but found %d", numAccounts, rowCount)
		}
	}
}
