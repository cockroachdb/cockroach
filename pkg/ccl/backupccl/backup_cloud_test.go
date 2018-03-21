// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl_test

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func initNone(_ *testcluster.TestCluster) {}

// The tests in this file talk to remote APIs which require credentials.
// To run these tests, you need to supply credentials via env vars (the tests
// skip themselves if they are not set). Obtain these credentials from the
// admin consoles of the various cloud providers.
// customenv.mak (gitignored) may be a useful place to record these.
// Cockroach Labs Employees: symlink customenv.mk to copy in `production`.

// TestBackupRestoreS3 hits the real S3 and so could occasionally be flaky. It's
// only run if the AWS_S3_BUCKET environment var is set.
func TestCloudBackupRestoreS3(t *testing.T) {
	defer leaktest.AfterTest(t)()
	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		t.Skipf("No AWS env keys (%v)", err)
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		t.Skip("AWS_S3_BUCKET env var must be set")
	}

	// TODO(dt): this prevents leaking an http conn goroutine.
	defer func(disableKeepAlives bool) {
		http.DefaultTransport.(*http.Transport).DisableKeepAlives = disableKeepAlives
	}(http.DefaultTransport.(*http.Transport).DisableKeepAlives)
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	ctx, tc, _, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, initNone)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreS3-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "s3", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(storageccl.S3AccessKeyParam, creds.AccessKeyID)
	values.Add(storageccl.S3SecretParam, creds.SecretAccessKey)
	uri.RawQuery = values.Encode()

	backupAndRestore(ctx, t, tc, uri.String(), numAccounts)
}

// TestBackupRestoreGoogleCloudStorage hits the real GCS and so could
// occasionally be flaky. It's only run if the GS_BUCKET environment var is set.
func TestCloudBackupRestoreGoogleCloudStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	bucket := os.Getenv("GS_BUCKET")
	if bucket == "" {
		t.Skip("GS_BUCKET env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer sql.TestDisableTableLeases()()
	const numAccounts = 1000

	// TODO(dt): this prevents leaking an http conn goroutine -- presumably the
	// conn is held open for reuse until the idle timeout. Ideally we'd test with
	// conn reuse enabled though, to match expected production behavior.
	defer func(disableKeepAlives bool) {
		http.DefaultTransport.(*http.Transport).DisableKeepAlives = disableKeepAlives
	}(http.DefaultTransport.(*http.Transport).DisableKeepAlives)
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	ctx, tc, _, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, initNone)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreGoogleCloudStorage-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "gs", Host: bucket, Path: prefix}
	backupAndRestore(ctx, t, tc, uri.String(), numAccounts)
}

// TestBackupRestoreAzure hits the real Azure Blob Storage and so could
// occasionally be flaky. It's only run if the AZURE_ACCOUNT_NAME and
// AZURE_ACCOUNT_KEY environment vars are set.
func TestCloudBackupRestoreAzure(t *testing.T) {
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
	defer func(disableKeepAlives bool) {
		http.DefaultTransport.(*http.Transport).DisableKeepAlives = disableKeepAlives
	}(http.DefaultTransport.(*http.Transport).DisableKeepAlives)
	http.DefaultTransport.(*http.Transport).DisableKeepAlives = true

	ctx, tc, _, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, initNone)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreAzure-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "azure", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(storageccl.AzureAccountNameParam, accountName)
	values.Add(storageccl.AzureAccountKeyParam, accountKey)
	uri.RawQuery = values.Encode()

	backupAndRestore(ctx, t, tc, uri.String(), numAccounts)
}
