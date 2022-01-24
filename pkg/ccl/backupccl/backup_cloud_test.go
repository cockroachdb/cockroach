// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cloud/azure"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// InitManualReplication calls tc.ToggleReplicateQueues(false).
//
// Note that the test harnesses that use this typically call
// tc.WaitForFullReplication before calling this method,
// so up-replication has usually already taken place.
func InitManualReplication(tc *testcluster.TestCluster) {
	tc.ToggleReplicateQueues(false)
}

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
	defer log.Scope(t).Close(t)
	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		skip.IgnoreLintf(t, "No AWS env keys (%v)", err)
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer lease.TestingDisableTableLeases()()
	const numAccounts = 1000

	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, InitManualReplication)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreS3-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "s3", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(amazon.AWSAccessKeyParam, creds.AccessKeyID)
	values.Add(amazon.AWSSecretParam, creds.SecretAccessKey)
	uri.RawQuery = values.Encode()

	backupAndRestore(ctx, t, tc, []string{uri.String()}, []string{uri.String()}, numAccounts)
}

// TestBackupRestoreGoogleCloudStorage hits the real GCS and so could
// occasionally be flaky. It's only run if the GS_BUCKET environment var is set.
func TestCloudBackupRestoreGoogleCloudStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	bucket := os.Getenv("GS_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "GS_BUCKET env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer lease.TestingDisableTableLeases()()
	const numAccounts = 1000

	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, InitManualReplication)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreGoogleCloudStorage-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "gs", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	uri.RawQuery = values.Encode()
	backupAndRestore(ctx, t, tc, []string{uri.String()}, []string{uri.String()}, numAccounts)
}

// TestBackupRestoreAzure hits the real Azure Blob Storage and so could
// occasionally be flaky. It's only run if the AZURE_ACCOUNT_NAME and
// AZURE_ACCOUNT_KEY environment vars are set.
func TestCloudBackupRestoreAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	accountKey := os.Getenv("AZURE_ACCOUNT_KEY")
	if accountName == "" || accountKey == "" {
		skip.IgnoreLint(t, "AZURE_ACCOUNT_NAME and AZURE_ACCOUNT_KEY env vars must be set")
	}
	bucket := os.Getenv("AZURE_CONTAINER")
	if bucket == "" {
		skip.IgnoreLint(t, "AZURE_CONTAINER env var must be set")
	}

	// TODO(dan): Actually invalidate the descriptor cache and delete this line.
	defer lease.TestingDisableTableLeases()()
	const numAccounts = 1000

	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, InitManualReplication)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreAzure-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "azure", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(azure.AzureAccountNameParam, accountName)
	values.Add(azure.AzureAccountKeyParam, accountKey)
	uri.RawQuery = values.Encode()

	backupAndRestore(ctx, t, tc, []string{uri.String()}, []string{uri.String()}, numAccounts)
}
