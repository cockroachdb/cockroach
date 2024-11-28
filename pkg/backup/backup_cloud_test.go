// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cloud/azure"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

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
	creds, baseBucket := requiredS3CredsAndBucket(t)

	const numAccounts = 1000
	ctx := context.Background()

	for _, locked := range []bool{true, false} {
		bucket := baseBucket
		testName := "regular-bucket"
		if locked {
			testName = "object-locked-bucket"
			bucket += "-locked"
		}

		t.Run(testName, func(t *testing.T) {
			tc, db, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, InitManualReplication)
			defer cleanupFn()
			prefix := fmt.Sprintf("TestBackupRestoreS3-%d", timeutil.Now().UnixNano())
			uri := setupS3URI(t, db, bucket, prefix, creds)
			backupAndRestore(ctx, t, tc, []string{uri.String()}, []string{uri.String()}, numAccounts, nil)
		})
	}
}

// TestCloudBackupRestoreS3WithLegacyPut tests that backup/restore works when
// cloudstorage.s3.buffer_and_put_uploads.enabled=true is set.
func TestCloudBackupRestoreS3WithLegacyPut(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	creds, bucket := requiredS3CredsAndBucket(t)

	const numAccounts = 1000

	ctx := context.Background()
	tc, db, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, InitManualReplication)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreS3-%d", timeutil.Now().UnixNano())
	db.Exec(t, "SET CLUSTER SETTING cloudstorage.s3.buffer_and_put_uploads.enabled=true")
	uri := setupS3URI(t, db, bucket, prefix, creds)
	backupAndRestore(ctx, t, tc, []string{uri.String()}, []string{uri.String()}, numAccounts, nil)
}

func requiredS3CredsAndBucket(t *testing.T) (aws.Credentials, string) {
	t.Helper()
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err)
	if !envConfig.Credentials.HasKeys() {
		skip.IgnoreLintf(t, "No AWS env keys (%v)", err)
	}

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}
	return envConfig.Credentials, bucket
}

func setupS3URI(
	t *testing.T, db *sqlutils.SQLRunner, bucket string, prefix string, creds aws.Credentials,
) url.URL {
	t.Helper()
	endpoint := os.Getenv(amazon.NightlyEnvVarS3Params[amazon.AWSEndpointParam])
	customCACert := os.Getenv("AWS_CUSTOM_CA_CERT")
	region := os.Getenv(amazon.NightlyEnvVarS3Params[amazon.S3RegionParam])
	if customCACert != "" {
		db.Exec(t, fmt.Sprintf("SET CLUSTER SETTING cloudstorage.http.custom_ca='%s'", customCACert))
	}

	uri := url.URL{Scheme: "s3", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(amazon.AWSAccessKeyParam, creds.AccessKeyID)
	values.Add(amazon.AWSSecretParam, creds.SecretAccessKey)
	if endpoint != "" {
		values.Add(amazon.AWSEndpointParam, endpoint)
	}
	if region != "" {
		values.Add(amazon.S3RegionParam, region)
	}
	uri.RawQuery = values.Encode()
	return uri
}

func TestOnlineRestoreS3(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numAccounts = 1000
	_, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, singleNode, numAccounts, InitManualReplication)
	defer cleanupFn()

	creds, bucket := requiredS3CredsAndBucket(t)
	prefix := fmt.Sprintf("TestOnlineRestoreS3-%d", timeutil.Now().UnixNano())
	uri := setupS3URI(t, sqlDB, bucket, prefix, creds)
	externalStorage := uri.String()

	sqlDB.Exec(t, fmt.Sprintf("BACKUP INTO '%s'", externalStorage))

	params := base.TestClusterArgs{}
	_, rSQLDB, cleanupFnRestored := backupRestoreTestSetupEmpty(t, 1, "", InitManualReplication, params)
	defer cleanupFnRestored()

	bankOnlineRestore(t, rSQLDB, numAccounts, externalStorage)
}

// TestBackupRestoreGoogleCloudStorage hits the real GCS and so could
// occasionally be flaky. It's only run if the GS_BUCKET environment var is set.
func TestCloudBackupRestoreGoogleCloudStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	bucket := os.Getenv("GOOGLE_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "GOOGLE_BUCKET env var must be set")
	}

	const numAccounts = 1000

	ctx := context.Background()
	tc, _, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, InitManualReplication)
	defer cleanupFn()
	prefix := fmt.Sprintf("TestBackupRestoreGoogleCloudStorage-%d", timeutil.Now().UnixNano())
	uri := url.URL{Scheme: "gs", Host: bucket, Path: prefix}
	values := uri.Query()
	values.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	uri.RawQuery = values.Encode()
	backupAndRestore(ctx, t, tc, []string{uri.String()}, []string{uri.String()}, numAccounts, nil)
}

// TestCloudBackupRestoreAzure hits the real Azure Blob Storage and so could
// occasionally be flaky. It's only run if the AZURE_ACCOUNT_NAME
// and AZURE_CONTAINER environment vars are set.
func TestCloudBackupRestoreAzure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	if accountName == "" {
		skip.IgnoreLint(t, "AZURE_ACCOUNT_NAME env var must be set")
	}

	bucket := os.Getenv("AZURE_CONTAINER")
	if bucket == "" {
		skip.IgnoreLint(t, "AZURE_CONTAINER env var must be set")
	}

	storageCases := []struct {
		name   string
		params map[string]string
	}{
		{
			"storage auth legacy",
			map[string]string{
				// NB: the Azure Account key must not be url encoded.
				"AZURE_ACCOUNT_KEY": os.Getenv("AZURE_ACCOUNT_KEY")},
		},
		{
			"storage auth explicit",
			map[string]string{
				"AZURE_CLIENT_ID":     os.Getenv("AZURE_CLIENT_ID"),
				"AZURE_CLIENT_SECRET": os.Getenv("AZURE_CLIENT_SECRET"),
				"AZURE_TENANT_ID":     os.Getenv("AZURE_TENANT_ID")},
		},
		{
			"storage auth implicit",
			map[string]string{"AUTH": "implicit"},
		},
	}

	kmsCases := []struct {
		name   string
		params map[string]string
	}{
		{
			"no kms",
			map[string]string{},
		},
		{
			"kms auth explicit",
			map[string]string{
				"AZURE_CLIENT_ID":     os.Getenv("AZURE_CLIENT_ID"),
				"AZURE_CLIENT_SECRET": os.Getenv("AZURE_CLIENT_SECRET"),
				"AZURE_TENANT_ID":     os.Getenv("AZURE_TENANT_ID"),
				"AZURE_VAULT_NAME":    os.Getenv("AZURE_VAULT_NAME")},
		},
		{
			"kms auth implicit",
			map[string]string{
				"AUTH":             "implicit",
				"AZURE_VAULT_NAME": os.Getenv("AZURE_VAULT_NAME")},
		},
	}

	for _, sc := range storageCases {
		for _, kc := range kmsCases {
			name := fmt.Sprintf("%s/%s", sc.name, kc.name)
			t.Run(name, func(t *testing.T) {
				const numAccounts = 1000

				ctx := context.Background()
				testCluster, _, _, cleanupFn := backupRestoreTestSetup(t, 1, numAccounts, InitManualReplication)
				defer cleanupFn()
				prefix := fmt.Sprintf("TestBackupRestoreAzure-%d", timeutil.Now().UnixNano())

				storageURI := url.URL{Scheme: "azure", Host: bucket, Path: prefix}
				storageValues := storageURI.Query()
				storageValues.Add(azure.AzureAccountNameParam, accountName)
				for k, v := range sc.params {
					storageValues.Add(k, v)
				}
				storageURI.RawQuery = storageValues.Encode()

				var kmsURI []string
				if len(kc.params) != 0 {
					kmsValues := make(url.Values)
					for k, v := range kc.params {
						kmsValues.Add(k, v)
					}
					kmsURI = append(kmsURI,
						fmt.Sprintf(
							"azure-kms:///%s/%s?%s",
							os.Getenv("AZURE_KMS_KEY_NAME"),
							os.Getenv("AZURE_KMS_KEY_VERSION"),
							kmsValues.Encode()))
				}

				backupAndRestore(ctx, t, testCluster, []string{storageURI.String()}, []string{storageURI.String()}, numAccounts, kmsURI)
			})
		}
	}
}

// TestCloudBackupRestoreKMSInaccessibleMetric tests that backup statements
// updates the KMSInaccessibleError metric.
func TestCloudBackupRestoreKMSInaccessibleMetric(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	azureVaultName := os.Getenv("AZURE_VAULT_NAME")
	if azureVaultName == "" {
		skip.IgnoreLintf(t, "AZURE_VAULT_NAME env var must be set")
	}

	for _, tt := range []struct {
		name string
		uri  string
	}{
		{
			name: "s3",
			uri:  "aws-kms:///non-existent-key?AUTH=implicit&REGION=us-east-1",
		},
		{
			name: "gcs",
			uri:  "gcp-kms:///non-existent-key?AUTH=implicit",
		},
		{
			name: "azure",
			uri:  fmt.Sprintf("azure-kms:///non-existent-key/000?AUTH=implicit&AZURE_VAULT_NAME=%s", azureVaultName),
		},
	} {
		tc, sqlDB, _, cleanupFn := backupRestoreTestSetup(t, 1, 1, InitManualReplication)
		defer cleanupFn()

		t.Run(tt.name, func(t *testing.T) {
			testStart := timeutil.Now().Unix()
			bm := tc.Server(0).JobRegistry().(*jobs.Registry).MetricsStruct().Backup.(*BackupMetrics)

			// LastKMSInaccessibleErrorTime should not be set without
			// updates_cluster_monitoring_metrics despite a KMS error.
			sqlDB.ExpectErr(t, "failed to encrypt data key", "BACKUP INTO 'userfile:///foo' WITH OPTIONS (kms = $1)", tt.uri)
			require.Equal(t, bm.LastKMSInaccessibleErrorTime.Value(), int64(0))

			// LastKMSInaccessibleErrorTime should be set with
			// updates_cluster_monitoring_metrics.
			sqlDB.ExpectErr(t, "failed to encrypt data key", "BACKUP INTO 'userfile:///foo' WITH OPTIONS (kms = $1, updates_cluster_monitoring_metrics)", tt.uri)
			require.GreaterOrEqual(t, bm.LastKMSInaccessibleErrorTime.Value(), testStart)
		})
	}
}
