// Copyright 2025 The Cockroach Authors.
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
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/cockroachdb/cockroach/pkg/backup/backuptestutils"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cloud/azure"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/besteffort"
	"github.com/cockroachdb/cockroach/pkg/util/fault"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

// TestCloudBackupRestore_FlakyStorage tests backup and restore operations with
// flaky storage enabled. It creates a table with rows, performs backups with
// retries, drops the table, performs restore with retries, and verifies the
// restored data matches the original.
func TestCloudBackupRestore_FlakyStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Allow besteffort operations to fail without panicking since fault
	// injection may cause cleanup/telemetry operations to fail.
	defer besteffort.TestAllowAllFailures()()

	// TODO(at): restructure test to work with OR metamorphic.
	backuptestutils.DisableFastRestoreForTest(t)

	// Set up a slim test server with flaky storage enabled
	params := base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
				CloudStorageKnobs: &cloud.TestingKnobs{
					OpFaults: fault.NewProbabilisticFaults(0.01),
					IoFaults: fault.NewProbabilisticFaults(0.001),
				},
			},
		},
	}

	ctx := context.Background()

	tc, sqlDB, _, cleanupFn := backupRestoreTestSetupWithParams(
		t, singleNode, 0, InitManualReplication, params)
	defer cleanupFn()

	sql := sqlutils.MakeSQLRunner(tc.Conns[0])

	backupStores := []backupStoreURI{
		nodelocalBackupStoreURI{},
		gcpBackupStoreURI{},
		s3BackupStoreURI{},
		azureBackupStoreURI{},
	}

	for _, store := range backupStores {
		t.Run(fmt.Sprintf("cloud=%s", store.Cloud()), func(t *testing.T) {
			if !store.Available(t) {
				skip.IgnoreLintf(
					t, "backup store URI for cloud %s is not available, skipping tests with this store", store.Cloud(),
				)
			}
			uri := store.URI(t, sql)
			backupURI := uri.String()
			t.Logf("using backup store URI: %s", backupURI)

			// Cloud storage operations are slower due to network latency,
			// so use a longer timeout than the default SucceedsSoon duration.
			successTimeout := testutils.DefaultSucceedsSoonDuration
			if store.Cloud() != "nodelocal" {
				successTimeout = 90 * time.Second
			}

			nextRow := 1
			writeRows := func(t *testing.T, sql *sqlutils.SQLRunner) {
				for i := 0; i < 10; i++ {
					sql.Exec(t, fmt.Sprintf(`INSERT INTO testdb.test_table VALUES (%d, 'row_%d', %d)`, nextRow, nextRow, nextRow*10))
					nextRow++
				}
			}

			sql.Exec(t, `CREATE DATABASE testdb`)
			defer sql.Exec(t, `DROP DATABASE IF EXISTS testdb CASCADE`)
			sql.Exec(t, `CREATE TABLE testdb.test_table (id INT PRIMARY KEY, name STRING, value INT)`)

			writeRows(t, sql)
			testutils.SucceedsWithin(t, func() error {
				_, err := sqlDB.DB.ExecContext(ctx, `BACKUP TABLE testdb.test_table INTO $1`, backupURI)
				return err
			}, successTimeout)

			for i := 0; i < 10; i++ {
				writeRows(t, sql)
				testutils.SucceedsWithin(t, func() error {
					_, err := sqlDB.DB.ExecContext(
						ctx, `BACKUP TABLE testdb.test_table INTO LATEST IN $1`, backupURI,
					)
					return err
				}, successTimeout)
			}

			originalFingerprint := sql.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)

			sqlDB.Exec(t, `DROP TABLE testdb.test_table`)

			testutils.SucceedsWithin(t, func() error {
				_, err := sqlDB.DB.ExecContext(
					ctx, `RESTORE TABLE testdb.test_table FROM LATEST IN $1`, backupURI,
				)
				return err
			}, successTimeout)

			restoredFingerprint := sqlDB.QueryStr(t, `SHOW EXPERIMENTAL_FINGERPRINTS FROM TABLE testdb.test_table`)
			require.Equal(t, originalFingerprint, restoredFingerprint, "fingerprints should match")
		})
	}
}

type backupStoreURI interface {
	// Cloud returns the cloud provider associated with this backup store URI.
	Cloud() string
	// Available returns true if the backup store URI is available for testing.
	Available(t *testing.T) bool
	// URI returns the backup store URI to be used in backup and restore operations.
	URI(t *testing.T, db *sqlutils.SQLRunner) url.URL
}

type gcpBackupStoreURI struct{}

func (g gcpBackupStoreURI) Cloud() string {
	return "gcp"
}

func (g gcpBackupStoreURI) Available(_ *testing.T) bool {
	return os.Getenv("GOOGLE_BUCKET") != ""
}

func (g gcpBackupStoreURI) URI(t *testing.T, _ *sqlutils.SQLRunner) url.URL {
	bucket := os.Getenv("GOOGLE_BUCKET")
	require.NotEmpty(t, bucket, "GOOGLE_BUCKET environment variable must be set for GCP backup store URI")
	uri := url.URL{Scheme: "gs", Host: bucket, Path: generateBucketPath(t)}
	values := uri.Query()
	values.Add(cloud.AuthParam, cloud.AuthParamImplicit)
	uri.RawQuery = values.Encode()
	return uri
}

type s3BackupStoreURI struct{}

func (s s3BackupStoreURI) Cloud() string {
	return "aws"
}

func (s s3BackupStoreURI) Available(t *testing.T) bool {
	t.Helper()
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err, "failed to load AWS credentials from environment")
	if !envConfig.Credentials.HasKeys() {
		return false
	}
	return os.Getenv("AWS_BUCKET") != ""
}

func (s s3BackupStoreURI) URI(t *testing.T, db *sqlutils.SQLRunner) url.URL {
	t.Helper()
	envConfig, err := config.NewEnvConfig()
	require.NoError(t, err, "failed to load AWS credentials from environment")
	require.True(t, envConfig.Credentials.HasKeys(), "AWS credentials must be set in environment for S3 backup store URI")
	bucket := os.Getenv("AWS_BUCKET")
	require.NotEmpty(t, bucket, "AWS_BUCKET environment variable must be set for S3 backup store URI")

	endpoint := os.Getenv(amazon.NightlyEnvVarS3Params[amazon.AWSEndpointParam])
	customCACert := os.Getenv("AWS_CUSTOM_CA_CERT")
	region := os.Getenv(amazon.NightlyEnvVarS3Params[amazon.S3RegionParam])
	if customCACert != "" {
		db.Exec(t, fmt.Sprintf("SET CLUSTER SETTING cloudstorage.http.custom_ca='%s'", customCACert))
	}

	uri := url.URL{Scheme: "s3", Host: bucket, Path: generateBucketPath(t)}
	values := uri.Query()
	values.Add(amazon.AWSAccessKeyParam, envConfig.Credentials.AccessKeyID)
	values.Add(amazon.AWSSecretParam, envConfig.Credentials.SecretAccessKey)
	if endpoint != "" {
		values.Add(amazon.AWSEndpointParam, endpoint)
	}
	if region != "" {
		values.Add(amazon.S3RegionParam, region)
	}
	uri.RawQuery = values.Encode()
	return uri
}

type azureBackupStoreURI struct{}

func (a azureBackupStoreURI) Cloud() string {
	return "azure"
}

func (a azureBackupStoreURI) Available(_ *testing.T) bool {
	return os.Getenv("AZURE_ACCOUNT_NAME") != "" &&
		os.Getenv("AZURE_CONTAINER") != "" &&
		os.Getenv("AZURE_CLIENT_ID") != "" &&
		os.Getenv("AZURE_CLIENT_SECRET") != "" &&
		os.Getenv("AZURE_TENANT_ID") != "" &&
		os.Getenv("AZURE_VAULT_NAME") != ""
}

func (a azureBackupStoreURI) URI(t *testing.T, _ *sqlutils.SQLRunner) url.URL {
	accountName := os.Getenv("AZURE_ACCOUNT_NAME")
	container := os.Getenv("AZURE_CONTAINER")
	clientID := os.Getenv("AZURE_CLIENT_ID")
	clientSecret := os.Getenv("AZURE_CLIENT_SECRET")
	tenantID := os.Getenv("AZURE_TENANT_ID")

	require.NotEmpty(t, accountName, "AZURE_ACCOUNT_NAME environment variable must be set for Azure backup store URI")
	require.NotEmpty(t, container, "AZURE_CONTAINER environment variable must be set for Azure backup store URI")
	require.NotEmpty(t, clientID, "AZURE_CLIENT_ID environment variable must be set for Azure backup store URI")
	require.NotEmpty(t, clientSecret, "AZURE_CLIENT_SECRET environment variable must be set for Azure backup store URI")
	require.NotEmpty(t, tenantID, "AZURE_TENANT_ID environment variable must be set for Azure backup store URI")

	uri := url.URL{Scheme: "azure", Host: container, Path: generateBucketPath(t)}
	values := uri.Query()
	values.Add(azure.AzureAccountNameParam, accountName)
	values.Add(azure.AzureClientIDParam, clientID)
	values.Add(azure.AzureClientSecretParam, clientSecret)
	values.Add(azure.AzureTenantIDParam, tenantID)
	uri.RawQuery = values.Encode()
	return uri
}

type nodelocalBackupStoreURI struct{}

func (n nodelocalBackupStoreURI) Cloud() string {
	return "nodelocal"
}

func (n nodelocalBackupStoreURI) Available(_ *testing.T) bool {
	return true
}

func (n nodelocalBackupStoreURI) URI(t *testing.T, _ *sqlutils.SQLRunner) url.URL {
	t.Helper()
	uri := url.URL{Scheme: "nodelocal", Host: "1", Path: generateBucketPath(t)}
	return uri
}

func generateBucketPath(t *testing.T) string {
	return fmt.Sprintf(
		"%s-%d",
		url.PathEscape(t.Name()),
		timeutil.Now().UnixNano(),
	)
}
