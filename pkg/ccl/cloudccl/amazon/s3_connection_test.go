// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package amazon

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/externalconn/providers" // import External Connection providers.
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestS3ExternalConnection(t *testing.T) {
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

	// If environment credentials are not present, we want to
	// skip all S3 tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	creds, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		skip.IgnoreLint(t, "No AWS credentials")
	}
	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	t.Run("auth-implicit", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLintf(t, "we only run this test if a default role exists, "+
				"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html: %s", err)
		}

		// Set the AUTH to implicit.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamImplicit)

		s3URI := fmt.Sprintf("s3://%s/backup-ec-test-default?%s", bucket, params.Encode())
		ecName := "auth-implicit-s3"
		createExternalConnection(ecName, s3URI)
		backupAndRestoreFromExternalConnection(ecName)
	})

	t.Run("auth-specified", func(t *testing.T) {
		s3URI := amazon.S3URI(bucket, "backup-ec-test-default",
			&cloudpb.ExternalStorage_S3{
				AccessKey: creds.AccessKeyID,
				Secret:    creds.SecretAccessKey,
				Region:    "us-east-1",
				Auth:      cloud.AuthParamSpecified,
			},
		)
		ecName := "auth-specified-s3"
		createExternalConnection(ecName, s3URI)
		backupAndRestoreFromExternalConnection(ecName)
	})

	// Tests that we can put an object with server side encryption specified.
	t.Run("server-side-encryption", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLintf(t, "we only run this test if a default role exists, "+
				"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html: %s", err)
		}

		s3URI := amazon.S3URI(bucket, "backup-ec-test-sse-256", &cloudpb.ExternalStorage_S3{
			Region:        "us-east-1",
			Auth:          cloud.AuthParamImplicit,
			ServerEncMode: "AES256",
		})
		ecName := "server-side-encryption-s3"
		createExternalConnection(ecName, s3URI)
		backupAndRestoreFromExternalConnection(ecName)

		v := os.Getenv("AWS_KMS_KEY_ARN")
		if v == "" {
			skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
		}
		s3KMSURI := amazon.S3URI(bucket, "backup-ec-test-sse-kms", &cloudpb.ExternalStorage_S3{
			Region:        "us-east-1",
			Auth:          cloud.AuthParamImplicit,
			ServerEncMode: "aws:kms",
			ServerKMSID:   v,
		})
		ecName = "server-side-encryption-kms-s3"
		createExternalConnection(ecName, s3KMSURI)
		backupAndRestoreFromExternalConnection(ecName)
	})

	t.Run("server-side-encryption-invalid-params", func(t *testing.T) {
		// You can create an IAM that can access S3
		// in the AWS console, then set it up locally.
		// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLintf(t, "we only run this test if a default role exists, "+
				"refer to https://docs.aws.com/cli/latest/userguide/cli-configure-role.html: %s", err)
		}

		// Unsupported server side encryption option.
		invalidS3URI := amazon.S3URI(bucket, "backup-ec-test-sse-256", &cloudpb.ExternalStorage_S3{
			Region:        "us-east-1",
			Auth:          cloud.AuthParamImplicit,
			ServerEncMode: "unsupported-algorithm",
		})
		sqlDB.ExpectErr(t,
			"unsupported server encryption mode unsupported-algorithm. Supported values are `aws:kms` and `AES256",
			fmt.Sprintf(`BACKUP DATABASE foo INTO '%s'`, invalidS3URI))

		invalidS3URI = amazon.S3URI(bucket, "backup-ec-test-sse-256", &cloudpb.ExternalStorage_S3{
			Region:        "us-east-1",
			Auth:          cloud.AuthParamImplicit,
			ServerEncMode: "aws:kms",
		})

		// Specify aws:kms encryption mode but don't specify kms ID.
		sqlDB.ExpectErr(t, "AWS_SERVER_KMS_ID param must be set when using aws:kms server side encryption mode.", fmt.Sprintf(`BACKUP DATABASE foo INTO '%s'`,
			invalidS3URI))
	})
}
