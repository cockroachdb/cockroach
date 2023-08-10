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
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/amazon"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
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

	testID := cloudtestutils.NewTestID()

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

		s3URI := fmt.Sprintf("s3://%s/backup-ec-test-default-%d?%s", bucket, testID, params.Encode())
		ecName := "auth-implicit-s3"
		createExternalConnection(ecName, s3URI)
		backupAndRestoreFromExternalConnection(ecName)
	})

	t.Run("auth-specified", func(t *testing.T) {
		s3URI := amazon.S3URI(bucket, fmt.Sprintf("backup-ec-test-default-%d", testID),
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

		s3URI := amazon.S3URI(bucket, fmt.Sprintf("backup-ec-test-sse-256-%d", testID), &cloudpb.ExternalStorage_S3{
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
		s3KMSURI := amazon.S3URI(bucket, fmt.Sprintf("backup-ec-test-sse-kms-%d", testID), &cloudpb.ExternalStorage_S3{
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
		invalidS3URI := amazon.S3URI(bucket, fmt.Sprintf("backup-ec-test-sse-256-%d", testID), &cloudpb.ExternalStorage_S3{
			Region:        "us-east-1",
			Auth:          cloud.AuthParamImplicit,
			ServerEncMode: "unsupported-algorithm",
		})
		sqlDB.ExpectErr(t,
			"unsupported server encryption mode unsupported-algorithm. Supported values are `aws:kms` and `AES256",
			fmt.Sprintf(`BACKUP DATABASE foo INTO '%s'`, invalidS3URI))

		invalidS3URI = amazon.S3URI(bucket, fmt.Sprintf("backup-ec-test-sse-256-%d", testID), &cloudpb.ExternalStorage_S3{
			Region:        "us-east-1",
			Auth:          cloud.AuthParamImplicit,
			ServerEncMode: "aws:kms",
		})

		// Specify aws:kms encryption mode but don't specify kms ID.
		sqlDB.ExpectErr(t, "AWS_SERVER_KMS_ID param must be set when using aws:kms server side encryption mode.", fmt.Sprintf(`BACKUP DATABASE foo INTO '%s'`,
			invalidS3URI))
	})
}

func TestAWSKMSExternalConnection(t *testing.T) {
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
	backupAndRestoreFromExternalConnection := func(backupExternalConnectionName, kmsExternalConnectionName string) {
		backupURI := fmt.Sprintf("external://%s", backupExternalConnectionName)
		kmsURI := fmt.Sprintf("external://%s", kmsExternalConnectionName)
		sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE foo INTO '%s' WITH kms='%s'`, backupURI, kmsURI))
		sqlDB.Exec(t, fmt.Sprintf(`RESTORE DATABASE foo FROM LATEST IN '%s' WITH new_db_name = bar, kms='%s'`, backupURI, kmsURI))
		sqlDB.CheckQueryResults(t, `SELECT * FROM bar.foo`, [][]string{{"1"}, {"2"}, {"3"}})
		sqlDB.CheckQueryResults(t, `SELECT * FROM crdb_internal.invalid_objects`, [][]string{})
		sqlDB.Exec(t, `DROP DATABASE bar CASCADE`)
	}

	// If environment credentials are not present, we want to
	// skip all AWS KMS tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	_, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		skip.IgnoreLint(t, "Test only works with AWS credentials")
	}

	q := make(url.Values)
	expect := map[string]string{
		"AWS_ACCESS_KEY_ID":     amazon.AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": amazon.AWSSecretParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}
	// Get AWS KMS region from env variable.
	kmsRegion := os.Getenv("AWS_KMS_REGION")
	if kmsRegion == "" {
		skip.IgnoreLint(t, "AWS_KMS_REGION env var must be set")
	}
	q.Add(amazon.KMSRegionParam, kmsRegion)

	// Get AWS Key identifier from env variable.
	keyID := os.Getenv("AWS_KMS_KEY_ARN")
	if keyID == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	testID := cloudtestutils.NewTestID()
	// Create an external connection where we will write the backup.
	backupURI := fmt.Sprintf("s3://%s/backup-%d?%s=%s", bucket, testID,
		cloud.AuthParam, cloud.AuthParamImplicit)
	backupExternalConnectionName := "backup"
	createExternalConnection(backupExternalConnectionName, backupURI)

	t.Run("auth-implicit", func(t *testing.T) {
		// Set the AUTH to implicit.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamImplicit)
		params.Add(amazon.KMSRegionParam, kmsRegion)

		kmsURI := fmt.Sprintf("aws-kms:///%s?%s", keyID, params.Encode())
		createExternalConnection("auth-implicit-kms", kmsURI)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName,
			"auth-implicit-kms")
	})

	t.Run("auth-specified", func(t *testing.T) {
		kmsURI := fmt.Sprintf("aws-kms:///%s?%s", keyID, q.Encode())
		createExternalConnection("auth-specified-kms", kmsURI)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName,
			"auth-specified-kms")
	})

	t.Run("kms-uses-incorrect-external-connection-type", func(t *testing.T) {
		// Point the KMS to the External Connection object that represents an
		// ExternalStorage. This should be disallowed.
		backupExternalConnectionURI := fmt.Sprintf("external://%s", backupExternalConnectionName)
		sqlDB.ExpectErr(t,
			"KMS cannot use object of type STORAGE",
			fmt.Sprintf(`BACKUP DATABASE foo INTO '%s' WITH kms='%s'`,
				backupExternalConnectionURI, backupExternalConnectionURI))
	})
}

func TestAWSKMSExternalConnectionAssumeRole(t *testing.T) {
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
	disallowedCreateExternalConnection := func(externalConnectionName, uri string) {
		sqlDB.ExpectErr(t, "(PermissionDenied|AccessDenied|PERMISSION_DENIED)",
			fmt.Sprintf(`CREATE EXTERNAL CONNECTION '%s' AS '%s'`, externalConnectionName, uri))
	}
	backupAndRestoreFromExternalConnection := func(backupExternalConnectionName, kmsExternalConnectionName string) {
		backupURI := fmt.Sprintf("external://%s", backupExternalConnectionName)
		kmsURI := fmt.Sprintf("external://%s", kmsExternalConnectionName)
		sqlDB.Exec(t, fmt.Sprintf(`BACKUP DATABASE foo INTO '%s' WITH kms='%s'`, backupURI, kmsURI))
		sqlDB.Exec(t, fmt.Sprintf(`RESTORE DATABASE foo FROM LATEST IN '%s' WITH new_db_name = bar, kms='%s'`, backupURI, kmsURI))
		sqlDB.CheckQueryResults(t, `SELECT * FROM bar.foo`, [][]string{{"1"}, {"2"}, {"3"}})
		sqlDB.CheckQueryResults(t, `SELECT * FROM crdb_internal.invalid_objects`, [][]string{})
		sqlDB.Exec(t, `DROP DATABASE bar CASCADE`)
	}

	// If environment credentials are not present, we want to
	// skip all AWS KMS tests, including auth-implicit, even though
	// it is not used in auth-implicit.
	_, err := credentials.NewEnvCredentials().Get()
	if err != nil {
		skip.IgnoreLint(t, "Test only works with AWS credentials")
	}

	q := make(url.Values)
	expect := map[string]string{
		"AWS_ACCESS_KEY_ID":     amazon.AWSAccessKeyParam,
		"AWS_SECRET_ACCESS_KEY": amazon.AWSSecretParam,
		"AWS_ASSUME_ROLE":       amazon.AssumeRoleParam,
	}
	for env, param := range expect {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
		q.Add(param, v)
	}
	// Get AWS KMS region from env variable.
	kmsRegion := os.Getenv("AWS_KMS_REGION")
	if kmsRegion == "" {
		skip.IgnoreLint(t, "AWS_KMS_REGION env var must be set")
	}
	q.Add(amazon.KMSRegionParam, kmsRegion)
	q.Set(cloud.AuthParam, cloud.AuthParamSpecified)

	// Get AWS Key identifier from env variable.
	keyID := os.Getenv("AWS_KMS_KEY_ARN")
	if keyID == "" {
		skip.IgnoreLint(t, "AWS_KMS_KEY_ARN env var must be set")
	}

	bucket := os.Getenv("AWS_S3_BUCKET")
	if bucket == "" {
		skip.IgnoreLint(t, "AWS_S3_BUCKET env var must be set")
	}

	testID := cloudtestutils.NewTestID()
	// Create an external connection where we will write the backup.
	backupURI := fmt.Sprintf("s3://%s/backup-%d?%s=%s", bucket, testID,
		cloud.AuthParam, cloud.AuthParamImplicit)
	backupExternalConnectionName := "backup"
	createExternalConnection(backupExternalConnectionName, backupURI)

	t.Run("auth-assume-role-implicit", func(t *testing.T) {
		// You can create an IAM that can access AWS KMS
		// in the AWS console, then set it up locally.
		// https://docs.aws.com/cli/latest/userguide/cli-configure-role.html
		// We only run this test if default role exists.
		credentialsProvider := credentials.SharedCredentialsProvider{}
		_, err := credentialsProvider.Retrieve()
		if err != nil {
			skip.IgnoreLint(t, err)
		}

		// Create params for implicit user.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamImplicit)
		params.Add(amazon.AssumeRoleParam, q.Get(amazon.AssumeRoleParam))
		params.Add(amazon.KMSRegionParam, kmsRegion)

		uri := fmt.Sprintf("aws-kms:///%s?%s", keyID, params.Encode())
		createExternalConnection("auth-assume-role-implicit", uri)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName, "auth-assume-role-implicit")
	})

	t.Run("auth-assume-role-specified", func(t *testing.T) {
		uri := fmt.Sprintf("aws-kms:///%s?%s", keyID, q.Encode())
		createExternalConnection("auth-assume-role-specified", uri)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName, "auth-assume-role-specified")
	})

	t.Run("auth-assume-role-chaining", func(t *testing.T) {
		roleChainStr := os.Getenv("AWS_ROLE_ARN_CHAIN")
		roleChain := strings.Split(roleChainStr, ",")

		// First verify that none of the individual roles in the chain can be used
		// to access the KMS.
		for i, role := range roleChain {
			i := i
			q.Set(amazon.AssumeRoleParam, role)
			disallowedKMSURI := fmt.Sprintf("aws-kms:///%s?%s", keyID, q.Encode())
			disallowedECName := fmt.Sprintf("auth-assume-role-chaining-disallowed-%d", i)
			disallowedCreateExternalConnection(disallowedECName, disallowedKMSURI)
		}

		// Finally, check that the chain of roles can be used to access the KMS.
		q.Set(amazon.AssumeRoleParam, roleChainStr)
		uri := fmt.Sprintf("aws-kms:///%s?%s", keyID, q.Encode())
		createExternalConnection("auth-assume-role-chaining", uri)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName, "auth-assume-role-chaining")
	})
}
