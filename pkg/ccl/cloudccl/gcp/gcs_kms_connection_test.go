// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package gcp_test

import (
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"

	kms "cloud.google.com/go/kms/apiv1"
	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudtestutils"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/externalconn/providers" // import External Connection providers.
	"github.com/cockroachdb/cockroach/pkg/cloud/gcp"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2/google"
)

func TestGCSKMSExternalConnection(t *testing.T) {
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

	// The KeyID for GCS is the following format:
	// projects/{project name}/locations/{key region}/keyRings/{keyring name}/cryptoKeys/{key name}
	//
	// Get GCS Key identifier from env variable.
	keyID := os.Getenv("GOOGLE_KMS_KEY_NAME")
	if keyID == "" {
		skip.IgnoreLint(t, "GOOGLE_KMS_KEY_NAME env var must be set")
	}

	// Create an external connection where we will write the backup.
	backupURI := "nodelocal://1/backup"
	backupExternalConnectionName := "backup"
	createExternalConnection(backupExternalConnectionName, backupURI)

	t.Run("auth-implicit", func(t *testing.T) {
		if !cloudtestutils.IsImplicitAuthConfigured() {
			skip.IgnoreLint(t, "implicit auth is not configured")
		}

		// Set the AUTH to implicit.
		params := make(url.Values)
		params.Add(cloud.AuthParam, cloud.AuthParamImplicit)

		kmsURI := fmt.Sprintf("gs:///%s?%s", keyID, params.Encode())
		createExternalConnection("auth-implicit-kms", kmsURI)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName, "auth-implicit-kms")
	})

	t.Run("auth-specified", func(t *testing.T) {
		// Fetch the base64 encoded JSON credentials.
		credentials := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		if credentials == "" {
			skip.IgnoreLint(t, "GOOGLE_CREDENTIALS_JSON env var must be set")
		}
		encoded := base64.StdEncoding.EncodeToString([]byte(credentials))
		q := make(url.Values)
		q.Set(gcp.CredentialsParam, url.QueryEscape(encoded))

		// Set AUTH to specified.
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)

		kmsURI := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		createExternalConnection("auth-specified-kms", kmsURI)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName, "auth-specified-kms")
	})

	t.Run("auth-specified-bearer-token", func(t *testing.T) {
		// Fetch the base64 encoded JSON credentials.
		credentials := os.Getenv("GOOGLE_CREDENTIALS_JSON")
		if credentials == "" {
			skip.IgnoreLint(t, "GOOGLE_CREDENTIALS_JSON env var must be set")
		}

		ctx := context.Background()
		q := make(url.Values)
		source, err := google.JWTConfigFromJSON([]byte(credentials), kms.DefaultAuthScopes()...)
		require.NoError(t, err, "creating GCS oauth token source from specified credentials")
		ts := source.TokenSource(ctx)

		token, err := ts.Token()
		require.NoError(t, err, "getting token")
		q.Set(gcp.BearerTokenParam, token.AccessToken)

		// Set AUTH to specified.
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)

		kmsURI := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		createExternalConnection("auth-specified-bearer-token", kmsURI)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName,
			"auth-specified-bearer-token")
	})

	t.Run("kms-uses-incorrect-external-connection-type", func(t *testing.T) {
		// Point the KMS to the External Connection object that represents an
		// ExternalStorage. This should be disallowed.
		backupExternalConnectionURI := fmt.Sprintf("external://%s", backupExternalConnectionName)
		sqlDB.ExpectErr(t,
			"failed to load external connection object: expected External Connection object of type KMS but "+
				"'backup' is of type STORAGE",
			fmt.Sprintf(`BACKUP DATABASE foo INTO '%s' WITH kms='%s'`,
				backupExternalConnectionURI, backupExternalConnectionURI))
	})
}

func TestGCSExternalConnectionAssumeRole(t *testing.T) {
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
		fmt.Printf("created external connection %s\n\n", externalConnectionName)
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
	disallowedBackupToExternalConnection := func(backupExternalConnectionName, kmsExternalConnectionName string) {
		backupURI := fmt.Sprintf("external://%s", backupExternalConnectionName)
		kmsURI := fmt.Sprintf("external://%s", kmsExternalConnectionName)
		fmt.Printf("backing up into %s with kms %s\n\n", backupURI, kmsURI)
		sqlDB.ExpectErr(t, "(PermissionDenied|AccessDenied|PERMISSION_DENIED)",
			fmt.Sprintf(`BACKUP INTO '%s' WITH kms='%s'`, backupURI, kmsURI))
	}

	envVars := []string{
		"GOOGLE_CREDENTIALS_JSON",
		"GOOGLE_APPLICATION_CREDENTIALS",
		"ASSUME_SERVICE_ACCOUNT",
		"GOOGLE_LIMITED_KEY_ID",
	}
	for _, env := range envVars {
		v := os.Getenv(env)
		if v == "" {
			skip.IgnoreLintf(t, "%s env var must be set", env)
		}
	}

	keyID := os.Getenv("GOOGLE_LIMITED_KEY_ID")
	assumedAccount := os.Getenv("ASSUME_SERVICE_ACCOUNT")
	encodedCredentials := base64.StdEncoding.EncodeToString([]byte(os.Getenv("GOOGLE_CREDENTIALS_JSON")))

	// Create an external connection where we will write the backup.
	backupURI := "nodelocal://1/backup"
	backupExternalConnectionName := "backup"
	createExternalConnection(backupExternalConnectionName, backupURI)

	t.Run("auth-assume-role-implicit", func(t *testing.T) {
		disallowedKMSURI := fmt.Sprintf("gs:///%s?%s=%s", keyID, cloud.AuthParam, cloud.AuthParamImplicit)
		disallowedECName := "auth-assume-role-implicit-disallowed"
		createExternalConnection(disallowedECName, disallowedKMSURI)
		disallowedBackupToExternalConnection(backupExternalConnectionName, disallowedECName)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamImplicit)
		q.Set(gcp.AssumeRoleParam, assumedAccount)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		createExternalConnection("auth-assume-role-implicit", uri)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName, "auth-assume-role-implicit")
	})

	t.Run("auth-assume-role-specified", func(t *testing.T) {
		disallowedKMSURI := fmt.Sprintf("gs:///%s?%s=%s&%s=%s", keyID, cloud.AuthParam,
			cloud.AuthParamSpecified, gcp.CredentialsParam, url.QueryEscape(encodedCredentials))
		disallowedECName := "auth-assume-role-specified-disallowed"
		createExternalConnection(disallowedECName, disallowedKMSURI)
		disallowedBackupToExternalConnection(backupExternalConnectionName, disallowedECName)

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
		q.Set(gcp.AssumeRoleParam, assumedAccount)
		q.Set(gcp.CredentialsParam, encodedCredentials)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		createExternalConnection("auth-assume-role-specified", uri)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName, "auth-assume-role-specified")
	})

	t.Run("auth-assume-role-chaining", func(t *testing.T) {
		roleChainStr := os.Getenv("ASSUME_SERVICE_ACCOUNT_CHAIN")
		if roleChainStr == "" {
			skip.IgnoreLint(t, "ASSUME_SERVICE_ACCOUNT_CHAIN env var must be set")
		}
		roleChain := strings.Split(roleChainStr, ",")

		q := make(url.Values)
		q.Set(cloud.AuthParam, cloud.AuthParamSpecified)
		q.Set(gcp.CredentialsParam, encodedCredentials)

		// First verify that none of the individual roles in the chain can be used
		// to access the KMS.
		for i, role := range roleChain {
			i := i
			q.Set(gcp.AssumeRoleParam, role)
			disallowedKMSURI := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
			disallowedECName := fmt.Sprintf("auth-assume-role-chaining-disallowed-%d", i)
			createExternalConnection(disallowedECName, disallowedKMSURI)
			disallowedBackupToExternalConnection(backupExternalConnectionName, disallowedECName)
		}

		q.Set(gcp.AssumeRoleParam, roleChainStr)
		uri := fmt.Sprintf("gs:///%s?%s", keyID, q.Encode())
		createExternalConnection("auth-assume-role-chaining", uri)
		backupAndRestoreFromExternalConnection(backupExternalConnectionName, "auth-assume-role-chaining")
	})
}
