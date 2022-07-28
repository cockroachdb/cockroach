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
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
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
	keyID := envutil.EnvOrDefaultString("GOOGLE_KMS_KEY_NAME", "")
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
		credentials := envutil.EnvOrDefaultString("GOOGLE_CREDENTIALS_JSON", "")
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
		credentials := envutil.EnvOrDefaultString("GOOGLE_CREDENTIALS_JSON", "")
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
}
