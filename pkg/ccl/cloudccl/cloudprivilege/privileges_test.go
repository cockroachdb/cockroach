// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cloudprivilege

import (
	"context"
	gosql "database/sql"
	"fmt"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	_ "github.com/cockroachdb/cockroach/pkg/ccl" // import CCL to run backup and restore
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestURIRequiresAdminRole tests the logic that guards certain privileged
// ExternalStorage IO paths with an admin only or EXTERNALIOIMPLICITACCESS
// privilege checks.
func TestURIRequiresAdminOrPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const nodes = 1

	ctx := context.Background()
	tc := serverutils.StartNewTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{
		SQLMemoryPoolSize: 256 << 20,
	}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.ServerConn(0)
	rootDB := sqlutils.MakeSQLRunner(conn)

	rootDB.Exec(t, `CREATE USER testuser`)
	pgURL, cleanupFunc := sqlutils.PGUrl(
		t, tc.Server(0).ServingSQLAddr(), "TestURIRequiresAdminRole-testuser",
		url.User("testuser"),
	)
	defer cleanupFunc()
	testuser, err := gosql.Open("postgres", pgURL.String())
	require.NoError(t, err)
	defer testuser.Close()
	rootDB.Exec(t, `CREATE TABLE foo (id INT)`)

	// Grant SELECT so that EXPORT fails when checking URI privileges.
	rootDB.Exec(t, `GRANT SELECT ON TABLE foo TO testuser`)

	for _, tc := range []struct {
		name                   string
		uri                    string
		isAPrivilegedOperation bool
	}{
		{
			name:                   "s3-implicit",
			uri:                    "s3://foo/bar?AUTH=implicit",
			isAPrivilegedOperation: true,
		},
		{
			name:                   "s3-specified",
			uri:                    "s3://foo/bar?AUTH=specified&AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456",
			isAPrivilegedOperation: false,
		},
		{
			name:                   "s3-custom",
			uri:                    "s3://foo/bar?AUTH=specified&AWS_ACCESS_KEY_ID=123&AWS_SECRET_ACCESS_KEY=456&AWS_ENDPOINT=baz",
			isAPrivilegedOperation: true,
		},
		{
			name:                   "gs-implicit",
			uri:                    "gs://foo/bar?AUTH=implicit",
			isAPrivilegedOperation: true,
		},
		{
			name:                   "gs-specified",
			uri:                    "gs://foo/bar?AUTH=specified",
			isAPrivilegedOperation: false,
		},
		{
			name:                   "userfile",
			uri:                    "userfile:///foo",
			isAPrivilegedOperation: false,
		},
		{
			name:                   "nodelocal",
			uri:                    "nodelocal://self/foo",
			isAPrivilegedOperation: true,
		},
		{
			name:                   "http",
			uri:                    "http://foo/bar",
			isAPrivilegedOperation: true,
		},
		{
			name:                   "https",
			uri:                    "https://foo/bar",
			isAPrivilegedOperation: true,
		},
		{
			name:                   "external",
			uri:                    "external://foo/bar",
			isAPrivilegedOperation: false,
		},
		{
			name:                   "azure-legacy",
			uri:                    "azure://foo/bar?AZURE_ACCOUNT_NAME=random&AZURE_ACCOUNT_KEY=random",
			isAPrivilegedOperation: false,
		},
		{
			name:                   "azure-specified",
			uri:                    "azure://foo/bar?AUTH=specified&AZURE_ACCOUNT_NAME=random&AZURE_CLIENT_ID=id&AZURE_CLIENT_SECRET=sec&AZURE_TENANT_ID=ten",
			isAPrivilegedOperation: false,
		},
		{
			name:                   "azure-implicit",
			uri:                    "azure://foo/bar?AUTH=implicit&AZURE_ACCOUNT_NAME=random",
			isAPrivilegedOperation: true,
		},
	} {
		t.Run(tc.name+"-via-import", func(t *testing.T) {
			_, err := testuser.Exec(fmt.Sprintf(`IMPORT INTO foo CSV DATA ('%s')`, tc.uri))
			if tc.isAPrivilegedOperation {
				require.True(t, testutils.IsError(err, "only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access"))
			} else {
				require.False(t, testutils.IsError(err, "only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access"))
			}
		})

		t.Run(tc.name+"-via-export", func(t *testing.T) {
			_, err := testuser.Exec(fmt.Sprintf(`EXPORT INTO CSV '%s' FROM TABLE foo`, tc.uri))
			if tc.isAPrivilegedOperation {
				require.True(t, testutils.IsError(err, "only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access"))
			} else {
				require.False(t, testutils.IsError(err, "only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access"))
			}
		})

		t.Run(tc.name+"-via-backup", func(t *testing.T) {
			_, err := testuser.Exec(fmt.Sprintf(`BACKUP TABLE foo INTO '%s'`, tc.uri))
			if tc.isAPrivilegedOperation {
				require.True(t, testutils.IsError(err, "only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access"))
			} else {
				require.False(t, testutils.IsError(err, "only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access"))
			}
		})

		t.Run(tc.name+"-via-restore", func(t *testing.T) {
			_, err := testuser.Exec(fmt.Sprintf(`RESTORE TABLE foo FROM LATEST IN '%s'`, tc.uri))
			if tc.isAPrivilegedOperation {
				require.True(t, testutils.IsError(err, "only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access"))
			} else {
				require.False(t, testutils.IsError(err, "only users with the admin role or the EXTERNALIOIMPLICITACCESS system privilege are allowed to access"))
			}
		})

		t.Run(tc.name+"-direct", func(t *testing.T) {
			conf, err := cloud.ExternalStorageConfFromURI(tc.uri, username.RootUserName())
			require.NoError(t, err)
			require.Equal(t, !tc.isAPrivilegedOperation, conf.AccessIsWithExplicitAuth())
		})
	}
}
