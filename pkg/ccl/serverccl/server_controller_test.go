// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package serverccl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestServerControllerHTTP(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
	})
	defer s.Stopper().Stop(ctx)

	// Retrieve a privileged HTTP client. NB: this also populates
	// system.web_sessions.
	url := s.AdminURL()
	client, err := s.GetAdminHTTPClient()
	require.NoError(t, err)

	// Now retrieve the entry in the system tenant's web sessions.
	row := db.QueryRow(`SELECT id,"hashedSecret",username,"createdAt","expiresAt" FROM system.web_sessions`)
	var id int64
	var secret string
	var username string
	var created, expires time.Time
	require.NoError(t, row.Scan(&id, &secret, &username, &created, &expires))

	// Create our own test tenant with a known name.
	_, err = db.Exec("SELECT crdb_internal.create_tenant(10, 'hello')")
	require.NoError(t, err)

	// Get a SQL connection to the test tenant.
	sqlAddr := s.(*server.TestServer).TestingGetSQLAddrForTenant("hello")
	db2 := serverutils.OpenDBConn(t, sqlAddr, "defaultdb", false, s.Stopper())

	// Instantiate the HTTP test username and privileges into the test tenant.
	_, err = db2.Exec(fmt.Sprintf(`CREATE USER %s`, lexbase.EscapeSQLIdent(username)))
	require.NoError(t, err)
	_, err = db2.Exec(fmt.Sprintf(`GRANT admin TO %s`, lexbase.EscapeSQLIdent(username)))
	require.NoError(t, err)

	// Copy the session entry to the test tenant.
	_, err = db2.Exec(`INSERT INTO system.web_sessions(id, "hashedSecret", username, "createdAt", "expiresAt")
VALUES($1, $2, $3, $4, $5)`, id, secret, username, created, expires)
	require.NoError(t, err)

	// From this point, we are expecting the ability to access both tenants using
	// the same cookie jar.
	// Let's assert this is true by retrieving session lists, asserting
	// they are different and that each of them contains the appropriate entries.

	// Make our session to the system tenant recognizable in session lists.
	_, err = db.Exec("SET application_name = 'hello system'")
	require.NoError(t, err)

	// Ditto for the test tenant.
	_, err = db2.Exec("SET application_name = 'hello hello'")
	require.NoError(t, err)

	get := func(req *http.Request) (*serverpb.ListSessionsResponse, error) {
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, errors.Newf("request failed: %v", resp.StatusCode)
		}
		if resp.StatusCode != http.StatusOK {
			return nil, errors.Newf("request failed: %v / %q", resp.StatusCode, string(body))
		}
		var ls serverpb.ListSessionsResponse
		if err := protoutil.Unmarshal(body, &ls); err != nil {
			return nil, err
		}
		return &ls, err
	}

	req, err := http.NewRequest("GET", url+"/_status/sessions", nil)
	require.NoError(t, err)
	req.Header.Set("Content-Type", httputil.ProtoContentType)

	// Retrieve the session list for the system tenant.
	req.Header.Set(server.TenantSelectHeader, catconstants.SystemTenantName)
	body, err := get(req)
	require.NoError(t, err)
	t.Logf("first response:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello system")

	// Ditto for the test tenant.
	req.Header.Set(server.TenantSelectHeader, "hello")
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("second response:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello hello")
}
