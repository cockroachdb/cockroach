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
	gosql "database/sql"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/testutils"
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
	aurl := s.AdminURL()
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
	_, err = db.Exec("CREATE TENANT hello; ALTER TENANT hello START SERVICE SHARED")
	require.NoError(t, err)

	// Get a SQL connection to the test tenant.
	sqlAddr := s.ServingSQLAddr()
	var db2 *gosql.DB
	testutils.SucceedsSoon(t, func() error {
		var err error
		db2, err = serverutils.OpenDBConnE(sqlAddr, "cluster:hello/defaultdb", false, s.Stopper())
		// Expect no error yet: the connection is opened lazily; an
		// error here means the parameters were incorrect.
		require.NoError(t, err)

		// This actually uses the connection.
		return db2.Ping()
	})

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
		req.Header.Set("Content-Type", httputil.ProtoContentType)
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

	newreq := func() *http.Request {
		req, err := http.NewRequest("GET", aurl+"/_status/sessions", nil)
		require.NoError(t, err)
		return req
	}

	// Retrieve the session list for the system tenant.
	req := newreq()
	req.Header.Set(server.TenantSelectHeader, catconstants.SystemTenantName)
	body, err := get(req)
	require.NoError(t, err)
	t.Logf("response 1:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello system")

	// Ditto for the test tenant.
	req = newreq()
	req.Header.Set(server.TenantSelectHeader, "hello")
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("response 2:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello hello")

	c := &http.Cookie{
		Name:     server.TenantSelectCookieName,
		Value:    catconstants.SystemTenantName,
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
	}
	purl, err := url.Parse(aurl)
	require.NoError(t, err)
	client.Jar.SetCookies(purl, []*http.Cookie{c})

	req = newreq()
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("response 3:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello system")

	c.Value = "hello"
	client.Jar.SetCookies(purl, []*http.Cookie{c})
	req = newreq()
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("response 4:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello hello")

	// Finally, do it again with both cookie and header. Verify
	// that the header wins.
	req = newreq()
	req.Header.Set(server.TenantSelectHeader, catconstants.SystemTenantName)
	body, err = get(req)
	require.NoError(t, err)
	t.Logf("response 5:\n%#v", body)
	require.Equal(t, len(body.Sessions), 1)
	require.Equal(t, body.Sessions[0].ApplicationName, "hello system")
}

func TestServerStartStop(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		DisableDefaultTestTenant: true,
	})
	defer s.Stopper().Stop(ctx)

	sqlAddr := s.ServingSQLAddr()

	// Create our own test tenant with a known name.
	_, err := db.Exec("CREATE TENANT hello")
	require.NoError(t, err)

	// Make the service alive.
	_, err = db.Exec("ALTER TENANT hello START SERVICE SHARED")
	require.NoError(t, err)

	// Check the liveness.
	testutils.SucceedsSoon(t, func() error {
		db2, err := serverutils.OpenDBConnE(sqlAddr, "cluster:hello/defaultdb", false, s.Stopper())
		// Expect no error yet: the connection is opened lazily; an
		// error here means the parameters were incorrect.
		require.NoError(t, err)

		defer db2.Close()
		if err := db2.Ping(); err != nil {
			return err
		}
		return nil
	})

	// Stop the service.     .
	_, err = db.Exec("ALTER TENANT hello STOP SERVICE")
	require.NoError(t, err)

	// Verify that the service is indeed stopped.
	testutils.SucceedsSoon(t, func() error {
		db2, err := serverutils.OpenDBConnE(sqlAddr, "cluster:hello/defaultdb", false, s.Stopper())
		// Expect no error yet: the connection is opened lazily; an
		// error here means the parameters were incorrect.
		require.NoError(t, err)
		defer db2.Close()
		if err := db2.Ping(); err != nil {
			// Connection error: success.
			return nil
		}
		return errors.New("server still alive")
	})
}
