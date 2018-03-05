// Copyright 2015 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	gosql "database/sql"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"testing"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type ctxI interface {
	GetHTTPClient() (http.Client, error)
	HTTPRequestScheme() string
}

var _ ctxI = insecureCtx{}
var _ ctxI = (*base.Config)(nil)

type insecureCtx struct{}

func (insecureCtx) GetHTTPClient() (http.Client, error) {
	return http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}, nil
}

func (insecureCtx) HTTPRequestScheme() string {
	return "https"
}

// Verify client certificate enforcement and user whitelisting.
func TestSSLEnforcement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		// This test is verifying the (unimplemented) authentication of SSL
		// client certificates over HTTP endpoints. Web session authentication
		// is disabled in order to avoid the need to authenticate the individual
		// clients being instantiated.
		DisableWebSessionAuthentication: true,
	})
	defer s.Stopper().Stop(context.TODO())

	// HTTPS with client certs for security.RootUser.
	rootCertsContext := testutils.NewTestBaseContext(security.RootUser)
	// HTTPS with client certs for security.NodeUser.
	nodeCertsContext := testutils.NewNodeTestBaseContext()
	// HTTPS with client certs for TestUser.
	testCertsContext := testutils.NewTestBaseContext(TestUser)
	// HTTPS without client certs. The user does not matter.
	noCertsContext := insecureCtx{}
	// Plain http.
	insecureContext := testutils.NewTestBaseContext(TestUser)
	insecureContext.Insecure = true

	kvGet := &roachpb.GetRequest{}
	kvGet.Key = roachpb.Key("/")

	for _, tc := range []struct {
		path string
		ctx  ctxI
		code int // http response code
	}{
		// Health endpoint is special-cased; allowed to serve on HTTP.
		{"/health", insecureContext, http.StatusOK},

		// /ui/: basic file server: no auth.
		{"", rootCertsContext, http.StatusOK},
		{"", nodeCertsContext, http.StatusOK},
		{"", testCertsContext, http.StatusOK},
		{"", noCertsContext, http.StatusOK},
		{"", insecureContext, http.StatusTemporaryRedirect},

		// /_admin/: server.adminServer: no auth.
		{adminPrefix + "health", rootCertsContext, http.StatusOK},
		{adminPrefix + "health", nodeCertsContext, http.StatusOK},
		{adminPrefix + "health", testCertsContext, http.StatusOK},
		{adminPrefix + "health", noCertsContext, http.StatusOK},
		{adminPrefix + "health", insecureContext, http.StatusTemporaryRedirect},

		// /debug/: server.adminServer: no auth.
		{debug.Endpoint + "vars", rootCertsContext, http.StatusOK},
		{debug.Endpoint + "vars", nodeCertsContext, http.StatusOK},
		{debug.Endpoint + "vars", testCertsContext, http.StatusOK},
		{debug.Endpoint + "vars", noCertsContext, http.StatusOK},
		{debug.Endpoint + "vars", insecureContext, http.StatusTemporaryRedirect},

		// /_status/nodes: server.statusServer: no auth.
		{statusPrefix + "nodes", rootCertsContext, http.StatusOK},
		{statusPrefix + "nodes", nodeCertsContext, http.StatusOK},
		{statusPrefix + "nodes", testCertsContext, http.StatusOK},
		{statusPrefix + "nodes", noCertsContext, http.StatusOK},
		{statusPrefix + "nodes", insecureContext, http.StatusTemporaryRedirect},

		// /ts/: ts.Server: no auth.
		{ts.URLPrefix, rootCertsContext, http.StatusNotFound},
		{ts.URLPrefix, nodeCertsContext, http.StatusNotFound},
		{ts.URLPrefix, testCertsContext, http.StatusNotFound},
		{ts.URLPrefix, noCertsContext, http.StatusNotFound},
		{ts.URLPrefix, insecureContext, http.StatusTemporaryRedirect},
	} {
		t.Run("", func(t *testing.T) {
			client, err := tc.ctx.GetHTTPClient()
			if err != nil {
				t.Fatal(err)
			}
			// Avoid automatically following redirects.
			client.CheckRedirect = func(*http.Request, []*http.Request) error {
				return http.ErrUseLastResponse
			}
			url := url.URL{
				Scheme: tc.ctx.HTTPRequestScheme(),
				Host:   s.(*TestServer).Cfg.HTTPAddr,
				Path:   tc.path,
			}
			resp, err := client.Get(url.String())
			if err != nil {
				t.Fatal(err)
			}

			defer resp.Body.Close()
			if resp.StatusCode != tc.code {
				t.Errorf("expected status code %d, got %d", tc.code, resp.StatusCode)
				u, err := resp.Location()
				t.Errorf("orig=%s url=%s err=%v", tc.path, u, err)
			}
		})
	}
}

func TestVerifyPassword(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	if util.RaceEnabled {
		// The default bcrypt cost makes this test approximately 30s slower when the
		// race detector is on.
		defer func(prev int) { security.BcryptCost = prev }(security.BcryptCost)
		security.BcryptCost = bcrypt.MinCost
	}

	for _, user := range []struct {
		username string
		password string
	}{
		{"azure_diamond", "hunter2"},
		{"druidia", "12345"},
	} {
		cmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", user.username, user.password)
		if _, err := db.Exec(cmd); err != nil {
			t.Fatalf("failed to create user: %s", err)
		}
	}

	for _, tc := range []struct {
		username           string
		password           string
		shouldAuthenticate bool
		expectedErrString  string
	}{
		{"azure_diamond", "hunter2", true, ""},
		{"azure_diamond", "hunter", false, "crypto/bcrypt"},
		{"azure_diamond", "", false, "crypto/bcrypt"},
		{"azure_diamond", "🍦", false, "crypto/bcrypt"},
		{"azure_diamond", "hunter2345", false, "crypto/bcrypt"},
		{"azure_diamond", "shunter2", false, "crypto/bcrypt"},
		{"azure_diamond", "12345", false, "crypto/bcrypt"},
		{"azure_diamond", "*******", false, "crypto/bcrypt"},
		{"druidia", "12345", true, ""},
		{"druidia", "hunter2", false, "crypto/bcrypt"},
		{"root", "", false, "crypto/bcrypt"},
		{"", "", false, "does not exist"},
		{"doesntexist", "zxcvbn", false, "does not exist"},
	} {
		t.Run("", func(t *testing.T) {
			valid, err := ts.authentication.verifyPassword(context.TODO(), tc.username, tc.password)
			if err != nil {
				t.Errorf(
					"credentials %s/%s failed with error %s, wanted no error",
					tc.username,
					tc.password,
					err,
				)
			}
			if valid != tc.shouldAuthenticate {
				t.Errorf(
					"credentials %s/%s valid = %t, wanted %t",
					tc.username,
					tc.password,
					valid,
					tc.shouldAuthenticate,
				)
			}
		})
	}
}

func TestCreateSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	username := "testUser"

	// Create an authentication, noting the time before and after creation. This
	// lets us ensure that the timestamps created are accurate.
	timeBoundBefore := ts.clock.PhysicalTime()
	id, origSecret, err := ts.authentication.newAuthSession(context.TODO(), username)
	if err != nil {
		t.Fatalf("error creating auth session: %s", err)
	}
	timeBoundAfter := ts.clock.PhysicalTime()

	// Query fields from created session.
	query := `
SELECT "hashedSecret", "username", "createdAt", "lastUsedAt", "expiresAt", "revokedAt", "auditInfo"
FROM system.web_sessions
WHERE id = $1`

	result := db.QueryRow(query, id)
	var (
		sessHashedSecret []byte
		sessUsername     string
		sessCreated      time.Time
		sessLastUsed     time.Time
		sessExpires      time.Time
		sessRevoked      pq.NullTime
		sessAuditInfo    gosql.NullString
	)
	if err := result.Scan(
		&sessHashedSecret,
		&sessUsername,
		&sessCreated,
		&sessLastUsed,
		&sessExpires,
		&sessRevoked,
		&sessAuditInfo,
	); err != nil {
		t.Fatalf("error querying created auth session: %s", err)
	}

	// Verify hashed secret matches original secret
	hasher := sha256.New()
	hashedSecret := hasher.Sum(origSecret)
	if !bytes.Equal(sessHashedSecret, hashedSecret) {
		t.Fatalf("hashed value of secret: \n%#v\ncomputed as: \n%#v\nwanted: \n%#v", origSecret, hashedSecret, sessHashedSecret)
	}

	// Username.
	if a, e := sessUsername, username; a != e {
		t.Fatalf("session username got %s, wanted %s", a, e)
	}

	// Timestamps.
	verifyTimestamp := func(actual time.Time, early time.Time, late time.Time) error {
		if actual.Before(early) {
			return errors.Errorf("time %s was before early bound %s", actual, early)
		}
		if late.Before(actual) {
			return errors.Errorf("time %s was after late bound %s", actual, late)
		}
		return nil
	}

	if err := verifyTimestamp(sessCreated, timeBoundBefore, timeBoundAfter); err != nil {
		t.Fatalf("bad createdAt timestamp: %s", err)
	}
	if err := verifyTimestamp(sessLastUsed, timeBoundBefore, timeBoundAfter); err != nil {
		t.Fatalf("bad lastUsedAt timestamp: %s", err)
	}
	timeout := webSessionTimeout.Get(&s.ClusterSettings().SV)
	if err := verifyTimestamp(
		sessExpires, timeBoundBefore.Add(timeout), timeBoundAfter.Add(timeout),
	); err != nil {
		t.Fatalf("bad expiresAt timestamp: %s", err)
	}

	// Null fields
	if sessRevoked.Valid {
		t.Fatalf("sess had revokedAt timestamp %s, wanted null", sessRevoked.Time)
	}
	if sessAuditInfo.Valid {
		t.Fatalf("sess had auditInfo %s, wanted null", sessAuditInfo.String)
	}
}

func TestVerifySession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)

	sessionUsername := "testUser"
	id, origSecret, err := ts.authentication.newAuthSession(context.TODO(), sessionUsername)
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		testname     string
		cookie       serverpb.SessionCookie
		shouldVerify bool
	}{
		{
			testname: "Valid cookie",
			cookie: serverpb.SessionCookie{
				ID:     id,
				Secret: origSecret,
			},
			shouldVerify: true,
		},
		{
			testname: "No secret",
			cookie: serverpb.SessionCookie{
				ID: id,
			},
			shouldVerify: false,
		},
		{
			testname: "Wrong secret",
			cookie: serverpb.SessionCookie{
				ID:     id,
				Secret: []byte{0x01, 0x02, 0x03, 0x04},
			},
			shouldVerify: false,
		},
		{
			testname: "No ID",
			cookie: serverpb.SessionCookie{
				Secret: origSecret,
			},
			shouldVerify: false,
		},
		{
			testname: "Wrong ID",
			cookie: serverpb.SessionCookie{
				ID:     123456,
				Secret: origSecret,
			},
			shouldVerify: false,
		},
		{
			testname:     "Empty cookie",
			cookie:       serverpb.SessionCookie{},
			shouldVerify: false,
		},
	} {
		t.Run(tc.testname, func(t *testing.T) {
			valid, username, err := ts.authentication.verifySession(context.TODO(), &tc.cookie)
			if err != nil {
				t.Fatalf("test got error %s, wanted no error", err)
			}
			if a, e := valid, tc.shouldVerify; a != e {
				t.Fatalf("cookie %v verification = %t, wanted %t", tc.cookie, a, e)
			}
			if a, e := username, sessionUsername; tc.shouldVerify && a != e {
				t.Fatalf("cookie %v verification returned username %s, wanted %s", tc.cookie, a, e)
			}
		})
	}
}

func TestAuthenticationAPIUserLogin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	ts := s.(*TestServer)
	if err := ts.WaitForInitialSplits(); err != nil {
		t.Fatal(err)
	}

	const (
		validUsername = "testuser"
		validPassword = "password"
	)

	cmd := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", validUsername, validPassword)
	if _, err := db.Exec(cmd); err != nil {
		t.Fatalf("failed to create user: %s", err)
	}

	tryLogin := func(username, password string) (*http.Response, error) {
		// We need to instantiate our own HTTP Request, because we must inspect
		// the returned headers.
		httpClient, err := ts.GetHTTPClient()
		if err != nil {
			t.Fatalf("could not get HTTP client: %s", err)
		}
		req := serverpb.UserLoginRequest{
			Username: username,
			Password: password,
		}
		var resp serverpb.UserLoginResponse
		return httputil.PostJSONWithRequest(
			httpClient, ts.AdminURL()+authPrefix+"login", &req, &resp,
		)
	}

	// Unsuccessful attempt. Should come back with a 401 and no "Set-Cookie"
	{
		response, err := tryLogin(validUsername, "wrongpassword")
		if !testutils.IsError(err, "status: 401") {
			t.Fatalf("login got error %s, wanted error with 401 status", err)
		}
		if cookies := response.Cookies(); len(cookies) > 0 {
			t.Fatalf("bad login got cookies %v, wanted empty", cookies)
		}
	}

	// Successful attempt. Should succeed and return a Set-Cookie header.
	response, err := tryLogin(validUsername, validPassword)
	if err != nil {
		t.Fatalf("good login got error %s, wanted no error", err)
	}
	cookies := response.Cookies()
	if len(cookies) == 0 {
		t.Fatalf("good login got no cookies: %v", response)
	}

	sessionCookie, err := decodeSessionCookie(cookies[0])
	if err != nil {
		t.Fatalf("failed to decode session cookie: %s", err)
	}

	// Look up session in database and verify hashed secret value and username.
	query := `SELECT "hashedSecret", "username" FROM system.web_sessions WHERE id = $1`
	result := db.QueryRow(query, sessionCookie.ID)
	var (
		sessHashedSecret []byte
		sessUsername     string
	)
	if err := result.Scan(&sessHashedSecret, &sessUsername); err != nil {
		t.Fatalf("error querying auth session: %s", err)
	}

	if a, e := sessUsername, validUsername; a != e {
		t.Fatalf("created auth session had username %s, wanted %s", a, e)
	}

	hasher := sha256.New()
	hashedSecret := hasher.Sum(sessionCookie.Secret)
	if a, e := sessHashedSecret, hashedSecret; !bytes.Equal(a, e) {
		t.Fatalf(
			"session secret hash was %v, wanted %v (derived from original secret %v)",
			a,
			e,
			sessionCookie.Secret,
		)
	}
}

// TestAuthenticationMux verifies that the authentication handler is used by all
// of the APIs it should be protecting. Authentication is enabled by default for
// the test server, and every test which accesses APIs uses an authenticated
// client (except for a few that specifically override it).  Therefore, this
// test verifies that authentication mux is attached to services at all by
// testing an endpoint of each with a verified and unverified client.
func TestAuthenticationMux(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.TODO())
	tsrv := s.(*TestServer)

	// Both the normal and authenticated client will be used for each test.
	normalClient, err := tsrv.GetHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	authClient, err := tsrv.GetAuthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	runRequest := func(
		client http.Client, method string, path string, body []byte, expected int,
	) error {
		req, err := http.NewRequest(method, tsrv.AdminURL()+path, bytes.NewBuffer(body))
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if a, e := resp.StatusCode, expected; a != e {
			message, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				message = []byte(err.Error())
			}
			return errors.Errorf("got status code %d (msg %s), wanted %d", a, string(message), e)
		}
		return nil
	}

	// Generate request for time series API.
	tsReq := tspb.TimeSeriesQueryRequest{
		StartNanos: 0,
		EndNanos:   100 * 1e9,
		Queries:    []tspb.Query{{Name: "test.metric"}},
	}
	var tsReqBuffer bytes.Buffer
	marshalFn := (&jsonpb.Marshaler{}).Marshal
	if err := marshalFn(&tsReqBuffer, &tsReq); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		method string
		path   string
		body   []byte
	}{
		{"GET", adminPrefix + "users", nil},
		{"GET", statusPrefix + "sessions", nil},
		{"POST", ts.URLPrefix + "query", tsReqBuffer.Bytes()},
	} {
		t.Run("path="+tc.path, func(t *testing.T) {
			// Verify normal client returns 401 Unauthorized.
			if err := runRequest(normalClient, tc.method, tc.path, tc.body, http.StatusUnauthorized); err != nil {
				t.Fatalf("request %s failed when not authorized: %s", tc.path, err)
			}

			// Verify authenticated client returns 200 OK.
			if err := runRequest(authClient, tc.method, tc.path, tc.body, http.StatusOK); err != nil {
				t.Fatalf("request %s failed when authorized: %s", tc.path, err)
			}
		})
	}
}
