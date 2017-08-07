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
	"crypto/sha256"
	"crypto/tls"
	gosql "database/sql"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/lib/pq"
	"golang.org/x/net/context"
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
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
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
		{"", insecureContext, http.StatusPermanentRedirect},

		// /_admin/: server.adminServer: no auth.
		{adminPrefix + "health", rootCertsContext, http.StatusOK},
		{adminPrefix + "health", nodeCertsContext, http.StatusOK},
		{adminPrefix + "health", testCertsContext, http.StatusOK},
		{adminPrefix + "health", noCertsContext, http.StatusOK},
		{adminPrefix + "health", insecureContext, http.StatusPermanentRedirect},

		// /debug/: server.adminServer: no auth.
		{debugEndpoint + "vars", rootCertsContext, http.StatusOK},
		{debugEndpoint + "vars", nodeCertsContext, http.StatusOK},
		{debugEndpoint + "vars", testCertsContext, http.StatusOK},
		{debugEndpoint + "vars", noCertsContext, http.StatusOK},
		{debugEndpoint + "vars", insecureContext, http.StatusPermanentRedirect},

		// /_status/nodes: server.statusServer: no auth.
		{statusPrefix + "nodes", rootCertsContext, http.StatusOK},
		{statusPrefix + "nodes", nodeCertsContext, http.StatusOK},
		{statusPrefix + "nodes", testCertsContext, http.StatusOK},
		{statusPrefix + "nodes", noCertsContext, http.StatusOK},
		{statusPrefix + "nodes", insecureContext, http.StatusPermanentRedirect},

		// /ts/: ts.Server: no auth.
		{ts.URLPrefix, rootCertsContext, http.StatusNotFound},
		{ts.URLPrefix, nodeCertsContext, http.StatusNotFound},
		{ts.URLPrefix, testCertsContext, http.StatusNotFound},
		{ts.URLPrefix, noCertsContext, http.StatusNotFound},
		{ts.URLPrefix, insecureContext, http.StatusPermanentRedirect},
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
			return fmt.Errorf("time %s was before early bound %s", actual, early)
		}
		if late.Before(actual) {
			return fmt.Errorf("time %s was after late bound %s", actual, late)
		}
		return nil
	}

	if err := verifyTimestamp(sessCreated, timeBoundBefore, timeBoundAfter); err != nil {
		t.Fatalf("bad createdAt timestamp: %s", err)
	}
	if err := verifyTimestamp(sessLastUsed, timeBoundBefore, timeBoundAfter); err != nil {
		t.Fatalf("bad lastUsedAt timestamp: %s", err)
	}
	timeout := s.ClusterSettings().WebSessionTimeout.Get()
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

	tryLogin := func(username, password string) (http.Header, error) {
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
		return httputil.PostJSONWithHeaders(
			httpClient, ts.AdminURL()+authPrefix+"login", nil, &req, &resp,
		)
	}

	// Unsuccessful attempt. Should come back with a 401 and no "Set-Cookie"
	headers, err := tryLogin(validUsername, "wrongpassword")
	if !testutils.IsError(err, "status: 401") {
		t.Fatalf("login got error %s, wanted error with 401 status", err)
	}
	if e := headers.Get("Set-Cookie"); e != "" {
		t.Fatalf("bad login got Set-Cookie %s, wanted empty", e)
	}

	// Successful attempt. Should succeed and return a Set-Cookie header.
	headers, err = tryLogin(validUsername, validPassword)
	if err != nil {
		t.Fatalf("good login got error %s, wanted no error", err)
	}
	rawCookie := headers.Get("set-cookie")
	if rawCookie == "" {
		t.Logf("%v", headers)
		t.Fatalf("good login got no Set-Cookie header")
	}

	// Validate the returned cookie.
	// http.Cookie doesn't export ReadCookies, so we construct a request
	// and use the Cookies() method.
	var cookie *http.Cookie
	{
		header := http.Header{}
		header.Set("cookie", rawCookie)
		request := http.Request{Header: header}
		cookie, err = request.Cookie(sessionCookieName)
		if err != nil {
			t.Fatalf("could not retrieve session cookie: %s", err)
		}
	}

	// Cookie value should be a base64 encoded protobuf.
	cookieBytes, err := base64.StdEncoding.DecodeString(cookie.Value)
	if err != nil {
		t.Fatalf("expected cookie to be base64 encoded, got %s: %s", cookie.Value, err)
	}
	var sessionCookieValue serverpb.SessionCookie
	if err := sessionCookieValue.Unmarshal(cookieBytes); err != nil {
		t.Fatalf("failed to unmarshal session cookie value: %s", err)
	}

	// Look up session in database and verify hashed secret value and username.
	id := sessionCookieValue.Id
	query := `SELECT "hashedSecret", "username" FROM system.web_sessions WHERE id = $1`
	result := db.QueryRow(query, id)
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
	hashedSecret := hasher.Sum(sessionCookieValue.Secret)
	if a, e := sessHashedSecret, hashedSecret; !bytes.Equal(a, e) {
		t.Fatalf(
			"session secret hash was %v, wanted %v (derived from original secret %v)",
			a,
			e,
			sessionCookieValue.Secret,
		)
	}
}
