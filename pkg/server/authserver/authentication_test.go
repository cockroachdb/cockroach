// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package authserver_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	gosql "database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/ctpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/apiconstants"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/ts"
	"github.com/cockroachdb/cockroach/pkg/ts/tspb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/jsonpb"
	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type ctxI interface {
	GetHTTPClient() (http.Client, error)
	HTTPRequestScheme() string
}

var _ ctxI = insecureCtx{}
var _ ctxI = (*rpc.Context)(nil)

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

// Verify client certificate enforcement and user allowlisting.
func TestSSLEnforcement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		// This test is verifying the (unimplemented) authentication of SSL
		// client certificates over HTTP endpoints. Web session authentication
		// is disabled in order to avoid the need to authenticate the individual
		// clients being instantiated.
		InsecureWebAccess: true,
	})
	defer srv.Stopper().Stop(ctx)

	if srv.DeploymentMode().IsExternal() {
		// Enable access to the nodes endpoint for the test tenant.
		require.NoError(t, srv.GrantTenantCapabilities(
			ctx, serverutils.TestTenantID(),
			map[tenantcapabilities.ID]string{tenantcapabilities.CanViewNodeInfo: "true"}))
	}

	s := srv.ApplicationLayer()

	newRPCContext := func(insecure bool, user username.SQLUsername) *rpc.Context {
		opts := rpc.DefaultContextOptions()
		opts.Insecure = insecure
		opts.User = user
		opts.Stopper = s.AppStopper()
		opts.Settings = s.ClusterSettings()
		return rpc.NewContext(ctx, opts)
	}

	// HTTPS with client certs for security.RootUser.
	rootCertsContext := newRPCContext(false, username.RootUserName())
	// HTTPS with client certs for security.NodeUser.
	nodeCertsContext := newRPCContext(false, username.NodeUserName())
	// HTTPS with client certs for TestUser.
	testCertsContext := newRPCContext(false, username.TestUserName())
	// HTTPS without client certs. The user does not matter.
	noCertsContext := insecureCtx{}
	// Plain http.
	insecureContext := newRPCContext(true, username.TestUserName())

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
		{apiconstants.AdminPrefix + "health", rootCertsContext, http.StatusOK},
		{apiconstants.AdminPrefix + "health", nodeCertsContext, http.StatusOK},
		{apiconstants.AdminPrefix + "health", testCertsContext, http.StatusOK},
		{apiconstants.AdminPrefix + "health", noCertsContext, http.StatusOK},
		{apiconstants.AdminPrefix + "health", insecureContext, http.StatusTemporaryRedirect},

		// /debug/: server.adminServer: no auth.
		{debug.Endpoint + "vars", rootCertsContext, http.StatusOK},
		{debug.Endpoint + "vars", nodeCertsContext, http.StatusOK},
		{debug.Endpoint + "vars", testCertsContext, http.StatusOK},
		{debug.Endpoint + "vars", noCertsContext, http.StatusOK},
		{debug.Endpoint + "vars", insecureContext, http.StatusTemporaryRedirect},

		// /_status/nodes: server.statusServer: no auth.
		{apiconstants.StatusPrefix + "nodes", rootCertsContext, http.StatusOK},
		{apiconstants.StatusPrefix + "nodes", nodeCertsContext, http.StatusOK},
		{apiconstants.StatusPrefix + "nodes", testCertsContext, http.StatusOK},
		{apiconstants.StatusPrefix + "nodes", noCertsContext, http.StatusOK},
		{apiconstants.StatusPrefix + "nodes", insecureContext, http.StatusTemporaryRedirect},

		// /ts/: ts.Server: no auth.
		{ts.URLPrefix, rootCertsContext, http.StatusNotFound},
		{ts.URLPrefix, nodeCertsContext, http.StatusNotFound},
		{ts.URLPrefix, testCertsContext, http.StatusNotFound},
		{ts.URLPrefix, noCertsContext, http.StatusNotFound},
		{ts.URLPrefix, insecureContext, http.StatusTemporaryRedirect},
	} {
		t.Run(tc.path, func(t *testing.T) {
			if tc.path == apiconstants.StatusPrefix+"nodes" && srv.TenantController().StartedDefaultTestTenant() {
				// TODO(multitenant): The /_status/nodes endpoint should be
				// available subject to a tenant capability.
				skip.WithIssue(t, 110009)
			}

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
				Host:   s.HTTPAddr(),
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

func TestVerifyPasswordDBConsole(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	ts := s.ApplicationLayer()

	if util.RaceEnabled {
		// The default bcrypt cost makes this test approximately 30s slower when the
		// race detector is on.
		security.BcryptCost.Override(ctx, &ts.ClusterSettings().SV, int64(bcrypt.MinCost))
	}

	//location is used for timezone testing.
	shanghaiLoc, err := timeutil.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}

	for _, user := range []struct {
		username         string
		password         string
		loginFlag        string
		validUntilClause string
		qargs            []interface{}
	}{
		{"azure_diamond", "hunter2", "", "", nil},
		{"druidia", "12345", "", "", nil},

		{"richardc", "12345", "NOLOGIN", "", nil},
		{"richardc2", "12345", "NOSQLLOGIN", "", nil},
		{"has_global_nosqlogin", "12345", "", "", nil},
		{"inherits_global_nosqlogin", "12345", "", "", nil},
		{"before_epoch", "12345", "", "VALID UNTIL '1969-01-01'", nil},
		{"epoch", "12345", "", "VALID UNTIL '1970-01-01'", nil},
		{"cockroach", "12345", "", "VALID UNTIL '2100-01-01'", nil},
		{"cthon98", "12345", "", "VALID UNTIL NULL", nil},

		{"toolate", "12345", "", "VALID UNTIL $1",
			[]interface{}{timeutil.Now().Add(-10 * time.Minute)}},
		{"timelord", "12345", "", "VALID UNTIL $1",
			[]interface{}{timeutil.Now().Add(59 * time.Minute).In(shanghaiLoc)}},
	} {
		username := username.MakeSQLUsernameFromPreNormalizedString(user.username)
		cmd := fmt.Sprintf(
			"CREATE USER %s WITH PASSWORD '%s' %s %s",
			username.SQLIdentifier(), user.password, user.loginFlag, user.validUntilClause)

		if _, err := db.Exec(cmd, user.qargs...); err != nil {
			t.Fatalf("failed to create user: %s", err)
		}
	}

	// Set up NOSQLLOGIN global privilege.
	_, err = db.Exec("GRANT SYSTEM NOSQLLOGIN TO has_global_nosqlogin")
	require.NoError(t, err)
	_, err = db.Exec("GRANT has_global_nosqlogin TO inherits_global_nosqlogin")
	require.NoError(t, err)

	for _, tc := range []struct {
		testName           string
		username           string
		password           string
		shouldAuthenticate bool
	}{
		{"valid login", "azure_diamond", "hunter2", true},
		{"wrong password", "azure_diamond", "hunter", false},
		{"empty password", "azure_diamond", "", false},
		{"wrong emoji password", "azure_diamond", "🍦", false},
		{"correct password with suffix should fail", "azure_diamond", "hunter2345", false},
		{"correct password with prefix should fail", "azure_diamond", "shunter2", false},
		{"wrong password all numeric", "azure_diamond", "12345", false},
		{"wrong password all stars", "azure_diamond", "*******", false},
		{"valid login numeric password", "druidia", "12345", true},
		{"wrong password matching other user", "druidia", "hunter2", false},
		{"root with empty password should fail", "root", "", false},
		{"empty username and password should fail", "", "", false},
		{"username does not exist should fail", "doesntexist", "zxcvbn", false},

		{"user with NOLOGIN role option should fail", "richardc", "12345", false},
		// The NOSQLLOGIN cases are the only cases where SQL and DB Console login outcomes differ.
		{"user with NOSQLLOGIN role option should succeed", "richardc2", "12345", true},
		{"user with NOSQLLOGIN global privilege should succeed", "has_global_nosqlogin", "12345", true},
		{"user who inherits NOSQLLOGIN global privilege should succeed", "inherits_global_nosqlogin", "12345", true},

		{"user with VALID UNTIL before the Unix epoch should fail", "before_epoch", "12345", false},
		{"user with VALID UNTIL at Unix epoch should fail", "epoch", "12345", false},
		{"user with VALID UNTIL future date should succeed", "cockroach", "12345", true},
		{"user with VALID UNTIL 10 minutes ago should fail", "toolate", "12345", false},
		{"user with VALID UNTIL future time in Shanghai time zone should succeed", "timelord", "12345", true},
		{"user with VALID UNTIL NULL should succeed", "cthon98", "12345", true},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			username := username.MakeSQLUsernameFromPreNormalizedString(tc.username)
			authServer := ts.HTTPAuthServer().(authserver.Server)
			verified, pwRetrieveFn, err := authServer.VerifyUserSessionDBConsole(context.Background(), username)
			if err != nil {
				t.Fatalf(
					"user session verification failed, credentials %s/%s failed with error %s, wanted no error",
					tc.username,
					tc.password,
					err,
				)
			}
			if !verified && tc.shouldAuthenticate {
				t.Fatalf("unexpected user %s for DB console login, verified: %t, expected shouldAuthenticate: %t",
					tc.username, verified, tc.shouldAuthenticate)
			} else if verified {
				valid, expired, err := authServer.VerifyPasswordDBConsole(context.Background(), username, tc.password, pwRetrieveFn)
				if err != nil {
					t.Errorf(
						"credentials %s/%s failed with error %s, wanted no error",
						tc.username,
						tc.password,
						err,
					)
				}
				if !valid && tc.shouldAuthenticate {
					t.Fatalf("unexpected credentials %s/%s for DB console login, valid: %t, expected shouldAuthenticate: %t",
						tc.username, tc.password, valid, tc.shouldAuthenticate)
				} else if valid && !expired != tc.shouldAuthenticate {
					t.Errorf(
						"credentials %s/%s valid = %t, wanted %t",
						tc.username,
						tc.password,
						valid,
						tc.shouldAuthenticate,
					)
				}
			}
		})
	}
}

func TestCreateSession(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	username := username.TestUserName()
	if err := ts.CreateAuthUser(username, false /* isAdmin */); err != nil {
		t.Fatal(err)
	}

	// Create an authentication, noting the time before and after creation. This
	// lets us ensure that the timestamps created are accurate.
	timeBoundBefore := ts.Clock().PhysicalTime()
	id, origSecret, err := ts.HTTPAuthServer().(authserver.Server).NewAuthSession(context.Background(), username)
	if err != nil {
		t.Fatalf("error creating auth session: %s", err)
	}
	timeBoundAfter := ts.Clock().PhysicalTime()

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
	_, _ = hasher.Write(origSecret)
	hashedSecret := hasher.Sum(nil)
	if !bytes.Equal(sessHashedSecret, hashedSecret) {
		t.Fatalf("hashed value of secret: \n%#v\ncomputed as: \n%#v\nwanted: \n%#v", origSecret, hashedSecret, sessHashedSecret)
	}

	// Username.
	if a, e := sessUsername, username.Normalized(); a != e {
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
	timeout := authserver.WebSessionTimeout.Get(&ts.ClusterSettings().SV)
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
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	sessionUsername := username.TestUserName()
	if err := ts.CreateAuthUser(sessionUsername, false /* isAdmin */); err != nil {
		t.Fatal(err)
	}

	authServer := ts.HTTPAuthServer().(authserver.Server)
	id, origSecret, err := authServer.NewAuthSession(context.Background(), sessionUsername)
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
			valid, username, err := authServer.VerifySession(context.Background(), &tc.cookie)
			if err != nil {
				t.Fatalf("test got error %s, wanted no error", err)
			}
			if a, e := valid, tc.shouldVerify; a != e {
				t.Fatalf("cookie %v verification = %t, wanted %t", tc.cookie, a, e)
			}
			if a, e := username, sessionUsername.Normalized(); tc.shouldVerify && a != e {
				t.Fatalf("cookie %v verification returned username %s, wanted %s", tc.cookie, a, e)
			}
		})
	}
}

func TestAuthenticationAPIUserLogin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

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
		httpClient, err := ts.GetUnauthenticatedHTTPClient()
		if util.RaceEnabled {
			httpClient.Timeout += 30 * time.Second
		}
		if err != nil {
			t.Fatalf("could not get HTTP client: %s", err)
		}
		req := serverpb.UserLoginRequest{
			Username: username,
			Password: password,
		}
		var resp serverpb.UserLoginResponse
		return httputil.PostJSONWithRequest(
			httpClient, ts.AdminURL().WithPath(authserver.LoginPath).String(), &req, &resp,
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
	sessionCookie, err := authserver.FindAndDecodeSessionCookie(context.Background(), ts.ClusterSettings(), cookies)
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
	_, _ = hasher.Write(sessionCookie.Secret)
	hashedSecret := hasher.Sum(nil)
	if a, e := sessHashedSecret, hashedSecret; !bytes.Equal(a, e) {
		t.Fatalf(
			"session secret hash was %v, wanted %v (derived from original secret %v)",
			a,
			e,
			sessionCookie.Secret,
		)
	}
}

func TestLogoutClearsCookies(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		DefaultTestTenant: base.TestControlsTenantsExplicitly,
	})
	defer s.Stopper().Stop(context.Background())

	testFunc := func(ts serverutils.ApplicationLayerInterface, expectTenantCookieInClearList bool) {
		// Log in.
		authHTTPClient, _, err := ts.GetAuthenticatedHTTPClientAndCookie(
			apiconstants.TestingUserName(), true, serverutils.SingleTenantSession,
		)
		require.NoError(t, err)

		// Log out.
		resp, err := authHTTPClient.Get(ts.AdminURL().WithPath(authserver.LogoutPath).String())
		require.NoError(t, err)
		defer resp.Body.Close()

		cookies := resp.Cookies()
		cNames := make([]string, len(cookies))
		for i, c := range cookies {
			require.Equal(t, "", c.Value)
			cNames[i] = c.Name
		}
		expected := []string{authserver.SessionCookieName}
		if expectTenantCookieInClearList {
			expected = append(expected, authserver.TenantSelectCookieName)
		}
		require.ElementsMatch(t, cNames, expected)
	}

	t.Run("system tenant", func(t *testing.T) {
		testFunc(s, true)
	})

	t.Run("secondary tenant", func(t *testing.T) {
		ts, err := s.TenantController().StartTenant(context.Background(), base.TestTenantArgs{TenantID: roachpb.MustMakeTenantID(10)})
		if err != nil {
			t.Fatal(err)
		}
		testFunc(ts, false)
	})
}

func TestLogout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.ApplicationLayer()

	// Log in.
	authHTTPClient, cookie, err := ts.GetAuthenticatedHTTPClientAndCookie(
		apiconstants.TestingUserName(), true, serverutils.SingleTenantSession,
	)
	if err != nil {
		t.Fatal("error opening HTTP client", err)
	}

	// Log out.
	var resp serverpb.UserLogoutResponse
	if err := httputil.GetJSON(authHTTPClient, ts.AdminURL().WithPath(authserver.LogoutPath).String(), &resp); err != nil {
		t.Fatal("logout request failed:", err)
	}

	// Verify that revokedAt has been set in the DB.
	query := `SELECT "revokedAt" FROM system.web_sessions WHERE id = $1`
	result := db.QueryRow(query, cookie.ID)
	var revokedAt string
	if err := result.Scan(&revokedAt); err != nil {
		t.Fatalf("error querying auth session: %s", err)
	}

	if revokedAt == "" {
		t.Fatal("expected revoked at to not be empty; was empty")
	}

	databasesURL := ts.AdminURL().WithPath("/_admin/v1/databases").String()

	// Verify that we're unauthorized after logout.
	response, err := authHTTPClient.Get(databasesURL)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusUnauthorized {
		t.Fatal("expected unauthorized response after logout; got", response.StatusCode)
	}

	// Try to use the revoked cookie; verify that it doesn't work.
	encodedCookie, err := authserver.EncodeSessionCookie(cookie, false /* forHTTPSOnly */)
	if err != nil {
		t.Fatal(err)
	}

	invalidAuthClient, err := s.GetUnauthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	invalidAuthClient.Jar = jar
	invalidAuthClient.Jar.SetCookies(s.AdminURL().URL, []*http.Cookie{encodedCookie})

	invalidAuthResp, err := invalidAuthClient.Get(databasesURL)
	if err != nil {
		t.Fatal(err)
	}
	defer invalidAuthResp.Body.Close()

	if invalidAuthResp.StatusCode != 401 {
		t.Fatal("expected unauthorized error; got", invalidAuthResp.StatusCode)
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
	defer log.Scope(t).Close(t)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	tsrv := s.ApplicationLayer()

	// Both the normal and authenticated client will be used for each test.
	normalClient, err := tsrv.GetUnauthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	authClient, err := tsrv.GetAdminHTTPClient()
	if err != nil {
		t.Fatal(err)
	}

	runRequest := func(
		client http.Client, method string, path string, body []byte, cookieHeader string, expected int,
	) error {
		req, err := http.NewRequest(method, tsrv.AdminURL().WithPath(path).String(), bytes.NewBuffer(body))
		if cookieHeader != "" {
			// The client still attaches its own cookies to this one.
			req.Header.Set("cookie", cookieHeader)
		}
		if err != nil {
			return err
		}
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		if a, e := resp.StatusCode, expected; a != e {
			message, err := io.ReadAll(resp.Body)
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
		method       string
		path         string
		body         []byte
		cookieHeader string
	}{
		{"GET", apiconstants.AdminPrefix + "users", nil, ""},
		{"GET", apiconstants.AdminPrefix + "users", nil, "session=badcookie"},
		{"GET", apiconstants.StatusPrefix + "sessions", nil, ""},
		{"POST", ts.URLPrefix + "query", tsReqBuffer.Bytes(), ""},
	} {
		t.Run("path="+tc.path, func(t *testing.T) {
			if s.TenantController().StartedDefaultTestTenant() && strings.HasPrefix(tc.path, ts.URLPrefix) {
				// As of this writing, timeseries requests to secondary
				// tenants are overly restricted. This is a feature gap. See
				// issue #102378.
				skip.Unimplemented(t, 102378)
			}

			// Verify normal client returns 401 Unauthorized.
			if err := runRequest(normalClient, tc.method, tc.path, tc.body, tc.cookieHeader, http.StatusUnauthorized); err != nil {
				t.Fatalf("request %s failed when not authorized: %s", tc.path, err)
			}

			// Verify authenticated client returns 200 OK.
			if err := runRequest(authClient, tc.method, tc.path, tc.body, tc.cookieHeader, http.StatusOK); err != nil {
				t.Fatalf("request %s failed when authorized: %s", tc.path, err)
			}
		})
	}
}

func TestGRPCAuthentication(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	ts := s.ApplicationLayer()

	// For each subsystem we pick a representative RPC. The idea is not to
	// exhaustively test each RPC but to prevent server startup from being
	// refactored in such a way that an entire subsystem becomes inadvertently
	// exempt from authentication checks.
	subsystems := []struct {
		name    string
		sendRPC func(context.Context, *grpc.ClientConn) error

		storageOnly bool
	}{
		{"gossip", func(ctx context.Context, conn *grpc.ClientConn) error {
			stream, err := gossip.NewGossipClient(conn).Gossip(ctx)
			if err != nil {
				return err
			}
			_ = stream.Send(&gossip.Request{})
			_, err = stream.Recv()
			return err
		}, true},
		{"internal", func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := kvpb.NewInternalClient(conn).Batch(ctx, &kvpb.BatchRequest{})
			return err
		}, true},
		{"perReplica", func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := kvserver.NewPerReplicaClient(conn).CollectChecksum(ctx, &kvserver.CollectChecksumRequest{})
			return err
		}, true},
		{"raft", func(ctx context.Context, conn *grpc.ClientConn) error {
			stream, err := kvserver.NewMultiRaftClient(conn).RaftMessageBatch(ctx)
			if err != nil {
				return err
			}
			_ = stream.Send(&kvserverpb.RaftMessageRequestBatch{})
			_, err = stream.Recv()
			return err
		}, true},
		{"closedTimestamp", func(ctx context.Context, conn *grpc.ClientConn) error {
			stream, err := ctpb.NewSideTransportClient(conn).PushUpdates(ctx)
			if err != nil {
				return err
			}
			_ = stream.Send(&ctpb.Update{})
			_, err = stream.Recv()
			return err
		}, true},
		{"distSQL", func(ctx context.Context, conn *grpc.ClientConn) error {
			stream, err := execinfrapb.NewDistSQLClient(conn).FlowStream(ctx)
			if err != nil {
				return err
			}
			_ = stream.Send(&execinfrapb.ProducerMessage{})
			_, err = stream.Recv()
			return err
		}, false},
		{"init", func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := serverpb.NewInitClient(conn).Bootstrap(ctx, &serverpb.BootstrapRequest{})
			return err
		}, true},
		{"admin", func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := serverpb.NewAdminClient(conn).Databases(ctx, &serverpb.DatabasesRequest{})
			return err
		}, false},
		{"status", func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := serverpb.NewStatusClient(conn).ListSessions(ctx, &serverpb.ListSessionsRequest{})
			return err
		}, false},
	}

	conn, err := grpc.DialContext(ctx, ts.RPCAddr(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{
			InsecureSkipVerify: true,
		})))
	if err != nil {
		t.Fatal(err)
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close() // nolint:grpcconnclose
	}(conn)
	for _, subsystem := range subsystems {
		if subsystem.storageOnly && s.TenantController().StartedDefaultTestTenant() {
			// Subsystem only available on the system tenant.
			continue
		}
		t.Run(fmt.Sprintf("no-cert/%s", subsystem.name), func(t *testing.T) {
			err := subsystem.sendRPC(ctx, conn)
			if exp := "TLSInfo is not available in request context"; !testutils.IsError(err, exp) {
				t.Errorf("expected %q error, but got %v", exp, err)
			}
		})
	}

	certManager, err := ts.RPCContext().GetCertificateManager()
	if err != nil {
		t.Fatal(err)
	}
	tlsConfig, err := certManager.GetClientTLSConfig(username.TestUserName())
	if err != nil {
		t.Fatal(err)
	}
	conn, err = grpc.DialContext(ctx, ts.RPCAddr(),
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	if err != nil {
		t.Fatal(err)
	}
	defer func(conn *grpc.ClientConn) {
		_ = conn.Close() // nolint:grpcconnclose
	}(conn)
	for _, subsystem := range subsystems {
		if subsystem.storageOnly && s.TenantController().StartedDefaultTestTenant() {
			// Subsystem only available on the system tenant.
			continue
		}
		t.Run(fmt.Sprintf("bad-user/%s", subsystem.name), func(t *testing.T) {
			err := subsystem.sendRPC(ctx, conn)
			if exp := `need root or node client cert to perform RPCs on this server`; !testutils.IsError(err, exp) {
				t.Errorf("expected %q error, but got %v", exp, err)
			}
		})
	}
}

func TestCreateAggregatedSessionCookieValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name        string
		mapArg      []authserver.SessionCookieValue
		resExpected string
	}{
		{"standard arg",
			[]authserver.SessionCookieValue{
				authserver.MakeSessionCookieValue("system", "session=abcd1234"),
				authserver.MakeSessionCookieValue("app", "session=efgh5678"),
			},
			"abcd1234,system,efgh5678,app",
		},
		{"empty arg", []authserver.SessionCookieValue{}, ""},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("create-session-cookie/%s", test.name), func(t *testing.T) {
			res := authserver.CreateAggregatedSessionCookieValue(test.mapArg)
			require.Equal(t, test.resExpected, res)
		})
	}
}

func TestFindSessionCookieValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	normalSessionStr := "abcd1234,system,efgh5678,app"
	tests := []struct {
		name              string
		sessionCookie     *http.Cookie
		tenantSelectValue string
		resExpected       string
		errorExpected     bool
	}{
		{
			name: "standard args",
			sessionCookie: &http.Cookie{
				Name:  authserver.SessionCookieName,
				Value: normalSessionStr,
				Path:  "/",
			},
			tenantSelectValue: "system",
			resExpected:       "abcd1234",
			errorExpected:     false,
		},
		{
			name:              "no multitenant session cookie",
			sessionCookie:     nil,
			tenantSelectValue: "system",
			resExpected:       "",
			errorExpected:     false,
		},
		{
			name: "no tenant cookie",
			sessionCookie: &http.Cookie{
				Name:  authserver.SessionCookieName,
				Value: normalSessionStr,
				Path:  "/",
			},
			resExpected:   "abcd1234",
			errorExpected: false,
		},
		{
			name: "empty string tenant cookie",
			sessionCookie: &http.Cookie{
				Name:  authserver.SessionCookieName,
				Value: normalSessionStr,
				Path:  "/",
			},
			tenantSelectValue: "",
			resExpected:       "abcd1234",
			errorExpected:     false,
		},
		{
			name: "no tenant name match",
			sessionCookie: &http.Cookie{
				Name:  authserver.SessionCookieName,
				Value: normalSessionStr,
				Path:  "/",
			},
			tenantSelectValue: "app2",
			resExpected:       "",
			errorExpected:     true,
		},
		{
			name: "legacy session cookie",
			sessionCookie: &http.Cookie{
				Name:  authserver.SessionCookieName,
				Value: "aaskjhf218==",
				Path:  "/",
			},
			tenantSelectValue: "",
			resExpected:       "aaskjhf218==",
			errorExpected:     false,
		},
	}
	for _, test := range tests {
		t.Run(fmt.Sprintf("find-session-cookie/%s", test.name), func(t *testing.T) {
			st := cluster.MakeClusterSettings()
			res, err := authserver.FindSessionCookieValueForTenant(st, test.sessionCookie, test.tenantSelectValue)
			require.Equal(t, test.resExpected, res)
			require.Equal(t, test.errorExpected, err != nil)
		})
	}
}
