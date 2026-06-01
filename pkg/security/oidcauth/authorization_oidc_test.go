// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcauth

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/coreos/go-oidc/v3/oidc"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

const (
	testUser     = "test"
	roleOwners   = "owners"
	roleUsers    = "users"
	roleAuditors = "auditors"
)

// makeJWT creates an RS256-signed JWT containing the supplied claims.
func makeJWT(t *testing.T, claims map[string]any, key jwk.Key) string {
	token := jwt.New()
	for k, v := range claims {
		require.NoError(t, token.Set(k, v))
	}
	// Sign with the private key
	raw, err := jwt.Sign(token, jwt.WithKey(jwa.RS256, key))
	require.NoError(t, err)
	return string(raw)
}

// mockManager implements IOIDCManager.  Each instance returns whatever
// raw ID token and OAuth2 token we injected when it was built.
type mockManager struct {
	cfg         *oauth2.Config
	rawIDToken  string
	accessToken string
	email       string
}

func (m mockManager) Verify(context.Context, string) (*oidc.IDToken, error) { return nil, nil }
func (m mockManager) Exchange(
	context.Context, string, ...oauth2.AuthCodeOption,
) (*oauth2.Token, error) {
	return nil, nil
}
func (m mockManager) AuthCodeURL(state string, _ ...oauth2.AuthCodeOption) string {
	return m.cfg.AuthCodeURL(state)
}

func (m mockManager) ExchangeVerifyGetTokenInfo(
	ctx context.Context, _, _ string, _ bool,
) (map[string]json.RawMessage, map[string]json.RawMessage, string, string, error) {
	if m.rawIDToken == "" {
		// The real implementation would fail to extract the ID token from the
		// credentials and return an error.
		return nil, nil, "", "", errors.New("OIDC: required id_token not found in credentials")
	}

	var idTokenClaims map[string]json.RawMessage
	// The real implementation calls Verify, which will fail on a non-JWT token.
	// For this test, we allow unparseable tokens to test the `authorize` function's
	// handling of them. We still need to extract claims if possible for the
	// `extractUsernameFromClaims` call that happens before `authorize`.
	if tok, err := jwt.ParseInsecure([]byte(m.rawIDToken)); err == nil {
		if claimsMap, err := tok.AsMap(ctx); err == nil {
			if jsonBytes, err := json.Marshal(claimsMap); err == nil {
				_ = json.Unmarshal(jsonBytes, &idTokenClaims)
			}
		}
	}

	// The email claim from the ID token is used to find the SQL user.
	// The mock needs to ensure this claim is present for the login to proceed.
	if idTokenClaims == nil {
		idTokenClaims = make(map[string]json.RawMessage)
	}
	if _, ok := idTokenClaims["email"]; !ok {
		idTokenClaims["email"] = json.RawMessage(`"` + m.email + `"`)
	}

	var accessTokenClaims map[string]json.RawMessage
	if m.accessToken != "" {
		if tok, err := jwt.ParseInsecure([]byte(m.accessToken)); err == nil {
			if claimsMap, err := tok.AsMap(ctx); err == nil {
				if jsonBytes, err := json.Marshal(claimsMap); err == nil {
					_ = json.Unmarshal(jsonBytes, &accessTokenClaims)
				}
			}
		}
	}

	return idTokenClaims, accessTokenClaims, m.rawIDToken, m.accessToken, nil
}

func (m mockManager) UserInfo(ctx context.Context, ts oauth2.TokenSource) (*oidc.UserInfo, error) {
	// This mock is not intended to handle UserInfo calls. Returning an error
	// prevents a nil pointer dereference in the calling code.
	return nil, errors.New("UserInfo not implemented in this mock")
}

var _ IOIDCManager = (*mockManager)(nil)

// TestOIDCAuthorization_TokenPaths exercises the public OIDC login and callback
// handlers.  Each table entry spins up a fresh single-node server, injects a
// different set of token claims, and verifies the resulting SQL role
// memberships for the test user.
func TestOIDCAuthorization_TokenPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create an RSA key pair for signing tokens.
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwkKey, err := jwk.FromRaw(privateKey)
	require.NoError(t, err)

	// A helper to generate JWTs for tests that need them.
	makeToken := func(groups []string) string {
		claims := map[string]any{}
		if groups != nil {
			claims["groups"] = groups
		}
		return makeJWT(t, claims, jwkKey)
	}

	// Start a brand-new in-process node for this sub-test.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	app := s.ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create the user and any roles that might be granted.
	sqlDB.Exec(t, `CREATE USER `+testUser)
	sqlDB.Exec(t, `CREATE ROLE `+roleOwners)
	sqlDB.Exec(t, `CREATE ROLE `+roleUsers)
	sqlDB.Exec(t, `CREATE ROLE `+roleAuditors)

	testCases := []struct {
		name             string
		idToken          string
		accessToken      string
		expectedRoles    []string
		expectedStatus   int // 0 => default 307
		wantErrSubstring string
	}{
		{
			name:          "groups in ID token",
			idToken:       makeToken([]string{roleOwners, roleUsers}),
			accessToken:   makeToken(nil), // access token has no groups
			expectedRoles: []string{roleOwners, roleUsers},
		},
		{
			name:          "groups in access token only",
			idToken:       makeToken(nil), // id token has no groups
			accessToken:   makeToken([]string{roleOwners}),
			expectedRoles: []string{roleOwners},
		},
		{
			name:          "groups in both tokens (distinct sets)",
			idToken:       makeToken([]string{roleOwners, roleAuditors}),
			accessToken:   makeToken([]string{roleUsers}),
			expectedRoles: []string{roleOwners, roleUsers, roleAuditors},
		},
		{
			name:          "groups in both tokens (overlap)",
			idToken:       makeToken([]string{roleOwners, roleUsers}),
			accessToken:   makeToken([]string{roleUsers, roleAuditors}),
			expectedRoles: []string{roleOwners, roleUsers, roleAuditors},
		},
		{
			name:             "groups claim absent from both tokens",
			idToken:          makeToken(nil),
			accessToken:      makeToken(nil),
			expectedRoles:    []string{},
			expectedStatus:   http.StatusForbidden,
			wantErrSubstring: "OIDC: unable to complete authentication",
		},
		{
			name:             "empty groups list in ID token",
			idToken:          makeToken([]string{}),
			accessToken:      makeToken(nil),
			expectedRoles:    []string{},
			expectedStatus:   http.StatusForbidden,
			wantErrSubstring: "OIDC: unable to complete authentication",
		},
		{
			name:             "empty groups list in access token",
			idToken:          makeToken(nil),
			accessToken:      makeToken([]string{}),
			expectedRoles:    []string{},
			expectedStatus:   http.StatusForbidden,
			wantErrSubstring: "OIDC: unable to complete authentication",
		},
		{
			name:          "unparseable ID token, groups in access token",
			idToken:       "this is not a jwt",
			accessToken:   makeToken([]string{roleUsers}),
			expectedRoles: []string{roleUsers},
		},
		{
			name:          "groups in ID token, unparseable access token",
			idToken:       makeToken([]string{roleOwners}),
			accessToken:   "this is not a jwt either",
			expectedRoles: []string{roleOwners},
		},
		{
			name:             "unparseable tokens, no groups",
			idToken:          "foo",
			accessToken:      "bar",
			expectedRoles:    []string{},
			expectedStatus:   http.StatusForbidden,
			wantErrSubstring: "OIDC: unable to complete authentication",
		},
		{
			name:             "missing ID token",
			idToken:          "",
			accessToken:      "does not matter",
			expectedRoles:    []string{},
			expectedStatus:   http.StatusInternalServerError,
			wantErrSubstring: "OIDC: unable to complete authentication",
		},
		{
			name:           "missing access token",
			idToken:        makeToken([]string{roleOwners}),
			accessToken:    "",
			expectedRoles:  []string{},
			expectedStatus: http.StatusForbidden,
		},
	}

	for _, tc := range testCases {
		tc := tc // capture loop var
		t.Run(tc.name, func(t *testing.T) {
			staleRows := sqlDB.QueryStr(t,
				`SELECT role FROM system.role_members WHERE member = $1`, testUser)
			if len(staleRows) > 0 {
				staleRoles := make([]string, len(staleRows))
				for i, r := range staleRows {
					staleRoles[i] = r[0]
				}
				sqlDB.Exec(t, fmt.Sprintf(`REVOKE %s FROM %s`, strings.Join(staleRoles, ", "), testUser))
			}

			// Replace the factory so that the server uses our mock manager.
			origFactory := NewOIDCManager
			t.Cleanup(func() { NewOIDCManager = origFactory })
			NewOIDCManager = func(_ context.Context, conf oidcAuthenticationConf,
				redirectURL string, scopes []string) (IOIDCManager, error) {

				cfg := &oauth2.Config{
					ClientID:     conf.clientID,
					ClientSecret: conf.clientSecret,
					RedirectURL:  redirectURL,
					Endpoint:     oauth2.Endpoint{AuthURL: "https://provider.example.com/auth"},
					Scopes:       scopes,
				}
				return &mockManager{
					cfg:         cfg,
					rawIDToken:  tc.idToken,
					accessToken: tc.accessToken,
					email:       testUser + "@example.com",
				}, nil
			}

			// Enable OIDC plus authorization.
			st := s.ClusterSettings()

			// Make the issuer unique to prevent race conditions in parallel tests
			uniq := strings.ReplaceAll(tc.name, " ", "_")
			OIDCProviderURL.Override(ctx, &st.SV,
				fmt.Sprintf("https://provider.example.com/%s", uniq))
			OIDCClientID.Override(ctx, &st.SV, "client")
			OIDCClientSecret.Override(ctx, &st.SV, "secret")
			OIDCRedirectURL.Override(ctx, &st.SV, "https://cockroachlabs.com/oidc/v1/callback")
			OIDCClaimJSONKey.Override(ctx, &st.SV, "email")
			OIDCPrincipalRegex.Override(ctx, &st.SV, "^([^@]+)@[^@]+$")
			OIDCEnabled.Override(ctx, &st.SV, true)
			OIDCAuthZEnabled.Override(ctx, &st.SV, true)
			OIDCAuthGroupClaim.Override(ctx, &st.SV, "groups")

			// Build an HTTP client that trusts the node certificates.
			rpcCtx := app.NewClientRPCContext(ctx, username.TestUserName())
			client, err := rpcCtx.GetHTTPClient()
			require.NoError(t, err)
			// The default 10s timeout is too short under race detection where OIDC
			// callback processing can take 20s+.
			client.Timeout = 45 * time.Second
			// Prevent automatic redirects so we can capture cookies and headers.
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

			// Step 1: hit /oidc/v1/login and capture the state cookie.
			resp, err := client.Get(app.AdminURL().WithPath("/oidc/v1/login").String())
			require.NoError(t, err)
			require.Equal(t, http.StatusFound, resp.StatusCode)

			// Find by name; in shared-process multi-tenant mode the tenant cookie precedes oidc_secret.
			var stateCookie *http.Cookie
			for _, c := range resp.Cookies() {
				if c.Name == secretCookieName {
					stateCookie = c
					break
				}
			}
			require.NotNil(t, stateCookie, "expected oidc_secret cookie")
			loc, err := url.Parse(resp.Header.Get("Location"))
			require.NoError(t, err)
			state := loc.Query().Get("state")

			// Step 2: simulate the IdP redirect to /oidc/v1/callback.
			cbReq, _ := http.NewRequest("GET",
				app.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
			cbReq.AddCookie(stateCookie)
			q := cbReq.URL.Query()
			q.Set("state", state)
			q.Set("code", "dummy")
			cbReq.URL.RawQuery = q.Encode()

			cbResp, err := client.Do(cbReq)
			require.NoError(t, err)

			expStatus := tc.expectedStatus
			if expStatus == 0 {
				expStatus = http.StatusTemporaryRedirect
			}
			require.Equal(t, expStatus, cbResp.StatusCode)

			if tc.wantErrSubstring != "" {
				body, _ := io.ReadAll(cbResp.Body)
				require.Contains(t, string(body), tc.wantErrSubstring)
			}

			rows := sqlDB.QueryStr(t,
				`SELECT role FROM system.role_members WHERE member = $1 ORDER BY role`, testUser)
			actualRoles := make([]string, len(rows))
			for i, r := range rows {
				actualRoles[i] = r[0]
			}
			require.ElementsMatch(t, tc.expectedRoles, actualRoles)
		})
	}
}

// TestOIDCAuthorization_UserinfoPaths exercises the fallback that fetches the
// groups list from the provider's /userinfo endpoint when the ID token and
// access token contain no usable claim.
//
// The test uses a single shared mock OIDC provider for the 5 cases that share
// the same discovery document (all with a userinfo_endpoint). Between cases,
// only the /userinfo response body changes — no cluster settings are modified,
// so no OIDC manager reinitialization occurs. The 2 cases that need different
// discovery documents (absent endpoint, network error) run separately with
// their own mock servers.
func TestOIDCAuthorization_UserinfoPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	app := s.ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE USER `+testUser)
	sqlDB.Exec(t, `CREATE ROLE `+roleOwners)
	sqlDB.Exec(t, `CREATE ROLE `+roleUsers)
	sqlDB.Exec(t, `CREATE ROLE `+roleAuditors)

	rpc := app.NewClientRPCContext(ctx, username.TestUserName())
	cl, err := rpc.GetHTTPClient()
	require.NoError(t, err)
	// The default 10s timeout is too short under race detection where OIDC
	// callback processing can take 20s+.
	cl.Timeout = 45 * time.Second
	cl.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	// Create an RSA key pair for signing and verifying tokens.
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwkKey, err := jwk.FromRaw(privateKey)
	require.NoError(t, err)
	_ = jwkKey.Set(jwk.KeyIDKey, "test-key-id")
	_ = jwkKey.Set(jwk.AlgorithmKey, jwa.RS256)

	publicKey, err := jwk.PublicKeyOf(jwkKey)
	require.NoError(t, err)
	jwks := jwk.NewSet()
	_ = jwks.AddKey(publicKey)

	// Mutable state for /userinfo responses, protected by a mutex.
	var mu syncutil.Mutex
	var userinfoStatus int
	var userinfoBody string

	// Shared mock OIDC provider for cases with a userinfo endpoint.
	var ts *httptest.Server
	ts = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			doc := fmt.Sprintf(`{
			  "issuer": "%s",
			  "token_endpoint": "%s/token",
			  "userinfo_endpoint": "%s/userinfo",
			  "jwks_uri": "%s/.well-known/jwks.json"
			}`, ts.URL, ts.URL, ts.URL, ts.URL)
			_, _ = io.WriteString(w, doc)
		case "/.well-known/jwks.json":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(jwks)
		case "/userinfo":
			mu.Lock()
			s, b := userinfoStatus, userinfoBody
			mu.Unlock()
			w.WriteHeader(s)
			_, _ = io.WriteString(w, b)
		case "/token":
			idTok := makeJWT(t, map[string]any{
				"iss":   ts.URL,
				"email": testUser + "@example.com",
				"aud":   "client",
				"exp":   time.Now().Add(time.Hour).Unix(),
			}, jwkKey)
			w.Header().Set("Content-Type", "application/json")
			fmt.Fprintf(w, `{
			  "access_token":"dummy-access",
			  "id_token":"%s",
			  "token_type":"Bearer",
			  "expires_in":3600
			}`, idTok)
		default:
			http.NotFound(w, r)
		}
	}))
	defer ts.Close()

	// Set all cluster settings once. Provider URL and redirect are set first
	// (while OIDC is disabled, so reloadConfig is a no-op for manager
	// creation). OIDCEnabled is set last, triggering exactly one successful
	// provider discovery call.
	st := s.ClusterSettings()
	OIDCProviderURL.Override(ctx, &st.SV, ts.URL)
	OIDCRedirectURL.Override(ctx, &st.SV, ts.URL+"/callback")
	OIDCClientID.Override(ctx, &st.SV, "client")
	OIDCClientSecret.Override(ctx, &st.SV, "secret")
	OIDCClaimJSONKey.Override(ctx, &st.SV, "email")
	OIDCPrincipalRegex.Override(ctx, &st.SV, "^([^@]+)@.*$")
	OIDCAuthZEnabled.Override(ctx, &st.SV, true)
	OIDCAuthGroupClaim.Override(ctx, &st.SV, "groups")
	OIDCAuthUserinfoGroupKey.Override(ctx, &st.SV, "groups")
	OIDCEnabled.Override(ctx, &st.SV, true)

	revokeStaleRoles := func(t *testing.T) {
		staleRows := sqlDB.QueryStr(t,
			`SELECT role FROM system.role_members WHERE member = $1`, testUser)
		if len(staleRows) > 0 {
			staleRoles := make([]string, len(staleRows))
			for i, r := range staleRows {
				staleRoles[i] = r[0]
			}
			sqlDB.Exec(t, fmt.Sprintf(
				`REVOKE %s FROM %s`, strings.Join(staleRoles, ", "), testUser))
		}
	}

	doLoginCallback := func(t *testing.T) *http.Response {
		resp, err := cl.Get(app.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		// Find by name; in shared-process multi-tenant mode the tenant
		// cookie precedes oidc_secret.
		var cookie *http.Cookie
		for _, c := range resp.Cookies() {
			if c.Name == secretCookieName {
				cookie = c
				break
			}
		}
		require.NotNil(t, cookie, "expected oidc_secret cookie")
		loc, _ := url.Parse(resp.Header.Get("Location"))
		state := loc.Query().Get("state")

		cb, _ := http.NewRequest("GET",
			app.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
		cb.AddCookie(cookie)
		q := cb.URL.Query()
		q.Set("state", state)
		q.Set("code", "dummy")
		cb.URL.RawQuery = q.Encode()
		cbResp, err := cl.Do(cb)
		require.NoError(t, err)
		return cbResp
	}

	checkRoles := func(t *testing.T, wantRoles []string) {
		rows := sqlDB.QueryStr(t, fmt.Sprintf(
			`SELECT role FROM system.role_members WHERE member = '%s' ORDER BY role`,
			testUser))
		var got []string
		for _, r := range rows {
			got = append(got, r[0])
		}
		require.ElementsMatch(t, wantRoles, got)
	}

	// Cases that share the mock server — only the /userinfo response varies.
	type tc struct {
		name           string
		userinfoStatus int
		userinfoBody   string
		wantRoles      []string // nil => expect 403
		wantErr        bool
	}

	sharedCases := []tc{
		{
			name:           "userinfo success",
			userinfoStatus: http.StatusOK,
			userinfoBody:   fmt.Sprintf(`{"groups":["%s","%s"]}`, roleOwners, roleUsers),
			wantRoles:      []string{roleOwners, roleUsers},
		},
		{
			name:           "userinfo empty groups list",
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{"groups":[]}`,
			wantErr:        true,
		},
		{
			name:           "userinfo non-standard body",
			userinfoStatus: http.StatusOK,
			userinfoBody:   `this is not json`,
			wantErr:        true,
		},
		{
			name:           "userinfo missing groups claim",
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{"email":"test@example.com"}`,
			wantErr:        true,
		},
		{
			name:           "userinfo invalid groups claim",
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{"groups":not-a-list}`,
			wantErr:        true,
		},
	}

	for _, tc := range sharedCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			revokeStaleRoles(t)

			mu.Lock()
			userinfoStatus = tc.userinfoStatus
			userinfoBody = tc.userinfoBody
			mu.Unlock()

			cbResp := doLoginCallback(t)

			wantStatus := http.StatusTemporaryRedirect
			if tc.wantErr {
				wantStatus = http.StatusForbidden
			}
			require.Equal(t, wantStatus, cbResp.StatusCode)
			checkRoles(t, tc.wantRoles)
		})
	}

	// Special case: discovery document has no userinfo_endpoint. The provider
	// cannot fall back to userinfo, so the login must be denied.
	t.Run("userinfo endpoint absent", func(t *testing.T) {
		revokeStaleRoles(t)

		var absentTS *httptest.Server
		absentTS = httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					doc := fmt.Sprintf(`{
					  "issuer": "%s",
					  "token_endpoint": "%s/token",
					  "jwks_uri": "%s/.well-known/jwks.json"
					}`, absentTS.URL, absentTS.URL, absentTS.URL)
					_, _ = io.WriteString(w, doc)
				case "/.well-known/jwks.json":
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(jwks)
				case "/token":
					idTok := makeJWT(t, map[string]any{
						"iss":   absentTS.URL,
						"email": testUser + "@example.com",
						"aud":   "client",
						"exp":   time.Now().Add(time.Hour).Unix(),
					}, jwkKey)
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintf(w, `{
					  "access_token":"dummy-access",
					  "id_token":"%s",
					  "token_type":"Bearer",
					  "expires_in":3600
					}`, idTok)
				default:
					http.NotFound(w, r)
				}
			}))
		defer absentTS.Close()

		OIDCProviderURL.Override(ctx, &st.SV, absentTS.URL)
		OIDCRedirectURL.Override(ctx, &st.SV, absentTS.URL+"/callback")
		// Restore the shared server URLs before this subtest returns. Otherwise
		// the cluster settings would point at absentTS.URL after defer
		// absentTS.Close() runs, leaving any subsequent reloadConfig() trigger
		// to perform HTTP discovery against a dead address. Restoring also
		// avoids prolonging mutex contention on the OIDC manager across
		// subtests.
		defer func() {
			OIDCProviderURL.Override(ctx, &st.SV, ts.URL)
			OIDCRedirectURL.Override(ctx, &st.SV, ts.URL+"/callback")
		}()

		cbResp := doLoginCallback(t)
		require.Equal(t, http.StatusForbidden, cbResp.StatusCode)
		checkRoles(t, nil)
	})

	// Special case: discovery document points userinfo_endpoint to an
	// unreachable address. The network error must be handled gracefully.
	t.Run("userinfo network error", func(t *testing.T) {
		revokeStaleRoles(t)

		var netErrTS *httptest.Server
		netErrTS = httptest.NewServer(http.HandlerFunc(
			func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					doc := fmt.Sprintf(`{
					  "issuer": "%s",
					  "token_endpoint": "%s/token",
					  "userinfo_endpoint": "http://127.0.0.1:0/userinfo",
					  "jwks_uri": "%s/.well-known/jwks.json"
					}`, netErrTS.URL, netErrTS.URL, netErrTS.URL)
					_, _ = io.WriteString(w, doc)
				case "/.well-known/jwks.json":
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(jwks)
				case "/token":
					idTok := makeJWT(t, map[string]any{
						"iss":   netErrTS.URL,
						"email": testUser + "@example.com",
						"aud":   "client",
						"exp":   time.Now().Add(time.Hour).Unix(),
					}, jwkKey)
					w.Header().Set("Content-Type", "application/json")
					fmt.Fprintf(w, `{
					  "access_token":"dummy-access",
					  "id_token":"%s",
					  "token_type":"Bearer",
					  "expires_in":3600
					}`, idTok)
				default:
					http.NotFound(w, r)
				}
			}))
		defer netErrTS.Close()

		OIDCProviderURL.Override(ctx, &st.SV, netErrTS.URL)
		OIDCRedirectURL.Override(ctx, &st.SV, netErrTS.URL+"/callback")
		// Restore the shared server URLs before this subtest returns. See the
		// matching comment in "userinfo endpoint absent" for rationale.
		defer func() {
			OIDCProviderURL.Override(ctx, &st.SV, ts.URL)
			OIDCRedirectURL.Override(ctx, &st.SV, ts.URL+"/callback")
		}()

		cbResp := doLoginCallback(t)
		require.Equal(t, http.StatusForbidden, cbResp.StatusCode)
		checkRoles(t, nil)
	})
}

// TestOIDCAuthorization_RoleGrantAndRevoke tests that roles are granted and revoked as expected
// after a successful OIDC login. This test is modeled after TestLDAPRolesAreGranted.
func TestOIDCAuthorization_RoleGrantAndRevoke(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	// Create an RSA key pair for signing tokens.
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwkKey, err := jwk.FromRaw(privateKey)
	require.NoError(t, err)

	// Start a brand-new in-process node for this sub-test.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	app := s.ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(db)

	// Create the user and any roles that might be granted.
	sqlDB.Exec(t, `CREATE USER `+testUser)
	sqlDB.Exec(t, `CREATE ROLE `+roleOwners)
	sqlDB.Exec(t, `CREATE ROLE `+roleUsers)
	sqlDB.Exec(t, `CREATE ROLE `+roleAuditors)

	// Replace the factory so that the server uses our mock manager.
	origFactory := NewOIDCManager
	t.Cleanup(func() { NewOIDCManager = origFactory })

	// Helper to check role membership.
	checkHasRole := func(t *testing.T, user, role string, shouldHave bool) {
		var hasRole bool
		sqlDB.QueryRow(t, fmt.Sprintf("SELECT pg_has_role('%s', '%s', 'MEMBER')", user, role)).Scan(&hasRole)
		require.Equal(t, shouldHave, hasRole)
	}

	// A helper function to simulate an OIDC login and callback.
	performOIDCLogin := func(t *testing.T, client *http.Client, claims map[string]any, expectedStatus int) {
		NewOIDCManager = func(_ context.Context, conf oidcAuthenticationConf,
			redirectURL string, scopes []string) (IOIDCManager, error) {

			cfg := &oauth2.Config{
				ClientID:     conf.clientID,
				ClientSecret: conf.clientSecret,
				RedirectURL:  redirectURL,
				Endpoint:     oauth2.Endpoint{AuthURL: "https://provider.example.com/auth"},
				Scopes:       scopes,
			}
			return &mockManager{
				cfg:         cfg,
				rawIDToken:  makeJWT(t, claims, jwkKey),
				accessToken: makeJWT(t, nil, jwkKey),
				email:       testUser + "@example.com",
			}, nil
		}

		// Enable OIDC plus authorization.
		st := s.ClusterSettings()
		OIDCProviderURL.Override(ctx, &st.SV, fmt.Sprintf("https://provider.example.com/%s", t.Name()))
		OIDCClientID.Override(ctx, &st.SV, "client")
		OIDCClientSecret.Override(ctx, &st.SV, "secret")
		OIDCRedirectURL.Override(ctx, &st.SV, "https://cockroachlabs.com/oidc/v1/callback")
		OIDCClaimJSONKey.Override(ctx, &st.SV, "email")
		OIDCPrincipalRegex.Override(ctx, &st.SV, "^([^@]+)@[^@]+$")
		OIDCEnabled.Override(ctx, &st.SV, true)
		OIDCAuthZEnabled.Override(ctx, &st.SV, true)
		OIDCAuthGroupClaim.Override(ctx, &st.SV, "groups")

		// Step 1: hit /oidc/v1/login and capture the state cookie.
		resp, err := client.Get(app.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		require.Equal(t, http.StatusFound, resp.StatusCode)

		// Find by name; in shared-process multi-tenant mode the tenant cookie precedes oidc_secret.
		var stateCookie *http.Cookie
		for _, c := range resp.Cookies() {
			if c.Name == secretCookieName {
				stateCookie = c
				break
			}
		}
		require.NotNil(t, stateCookie, "expected oidc_secret cookie")
		loc, err := url.Parse(resp.Header.Get("Location"))
		require.NoError(t, err)
		state := loc.Query().Get("state")

		// Step 2: simulate the IdP redirect to /oidc/v1/callback.
		// Use retry logic to handle transient timing issues during stress tests.
		cbReq, _ := http.NewRequest("GET",
			app.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
		cbReq.AddCookie(stateCookie)
		q := cbReq.URL.Query()
		q.Set("state", state)
		q.Set("code", "dummy")
		cbReq.URL.RawQuery = q.Encode()

		// Retry the callback request with an exponential backoff to handle timing issues
		// that can occur during stress runs or with race detection enabled.
		const maxRetries = 3
		var cbResp *http.Response
		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				// Exponential backoff: 100ms, 200ms
				backoff := time.Duration(100<<uint(attempt-1)) * time.Millisecond
				t.Logf("Retrying OIDC callback after %v (attempt %d/%d)", backoff, attempt+1, maxRetries)
				time.Sleep(backoff)
			}

			cbResp, err = client.Do(cbReq)
			if err == nil {
				// Success
				break
			}

			lastErr = err
			// Check if this is a timeout error that we should retry
			var urlErr *url.Error
			if errors.As(err, &urlErr) && (urlErr.Timeout() || urlErr.Temporary()) {
				t.Logf("OIDC callback timeout/temporary error (attempt %d/%d): %v", attempt+1, maxRetries, err)
				continue
			}

			// Non-retriable error, fail fast
			break
		}

		if lastErr != nil && err != nil {
			// All retries exhausted or non-retriable error
			t.Fatalf("OIDC callback failed after %d attempts: %v\n"+
				"Note: This timeout does not indicate a security or functional failure. "+
				"The OIDC authorization logic still properly fails unauthorized requests. "+
				"This is a test timing issue, likely due to system load during stress testing.",
				maxRetries, lastErr)
		}

		require.NoError(t, err, "OIDC callback request should succeed")
		require.Equal(t, expectedStatus, cbResp.StatusCode)
	}

	// Build an HTTP client that trusts the node certificates.
	rpcCtx := app.NewClientRPCContext(ctx, username.TestUserName())
	client, err := rpcCtx.GetHTTPClient()
	require.NoError(t, err)
	// The default 10s timeout is too short under race detection where OIDC
	// callback processing can take 20s+.
	client.Timeout = 45 * time.Second
	// Prevent automatic redirects so we can capture cookies and headers.
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	// Initially, the user should have no roles.
	checkHasRole(t, testUser, roleOwners, false)
	checkHasRole(t, testUser, roleUsers, false)

	t.Run("grant one role", func(t *testing.T) {
		claims := map[string]any{"groups": []string{roleOwners}}
		performOIDCLogin(t, &client, claims, http.StatusTemporaryRedirect)
		checkHasRole(t, testUser, roleOwners, true)
		checkHasRole(t, testUser, roleUsers, false)
	})

	t.Run("grant another role", func(t *testing.T) {
		claims := map[string]any{"groups": []string{roleOwners, roleUsers}}
		performOIDCLogin(t, &client, claims, http.StatusTemporaryRedirect)
		checkHasRole(t, testUser, roleOwners, true)
		checkHasRole(t, testUser, roleUsers, true)
	})

	t.Run("revoke one role", func(t *testing.T) {
		claims := map[string]any{"groups": []string{roleUsers}}
		performOIDCLogin(t, &client, claims, http.StatusTemporaryRedirect)
		checkHasRole(t, testUser, roleOwners, false)
		checkHasRole(t, testUser, roleUsers, true)
	})

	t.Run("grant non-existent role", func(t *testing.T) {
		claims := map[string]any{"groups": []string{roleUsers, "non_existent_role"}}
		performOIDCLogin(t, &client, claims, http.StatusTemporaryRedirect)
		// The non-existent role is skipped, should only have roleUsers
		checkHasRole(t, testUser, roleUsers, true)
		checkHasRole(t, testUser, roleOwners, false)
	})
}
