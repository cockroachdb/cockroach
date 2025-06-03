// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

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
	"github.com/cockroachdb/errors"
	"github.com/coreos/go-oidc"
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
			// Prevent automatic redirects so we can capture cookies and headers.
			client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

			// Step 1: hit /oidc/v1/login and capture the state cookie.
			resp, err := client.Get(app.AdminURL().WithPath("/oidc/v1/login").String())
			require.NoError(t, err)
			require.Equal(t, http.StatusFound, resp.StatusCode)

			stateCookie := resp.Cookies()[0]
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

			// Check which SQL roles were granted to the user.
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
func TestOIDCAuthorization_UserinfoPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	type tc struct {
		name           string
		discoveryDoc   string
		userinfoStatus int
		userinfoBody   string
		wantRoles      []string // nil => expect 403
		wantErr        bool
	}

	cases := []tc{
		{
			name: "userinfo success",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "userinfo_endpoint": "{{url}}/userinfo",
			  "jwks_uri": "{{url}}/.well-known/jwks.json"
			}`,
			userinfoStatus: http.StatusOK,
			userinfoBody:   fmt.Sprintf(`{"groups":["%s","%s"]}`, roleOwners, roleUsers),
			wantRoles:      []string{roleOwners, roleUsers},
		},
		{
			name: "userinfo endpoint absent",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "jwks_uri": "{{url}}/.well-known/jwks.json"
			}`,
			// no userinfo: should end in forbidden
			wantRoles: nil,
			wantErr:   true,
		},
		{
			name: "userinfo network error",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "userinfo_endpoint": "http://127.0.0.1:0/userinfo",
			  "jwks_uri": "{{url}}/.well-known/jwks.json"
			}`,
			wantRoles: nil,
			wantErr:   true,
		},
		{
			name: "userinfo empty groups list",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "userinfo_endpoint": "{{url}}/userinfo",
			  "jwks_uri": "{{url}}/.well-known/jwks.json"
			}`,
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{"groups":[]}`,
			wantRoles:      nil, // Roles should be revoked, login denied.
			wantErr:        true,
		},
		{
			name: "userinfo non-standard body",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "userinfo_endpoint": "{{url}}/userinfo",
			  "jwks_uri": "{{url}}/.well-known/jwks.json"
			}`,
			userinfoStatus: http.StatusOK,
			userinfoBody:   `this is not json`,
			wantRoles:      nil,
			wantErr:        true,
		},
		{
			name: "userinfo missing groups claim",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "userinfo_endpoint": "{{url}}/userinfo",
			  "jwks_uri": "{{url}}/.well-known/jwks.json"
			}`,
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{"email":"test@example.com"}`,
			wantRoles:      nil,
			wantErr:        true,
		},
		{
			name: "userinfo invalid groups claim",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "userinfo_endpoint": "{{url}}/userinfo",
			  "jwks_uri": "{{url}}/.well-known/jwks.json"
			}`,
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{"groups":not-a-list}`,
			wantRoles:      nil,
			wantErr:        true,
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Create an RSA key pair for signing and verifying tokens.
			privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
			require.NoError(t, err)
			jwkKey, err := jwk.FromRaw(privateKey)
			require.NoError(t, err)
			_ = jwkKey.Set(jwk.KeyIDKey, "test-key-id")
			_ = jwkKey.Set(jwk.AlgorithmKey, jwa.RS256)

			// The public key is what the verifier will use.
			publicKey, err := jwk.PublicKeyOf(jwkKey)
			require.NoError(t, err)
			jwks := jwk.NewSet()
			_ = jwks.AddKey(publicKey)

			var ts *httptest.Server
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					doc := strings.ReplaceAll(tc.discoveryDoc, "{{url}}", ts.URL)
					_, _ = io.WriteString(w, doc)
				case "/.well-known/jwks.json":
					w.Header().Set("Content-Type", "application/json")
					err := json.NewEncoder(w).Encode(jwks)
					require.NoError(t, err)
				case "/userinfo":
					w.WriteHeader(tc.userinfoStatus)
					_, _ = io.WriteString(w, tc.userinfoBody)
				case "/token":
					idTok := makeJWT(t, map[string]any{
						"iss":   ts.URL, // issuer must match provider URL
						"email": testUser + "@example.com",
						"aud":   "client",
						"exp":   time.Now().Add(time.Hour).Unix(),
					}, jwkKey)
					resp := `{
					  "access_token":"dummy-access",
					  "id_token":"` + idTok + `",
					  "token_type":"Bearer",
					  "expires_in":3600
					}`
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, resp)
				default:
					http.NotFound(w, r)
				}
			})
			ts = httptest.NewServer(handler)
			defer ts.Close()

			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			app := s.ApplicationLayer()
			sqlDB := sqlutils.MakeSQLRunner(db)
			sqlDB.Exec(t, `CREATE USER `+testUser)
			sqlDB.Exec(t, `CREATE ROLE `+roleOwners)
			sqlDB.Exec(t, `CREATE ROLE `+roleUsers)
			sqlDB.Exec(t, `CREATE ROLE `+roleAuditors)

			st := s.ClusterSettings()
			OIDCProviderURL.Override(ctx, &st.SV, ts.URL)
			OIDCClientID.Override(ctx, &st.SV, "client")
			OIDCClientSecret.Override(ctx, &st.SV, "secret")
			OIDCRedirectURL.Override(ctx, &st.SV, ts.URL+"/callback")
			OIDCClaimJSONKey.Override(ctx, &st.SV, "email")
			OIDCPrincipalRegex.Override(ctx, &st.SV, "^([^@]+)@.*$")
			OIDCEnabled.Override(ctx, &st.SV, true)
			OIDCAuthZEnabled.Override(ctx, &st.SV, true)
			OIDCAuthGroupClaim.Override(ctx, &st.SV, "groups")
			OIDCAuthUserinfoGroupKey.Override(ctx, &st.SV, "groups")

			rpc := app.NewClientRPCContext(ctx, username.TestUserName())
			cl, err := rpc.GetHTTPClient()
			require.NoError(t, err)
			cl.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

			resp, err := cl.Get(app.AdminURL().WithPath("/oidc/v1/login").String())
			require.NoError(t, err)
			cookie := resp.Cookies()[0]
			loc, _ := url.Parse(resp.Header.Get("Location"))
			state := loc.Query().Get("state")

			cb, _ := http.NewRequest("GET", app.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
			cb.AddCookie(cookie)
			q := cb.URL.Query()
			q.Set("state", state)
			q.Set("code", "dummy")
			cb.URL.RawQuery = q.Encode()
			cbResp, err := cl.Do(cb)
			require.NoError(t, err)

			wantStatus := http.StatusTemporaryRedirect
			if tc.wantErr {
				// The two error cases should now result in a Forbidden, because the
				// userinfo fallback will fail gracefully but no groups will be found.
				wantStatus = http.StatusForbidden
			}
			require.Equal(t, wantStatus, cbResp.StatusCode)

			rows := sqlDB.QueryStr(t, fmt.Sprintf(`SELECT role FROM system.role_members WHERE member = '%s' ORDER BY role`, testUser))
			var got []string
			for _, r := range rows {
				got = append(got, r[0])
			}
			require.ElementsMatch(t, tc.wantRoles, got)
		})
	}
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

		stateCookie := resp.Cookies()[0]
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
		require.Equal(t, expectedStatus, cbResp.StatusCode)
	}

	// Build an HTTP client that trusts the node certificates.
	rpcCtx := app.NewClientRPCContext(ctx, username.TestUserName())
	client, err := rpcCtx.GetHTTPClient()
	require.NoError(t, err)
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
