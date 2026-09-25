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
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
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

// mockManager is a configurable IOIDCManager used by the HTTP callback path.
// The tokens it returns from ExchangeVerifyGetTokenInfo are injected via
// setTokens and read under mu, so a single shared instance can be reconfigured
// between callback subtests.
type mockManager struct {
	mu          syncutil.Mutex
	cfg         *oauth2.Config
	rawIDToken  string
	accessToken string
	email       string
}

func (m *mockManager) setConfig(cfg *oauth2.Config) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg
}

func (m *mockManager) setTokens(rawIDToken, accessToken, email string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.rawIDToken = rawIDToken
	m.accessToken = accessToken
	m.email = email
}

func (*mockManager) Verify(context.Context, string) (*oidc.IDToken, error) {
	return nil, nil
}

func (*mockManager) Exchange(
	context.Context, string, ...oauth2.AuthCodeOption,
) (*oauth2.Token, error) {
	return nil, nil
}

func (m *mockManager) AuthCodeURL(state string, opts ...oauth2.AuthCodeOption) string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.AuthCodeURL(state, opts...)
}

func (m *mockManager) ExchangeVerifyGetTokenInfo(
	ctx context.Context, _, _ string, _ bool,
) (map[string]json.RawMessage, map[string]json.RawMessage, string, string, error) {
	m.mu.Lock()
	rawIDToken, accessToken, email := m.rawIDToken, m.accessToken, m.email
	m.mu.Unlock()

	if rawIDToken == "" {
		// The real implementation would fail to extract the ID token from the
		// credentials and return an error.
		return nil, nil, "", "",
			errors.New("OIDC: required id_token not found in credentials")
	}

	var idTokenClaims map[string]json.RawMessage
	// The real implementation calls Verify, which will fail on a non-JWT token.
	// For this test, we allow unparseable tokens to test the `authorize` function's
	// handling of them. We still need to extract claims if possible for the
	// `extractUsernameFromClaims` call that happens before `authorize`.
	if tok, err := jwt.ParseInsecure([]byte(rawIDToken)); err == nil {
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
		idTokenClaims["email"] = json.RawMessage(`"` + email + `"`)
	}

	var accessTokenClaims map[string]json.RawMessage
	if accessToken != "" {
		if tok, err := jwt.ParseInsecure([]byte(accessToken)); err == nil {
			if claimsMap, err := tok.AsMap(ctx); err == nil {
				if jsonBytes, err := json.Marshal(claimsMap); err == nil {
					_ = json.Unmarshal(jsonBytes, &accessTokenClaims)
				}
			}
		}
	}
	return idTokenClaims, accessTokenClaims, rawIDToken, accessToken, nil
}

func (*mockManager) UserInfo(context.Context, oauth2.TokenSource) (*oidc.UserInfo, error) {
	// This mock is not intended to handle UserInfo calls. Returning an error
	// prevents a nil pointer dereference in the calling code.
	return nil, errors.New("UserInfo not implemented in this mock")
}

var _ IOIDCManager = (*mockManager)(nil)

func TestOIDCAuthorization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Create an RSA key pair for signing tokens.
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	jwkKey, err := jwk.FromRaw(privateKey)
	require.NoError(t, err)
	// makeToken signs a JWT carrying the given groups claim. An empty slice
	// yields a present-but-empty claim, distinct from noGroupsToken below, which
	// omits the claim entirely.
	makeToken := func(groups any) string {
		return makeJWT(t, map[string]any{"groups": groups}, jwkKey)
	}
	noGroupsToken := makeJWT(t, map[string]any{}, jwkKey)

	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)
	app := s.ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(app.SQLConn(t))
	execCfg := app.ExecutorConfig().(sql.ExecutorConfig)
	sqlDB.Exec(t, `CREATE ROLE `+roleOwners)
	sqlDB.Exec(t, `CREATE ROLE `+roleUsers)
	sqlDB.Exec(t, `CREATE ROLE `+roleAuditors)

	getRoles := func(t *testing.T, user string) []string {
		t.Helper()
		rows := sqlDB.QueryStr(t,
			`SELECT role FROM system.role_members WHERE member = $1 ORDER BY role`, user)
		roles := make([]string, len(rows))
		for i := range rows {
			roles[i] = rows[i][0]
		}
		return roles
	}
	createUser := func(t *testing.T, user string, initialRoles ...string) {
		t.Helper()
		sqlDB.Exec(t, `CREATE USER `+user)
		if len(initialRoles) > 0 {
			sqlDB.Exec(t, fmt.Sprintf(
				`GRANT %s TO %s`, strings.Join(initialRoles, ", "), user))
		}
	}
	newAuthServer := func(manager IOIDCManager, userinfoGroupKey string) *oidcAuthenticationServer {
		return &oidcAuthenticationServer{
			conf: oidcAuthenticationConf{
				authZEnabled:     true,
				groupClaim:       "groups",
				userinfoGroupKey: userinfoGroupKey,
			},
			manager: manager,
			execCfg: &execCfg,
		}
	}

	mock := &mockManager{}
	realNewOIDCManager := NewOIDCManager
	defer testutils.TestingHook(
		&NewOIDCManager,
		func(_ context.Context, conf oidcAuthenticationConf, redirectURL string, scopes []string,
		) (IOIDCManager, error) {
			mock.setConfig(&oauth2.Config{
				ClientID:     conf.clientID,
				ClientSecret: conf.clientSecret,
				RedirectURL:  redirectURL,
				Endpoint: oauth2.Endpoint{
					AuthURL: "https://provider.example.com/auth",
				},
				Scopes: scopes,
			})
			return mock, nil
		},
	)()

	// Set all cluster settings once. Provider URL and redirect are set first
	// (while OIDC is disabled, so reloadConfig is a no-op for manager
	// creation). OIDCEnabled is set last, triggering exactly one manager
	// creation.
	st := s.ClusterSettings()
	OIDCProviderURL.Override(ctx, &st.SV, "https://provider.example.com")
	OIDCClientID.Override(ctx, &st.SV, "client")
	OIDCClientSecret.Override(ctx, &st.SV, "secret")
	OIDCRedirectURL.Override(ctx, &st.SV, "https://cockroachlabs.com/oidc/v1/callback")
	OIDCClaimJSONKey.Override(ctx, &st.SV, "email")
	OIDCPrincipalRegex.Override(ctx, &st.SV, "^([^@]+)@[^@]+$")
	OIDCAuthZEnabled.Override(ctx, &st.SV, true)
	OIDCAuthGroupClaim.Override(ctx, &st.SV, "groups")
	OIDCAuthUserinfoGroupKey.Override(ctx, &st.SV, "groups")
	OIDCEnabled.Override(ctx, &st.SV, true)

	t.Run("token paths", func(t *testing.T) {
		authServer := newAuthServer(mock, "groups")
		testCases := []struct {
			name          string
			idToken       string
			accessToken   string
			expectedRoles []string
			expectedError string
		}{
			{
				name:          "groups in ID token",
				idToken:       makeToken([]string{roleOwners, roleUsers}),
				accessToken:   noGroupsToken,
				expectedRoles: []string{roleOwners, roleUsers},
			},
			{
				name:          "groups in access token only",
				idToken:       noGroupsToken,
				accessToken:   makeToken([]string{roleOwners}),
				expectedRoles: []string{roleOwners},
			},
			{
				name:          "groups in both tokens with distinct sets",
				idToken:       makeToken([]string{roleOwners, roleAuditors}),
				accessToken:   makeToken([]string{roleUsers}),
				expectedRoles: []string{roleOwners, roleUsers, roleAuditors},
			},
			{
				name:          "groups in both tokens with overlap",
				idToken:       makeToken([]string{roleOwners, roleUsers}),
				accessToken:   makeToken([]string{roleUsers, roleAuditors}),
				expectedRoles: []string{roleOwners, roleUsers, roleAuditors},
			},
			{
				name:          "groups claim absent from both tokens",
				idToken:       noGroupsToken,
				accessToken:   noGroupsToken,
				expectedError: "no groups claim found",
			},
			{
				name:          "empty groups list in ID token",
				idToken:       makeToken([]string{}),
				accessToken:   noGroupsToken,
				expectedError: "groups list was empty",
			},
			{
				name:          "empty groups list in access token",
				idToken:       noGroupsToken,
				accessToken:   makeToken([]string{}),
				expectedError: "groups list was empty",
			},
			{
				name:          "unparseable ID token with groups in access token",
				idToken:       "this is not a jwt",
				accessToken:   makeToken([]string{roleUsers}),
				expectedRoles: []string{roleUsers},
			},
			{
				name:          "groups in ID token with unparseable access token",
				idToken:       makeToken([]string{roleOwners}),
				accessToken:   "this is not a jwt either",
				expectedRoles: []string{roleOwners},
			},
			{
				name:          "unparseable tokens with no groups",
				idToken:       "foo",
				accessToken:   "bar",
				expectedError: "no groups claim found",
			},
			{
				name:          "missing access token",
				idToken:       makeToken([]string{roleOwners}),
				expectedError: "required access_token missing",
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				user := fmt.Sprintf("token_user_%d", i)
				createUser(t, user)
				err := authServer.authorize(ctx, authServer.conf, authServer.manager, tc.idToken, tc.accessToken, user)
				if tc.expectedError == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tc.expectedError)
				}
				require.ElementsMatch(t, tc.expectedRoles, getRoles(t, user))
			})
		}
	})

	var userinfoMu syncutil.Mutex
	var currentUserinfoBody string
	newProviderManager := func(
		t *testing.T, userinfoURL string, serveUserinfo bool,
	) (IOIDCManager, func()) {
		t.Helper()
		var provider *httptest.Server
		provider = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/.well-known/openid-configuration":
				doc := map[string]any{
					"issuer":                 provider.URL,
					"authorization_endpoint": provider.URL + "/authorize",
					"token_endpoint":         provider.URL + "/token",
					"jwks_uri":               provider.URL + "/jwks",
				}
				if serveUserinfo {
					endpoint := userinfoURL
					if endpoint == "" {
						endpoint = provider.URL + "/userinfo"
					}
					doc["userinfo_endpoint"] = endpoint
				}
				_ = json.NewEncoder(w).Encode(doc)
			case "/userinfo":
				userinfoMu.Lock()
				body := currentUserinfoBody
				userinfoMu.Unlock()
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, body)
			default:
				http.NotFound(w, r)
			}
		}))
		manager, err := realNewOIDCManager(
			ctx,
			oidcAuthenticationConf{
				providerURL: provider.URL,
				clientID:    "client",
				httpClient:  httputil.NewClientWithTimeout(5 * time.Second),
			},
			"https://cockroachlabs.com/oidc/v1/callback",
			[]string{"openid"},
		)
		require.NoError(t, err)
		return manager, provider.Close
	}

	t.Run("userinfo paths", func(t *testing.T) {
		// Shared mock OIDC provider for cases with a userinfo endpoint.
		manager, closeProvider := newProviderManager(t, "", true)
		defer closeProvider()
		authServer := newAuthServer(manager, "groups")

		// Cases that share the mock server — only the /userinfo response varies.
		testCases := []struct {
			name          string
			idToken       string
			userinfoBody  string
			initialRoles  []string
			expectedRoles []string
			expectedError string
		}{
			{
				name:          "userinfo success",
				idToken:       noGroupsToken,
				userinfoBody:  `{"groups":["owners","users"]}`,
				expectedRoles: []string{roleOwners, roleUsers},
			},
			{
				name:          "userinfo empty groups list",
				idToken:       noGroupsToken,
				userinfoBody:  `{"groups":[]}`,
				expectedError: "groups list was empty",
			},
			{
				name:          "userinfo non-standard body",
				idToken:       noGroupsToken,
				userinfoBody:  `this is not json`,
				expectedError: "no groups claim found",
			},
			{
				name:          "userinfo missing groups claim",
				idToken:       noGroupsToken,
				userinfoBody:  `{"email":"test@example.com"}`,
				expectedError: "no groups claim found",
			},
			{
				name:          "userinfo invalid groups claim",
				idToken:       noGroupsToken,
				userinfoBody:  `{"groups":42}`,
				expectedError: "no groups claim found",
			},
			{
				// A token that carries an explicitly empty groups claim must still fall back
				// to /userinfo, since some IdPs return an empty array in the token and supply
				// the real groups only at the userinfo endpoint.
				name:          "empty token groups claim falls back to userinfo",
				idToken:       makeToken([]string{}),
				userinfoBody:  `{"groups":["users"]}`,
				expectedRoles: []string{roleUsers},
			},
			{
				// A userinfo 200 that omits the group key means no groups claim was found
				// from any source. The login is denied, but a pre-existing role must survive:
				// a missing claim signals a transient failure, not an explicit empty set.
				name:          "missing groups claim does not revoke existing roles",
				idToken:       noGroupsToken,
				userinfoBody:  `{"email":"test@example.com"}`,
				initialRoles:  []string{roleOwners},
				expectedRoles: []string{roleOwners},
				expectedError: "no groups claim found",
			},
		}

		for i, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				user := fmt.Sprintf("userinfo_user_%d", i)
				createUser(t, user, tc.initialRoles...)
				userinfoMu.Lock()
				currentUserinfoBody = tc.userinfoBody
				userinfoMu.Unlock()

				err := authServer.authorize(ctx, authServer.conf, authServer.manager, tc.idToken, "opaque-access-token", user)
				if tc.expectedError == "" {
					require.NoError(t, err)
				} else {
					require.ErrorContains(t, err, tc.expectedError)
				}
				require.ElementsMatch(t, tc.expectedRoles, getRoles(t, user))
			})
		}

		// Special case: discovery document has no userinfo_endpoint. The provider
		// cannot fall back to userinfo, so the login must be denied.
		t.Run("userinfo endpoint absent", func(t *testing.T) {
			manager, closeProvider := newProviderManager(t, "", false)
			defer closeProvider()
			user := "userinfo_endpoint_absent"
			createUser(t, user)
			authServer := newAuthServer(manager, "groups")
			err := authServer.authorize(
				ctx, authServer.conf, authServer.manager, noGroupsToken, "opaque-access-token", user)
			require.ErrorContains(t, err, "no groups claim found")
			require.Empty(t, getRoles(t, user))
		})

		// Special case: discovery document points userinfo_endpoint to an
		// unreachable address. The network error must be handled gracefully.
		t.Run("userinfo network error", func(t *testing.T) {
			manager, closeProvider := newProviderManager(
				t, "http://127.0.0.1:0/userinfo", true)
			defer closeProvider()
			user := "userinfo_network_error"
			createUser(t, user)
			authServer := newAuthServer(manager, "groups")
			err := authServer.authorize(
				ctx, authServer.conf, authServer.manager, noGroupsToken, "opaque-access-token", user)
			require.ErrorContains(t, err, "no groups claim found")
			require.Empty(t, getRoles(t, user))
		})
	})

	t.Run("role synchronization policy", func(t *testing.T) {
		authServer := newAuthServer(mock, "groups")

		t.Run("incremental grant and revoke", func(t *testing.T) {
			const user = "role_incremental"
			createUser(t, user)
			require.NoError(t, authServer.authorize(ctx, authServer.conf, authServer.manager,
				makeToken([]string{roleOwners}), noGroupsToken, user))
			require.ElementsMatch(t, []string{roleOwners}, getRoles(t, user))
			require.NoError(t, authServer.authorize(ctx, authServer.conf, authServer.manager,
				makeToken([]string{roleOwners, roleUsers}), noGroupsToken, user))
			require.ElementsMatch(t, []string{roleOwners, roleUsers}, getRoles(t, user))
			require.NoError(t, authServer.authorize(ctx, authServer.conf, authServer.manager,
				makeToken([]string{roleUsers}), noGroupsToken, user))
			require.ElementsMatch(t, []string{roleUsers}, getRoles(t, user))
		})

		t.Run("nonexistent role is skipped", func(t *testing.T) {
			const user = "role_nonexistent"
			createUser(t, user)
			require.NoError(t, authServer.authorize(ctx, authServer.conf, authServer.manager,
				makeToken([]string{roleUsers, "non_existent_role"}),
				noGroupsToken, user))
			// The non-existent role is skipped, should only have roleUsers
			require.ElementsMatch(t, []string{roleUsers}, getRoles(t, user))
		})

		t.Run("missing groups claim denies login without revoking roles", func(t *testing.T) {
			// A login whose tokens carry no groups claim (the userinfo fallback also
			// fails, as it would on a transient IdP error) must be denied and must NOT
			// revoke the user's existing roles: a missing claim is not an explicit
			// empty group set.
			const user = "role_missing_claim"
			createUser(t, user, roleOwners)
			err := authServer.authorize(ctx, authServer.conf, authServer.manager, noGroupsToken, noGroupsToken, user)
			require.ErrorContains(t, err, "no groups claim found")
			require.ElementsMatch(t, []string{roleOwners}, getRoles(t, user))
		})

		t.Run("explicit empty groups claim denies login and revokes roles", func(t *testing.T) {
			// An explicit empty groups claim is a policy decision that the user has no
			// roles: the sync revokes all roles and the login is then denied.
			const user = "role_empty_claim"
			createUser(t, user, roleOwners)
			err := authServer.authorize(
				ctx, authServer.conf, authServer.manager, makeToken([]string{}), noGroupsToken, user)
			require.ErrorContains(t, err, "groups list was empty")
			require.Empty(t, getRoles(t, user))
		})
	})

	// Build an HTTP client that trusts the node certificates.
	rpcCtx := app.NewClientRPCContext(ctx, username.TestUserName())
	client, err := rpcCtx.GetHTTPClient()
	require.NoError(t, err)
	// The default 10s timeout is too short under race detection where OIDC
	// callback processing can take 20s+.
	client.Timeout = 45 * time.Second
	// Prevent automatic redirects so we can capture cookies and headers.
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	doLoginCallback := func(t *testing.T) *http.Response {
		t.Helper()
		// Step 1: hit /oidc/v1/login and capture the state cookie.
		resp, err := client.Get(app.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		require.Equal(t, http.StatusFound, resp.StatusCode)
		// Find by name; in shared-process multi-tenant mode the tenant
		// cookie precedes oidc_secret.
		stateCookie := findCookie(resp, secretCookieName)
		require.NotNil(t, stateCookie, "expected oidc_secret cookie")
		location, err := url.Parse(resp.Header.Get("Location"))
		require.NoError(t, err)
		state := location.Query().Get("state")
		require.NoError(t, resp.Body.Close())

		// Step 2: simulate the IdP redirect to /oidc/v1/callback.
		req, err := http.NewRequest(http.MethodGet,
			app.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
		require.NoError(t, err)
		req.AddCookie(stateCookie)
		query := req.URL.Query()
		query.Set("state", state)
		query.Set("code", "dummy")
		req.URL.RawQuery = query.Encode()
		callbackResp, err := client.Do(req)
		require.NoError(t, err)
		return callbackResp
	}

	t.Run("HTTP callback wiring", func(t *testing.T) {
		testCases := []struct {
			name           string
			user           string
			idToken        string
			accessToken    string
			expectedStatus int
			expectedRoles  []string
		}{
			{
				name:           "ID token success maps to redirect",
				user:           "http_id_token",
				idToken:        makeToken([]string{roleOwners}),
				accessToken:    noGroupsToken,
				expectedStatus: http.StatusTemporaryRedirect,
				expectedRoles:  []string{roleOwners},
			},
			{
				name:           "access token success maps to redirect",
				user:           "http_access_token",
				idToken:        noGroupsToken,
				accessToken:    makeToken([]string{roleUsers}),
				expectedStatus: http.StatusTemporaryRedirect,
				expectedRoles:  []string{roleUsers},
			},
			{
				name:           "authorization failure maps to forbidden",
				user:           "http_forbidden",
				idToken:        noGroupsToken,
				accessToken:    noGroupsToken,
				expectedStatus: http.StatusForbidden,
			},
			{
				name:           "missing ID token maps to internal server error",
				user:           "http_missing_id",
				accessToken:    noGroupsToken,
				expectedStatus: http.StatusInternalServerError,
			},
			{
				name:           "missing access token maps to forbidden",
				user:           "http_missing_access",
				idToken:        makeToken([]string{roleOwners}),
				expectedStatus: http.StatusForbidden,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				createUser(t, tc.user)
				mock.setTokens(tc.idToken, tc.accessToken, tc.user+"@example.com")
				resp := doLoginCallback(t)
				defer resp.Body.Close()
				require.Equal(t, tc.expectedStatus, resp.StatusCode)
				if tc.expectedStatus != http.StatusTemporaryRedirect {
					body, err := io.ReadAll(resp.Body)
					require.NoError(t, err)
					require.Contains(t, string(body),
						"OIDC: unable to complete authentication")
				}
				require.ElementsMatch(t, tc.expectedRoles, getRoles(t, tc.user))
			})
		}
	})
}
