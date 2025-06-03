// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

import (
	"context"
	"encoding/json"
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
	oidc "github.com/coreos/go-oidc"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

// makeJWT creates an HS-signed JWT containing the supplied claims.
// The secret, algorithm, and signing step do not matter for these tests;
// we only need a syntactically correct token.
func makeJWT(t *testing.T, claims map[string]any) string {
	token := jwt.New()
	for k, v := range claims {
		_ = token.Set(k, v)
	}
	raw, err := jwt.Sign(token, jwt.WithKey(jwa.HS256, []byte("secret")))
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
func (m mockManager) ExchangeVerifyGetTokens(
	_ context.Context, _, _ string,
) (map[string]json.RawMessage, string, *oauth2.Token, error) {
	claims := map[string]json.RawMessage{
		"email": json.RawMessage(`"` + m.email + `"`),
	}
	return claims, m.rawIDToken, &oauth2.Token{AccessToken: m.accessToken}, nil
}

var _ IOIDCManager = (*mockManager)(nil)

// TestOIDCAuthorization_TokenPaths exercises the public OIDC login and callback
// handlers.  Each table entry spins up a fresh single-node server, injects a
// different set of token claims, and verifies the resulting SQL role
// memberships for the test user.
func TestOIDCAuthorization_TokenPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		name              string
		idTokenClaims     map[string]any
		accessTokenClaims map[string]any
		expectedRoles     []string
	}{
		{
			name: "groups in ID token",
			idTokenClaims: map[string]any{
				"groups": []string{"owners", "users"},
			},
			accessTokenClaims: nil,
			expectedRoles:     []string{"owners", "users"},
		},
		{
			name: "groups in access token only",
			idTokenClaims: map[string]any{
				"note": "no groups here",
			},
			accessTokenClaims: map[string]any{
				"groups": []string{"owners"},
			},
			expectedRoles: []string{"owners"},
		},
		{
			name: "no valid groups",
			idTokenClaims: map[string]any{
				"groups": []string{"%%%bad%%%"},
			},
			accessTokenClaims: nil,
			expectedRoles:     []string{}, // expect 403
		},
	}

	for _, tc := range testCases {
		tc := tc // capture loop var
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			// Start a brand-new in-process node for this sub-test.
			s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
			defer s.Stopper().Stop(ctx)
			app := s.ApplicationLayer()
			sqlDB := sqlutils.MakeSQLRunner(db)

			// Create the user and any roles that might be granted.
			const user = "test"
			sqlDB.Exec(t, `CREATE USER test`)
			sqlDB.Exec(t, `CREATE ROLE owners`)
			sqlDB.Exec(t, `CREATE ROLE users`)

			// Enable OIDC plus authorization.
			st := s.ClusterSettings()
			OIDCProviderURL.Override(ctx, &st.SV, "https://provider.example.com")
			OIDCClientID.Override(ctx, &st.SV, "client")
			OIDCClientSecret.Override(ctx, &st.SV, "secret")
			OIDCRedirectURL.Override(ctx, &st.SV, "https://cockroachlabs.com/oidc/v1/callback")
			OIDCClaimJSONKey.Override(ctx, &st.SV, "email")
			OIDCPrincipalRegex.Override(ctx, &st.SV, "^([^@]+)@[^@]+$")
			OIDCEnabled.Override(ctx, &st.SV, true)
			OIDCAuthZEnabled.Override(ctx, &st.SV, true)
			OIDCAuthGroupClaim.Override(ctx, &st.SV, "groups")

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
					rawIDToken:  makeJWT(t, tc.idTokenClaims),
					accessToken: makeJWT(t, tc.accessTokenClaims),
					email:       "test@example.com",
				}, nil
			}

			// Build an HTTP client that trusts the node certificates.
			rpcCtx := app.NewClientRPCContext(ctx, username.RootUserName())
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

			// choose status based on whether any roles are expected.
			wantStatus := http.StatusTemporaryRedirect
			if len(tc.expectedRoles) == 0 {
				wantStatus = http.StatusForbidden
			}
			require.Equal(t, wantStatus, cbResp.StatusCode)

			// Check which SQL roles were granted to the user.
			rows := sqlDB.QueryStr(t,
				`SELECT role FROM system.role_members WHERE member = $1 ORDER BY role`, user)
			actualRoles := make([]string, len(rows))
			for i, r := range rows {
				actualRoles[i] = r[0]
			}
			require.Equal(t, tc.expectedRoles, actualRoles)
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
		name             string
		discoveryDoc     string
		userinfoStatus   int
		userinfoBody     string
		wantRoles        []string // nil => expect 403
		wantErrSubstring string
	}

	cases := []tc{
		{
			name: "userinfo success",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "userinfo_endpoint": "{{url}}/userinfo"
			}`,
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{"groups":["owners","users"]}`,
			wantRoles:      []string{"owners", "users"},
		},
		{
			name: "userinfo endpoint absent",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token"
			}`,
			// no userinfo key – should end in forbidden
			wantRoles:        nil,
			wantErrSubstring: "unable to complete authentication", // genericHttpError
		},
		{
			name: "userinfo network error",
			discoveryDoc: `{
			  "issuer": "{{url}}",
			  "token_endpoint": "{{url}}/token",
			  "userinfo_endpoint": "http://127.0.0.1:0/userinfo"
			}`,
			wantRoles:        nil,
			wantErrSubstring: "unable to complete authentication", // genericHttpError
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()

			var ts *httptest.Server
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					doc := strings.ReplaceAll(tc.discoveryDoc, "{{url}}", ts.URL)
					_, _ = io.WriteString(w, doc)
				case "/userinfo":
					w.WriteHeader(tc.userinfoStatus)
					_, _ = io.WriteString(w, tc.userinfoBody)
				case "/token":
					idTok := makeJWT(t, map[string]any{
						"iss":   ts.URL, // issuer must match provider URL
						"email": "test@example.com",
						"aud":   "client",
						"exp":   time.Now().Add(time.Hour).Unix(),
					})
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
			sqlDB.Exec(t, `CREATE USER test`)
			sqlDB.Exec(t, `CREATE ROLE owners`)
			sqlDB.Exec(t, `CREATE ROLE users`)

			orig := NewOIDCManager
			t.Cleanup(func() { NewOIDCManager = orig })
			NewOIDCManager = func(_ context.Context, conf oidcAuthenticationConf, redirect string, scopes []string) (IOIDCManager, error) {
				cfg := &oauth2.Config{
					ClientID:     conf.clientID,
					ClientSecret: conf.clientSecret,
					RedirectURL:  redirect,
					Endpoint:     oauth2.Endpoint{AuthURL: ts.URL + "/auth"},
					Scopes:       scopes,
				}
				claims := map[string]any{
					"email": "test@example.com",
					"iss":   ts.URL,
					"aud":   "client",
					"exp":   time.Now().Add(time.Hour).Unix(),
				}
				return &mockManager{
					cfg:         cfg,
					rawIDToken:  makeJWT(t, claims),
					accessToken: makeJWT(t, claims),
					email:       "test@example.com",
				}, nil
			}

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

			rpc := app.NewClientRPCContext(ctx, username.RootUserName())
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
			if tc.wantRoles == nil {
				wantStatus = http.StatusForbidden
			}
			require.Equal(t, wantStatus, cbResp.StatusCode)

			rows := sqlDB.QueryStr(t, `SELECT role FROM system.role_members WHERE member = 'test' ORDER BY role`)
			var got []string
			for _, r := range rows {
				got = append(got, r[0])
			}
			require.Equal(t, tc.wantRoles, got)

			if tc.wantErrSubstring != "" {
				body, _ := io.ReadAll(cbResp.Body)
				require.Contains(t, string(body), tc.wantErrSubstring)
			}
		})
	}
}
