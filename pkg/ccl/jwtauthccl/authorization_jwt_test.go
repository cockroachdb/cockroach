// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
)

// makeTokenWithClaims builds a token from an arbitrary claim-map so that tests
// for authorisation and authentication share one helper (mirrors helper in
// authentication_jwt_test.go).
func makeTokenWithClaims(t *testing.T, claims map[string]any) jwt.Token {
	tok := jwt.New()
	for k, v := range claims {
		require.NoError(t, tok.Set(k, v))
	}
	return tok
}

// TestAuthorizationJWT_ExtractGroups exercises the public ExtractGroups method
// of the authenticator.
func TestAuthorizationJWT_ExtractGroups(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	// Enable JWT authorisation and configure the claim name ahead of time.
	JWTAuthZEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthGroupClaim.Override(ctx, &s.ClusterSettings().SV, "groups")

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	type testCase struct {
		name            string
		rawGroups       any      // value to embed in the JWT "groups" claim; nil => claim absent
		userinfoGroups  []string // what the mocked user-info endpoint should return; nil means endpoint disabled
		expect          []string // expected ExtractGroups() result
		expectErrSubstr string   // substring that must appear in error (empty => expect no error)
	}

	cases := []testCase{
		// groups already present in the token ­– no user-info call.
		{"json_array", []any{"OwnerS", " userS "}, nil, []string{"owners", "users"}, ""},
		{"comma_separated", "A,  b ,a", nil, []string{"a", "b"}, ""},
		{"space_separated", "Foo Bar baz", nil, []string{"bar", "baz", "foo"}, ""},

		// claim missing or malformed – fall back to user-info.
		{"userinfo_success", nil, []string{"team1"}, []string{"team1"}, ""},

		// malformed claim variants that end up with “no userinfo endpoint”.
		{"wrong_type", 17, nil, nil, "No userinfo endpoint"},
		{"claim_missing", nil, nil, nil, "No userinfo endpoint"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			// Build the token.
			claims := map[string]any{}
			if tc.rawGroups != nil {
				claims["groups"] = tc.rawGroups
			} else {
				// Ensure Subject exists when groups claim is absent so
				// fallback path has something sensible.
				claims[jwt.SubjectKey] = "alice"
			}
			tok := makeTokenWithClaims(t, claims)
			tokenBytes, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256, []byte("secret")))
			require.NoError(t, err)

			// Stub fetchGroupsFromUserinfo as required.
			restore := testutils.TestingHook(&fetchGroupsFromUserinfo, func(
				context.Context, *cluster.Settings, string, []byte, *jwtAuthenticator,
			) ([]string, error) {
				if tc.userinfoGroups == nil {
					// Simulate provider that does not expose a user-info endpoint.
					return nil, nil
				}
				return tc.userinfoGroups, nil
			})
			defer restore()

			groups, err := verifier.ExtractGroups(ctx, s.ClusterSettings(), tokenBytes)

			if tc.expectErrSubstr != "" {
				require.ErrorContains(t, err, tc.expectErrSubstr)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tc.expect, groups)
		})
	}

	// Malformed JWTs that should all fail early.
	t.Run("malformed_tokens", func(t *testing.T) {
		bad := [][]byte{
			{},                        // empty
			[]byte("not.a.jwt.token"), // two-dot form
			[]byte("eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6InRlc3QtcnNhIn0.eyJpc3MiOiJ0ZXN0LWlzc3VlciIsImF1ZCI6InRlc3RfY2x1c3RlciIsInVzZXJuYW1lIjoiYWxpY2UiLCJzdWIiOiIxMjM0NTY3ODkwIiwiaWF0IjoxNzQ5MjkzMDM4LCJleHAiOjQxMDI0NDQ4MDAsInJvbGVzIjpbIk93bmVyIiwidVNFUiJdfQ"), // missing signature
		}
		for _, tok := range bad {
			_, err := verifier.ExtractGroups(ctx, s.ClusterSettings(), tok)
			require.Error(t, err)
		}
	})
}

func TestAuthorizationJWT_UserinfoErrorPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthZEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthGroupClaim.Override(ctx, &s.ClusterSettings().SV, "groups")

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	// Helper: build a JWT with no "groups" claim so ExtractGroups must call user-info.
	makeToken := func(issuer string) []byte {
		tok := jwt.New()
		require.NoError(t, tok.Set(jwt.IssuerKey, issuer))
		require.NoError(t, tok.Set(jwt.SubjectKey, "alice"))
		tokenBytes, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256, []byte("secret")))
		require.NoError(t, err)
		return tokenBytes
	}

	type tc struct {
		name           string
		discoveryDoc   string // served from /.well-known/openid-configuration
		userinfoStatus int
		userinfoBody   string
		expectContains string // substring expected in the *client-visible* error
		expectHide     string // substring that must NOT appear
	}

	tests := []tc{
		{
			name:           "userinfo_endpoint_absent",
			discoveryDoc:   `{"issuer":"{{url}}"}`, // no userinfo_endpoint key
			expectContains: "JWT authorization: No userinfo endpoint",
			expectHide:     "issuer=",
		},
		{
			name:           "userinfo_key_missing",
			discoveryDoc:   `{"issuer":"{{url}}","userinfo_endpoint":"{{url}}/userinfo"}`,
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{}`, // empty JSON object => no groups key
			// no error expected, so expectContains = ""
			expectHide: "userinfo",
		},
		{
			name: "userinfo_network_error",
			// discovery returns endpoint that points to a non-listening address
			discoveryDoc:   `{"issuer":"{{url}}","userinfo_endpoint":"http://1.1.1.1:4444/userinfo"}`,
			expectContains: "JWT authorization: userinfo lookup failed",
			expectHide:     "1.1.1.1",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			// Spin up a discovery server specific to this sub-test.
			var ts *httptest.Server

			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/.well-known/openid-configuration":
					doc := strings.ReplaceAll(tt.discoveryDoc, "{{url}}", ts.URL)
					_, _ = io.WriteString(w, doc)
				case "/userinfo":
					w.WriteHeader(tt.userinfoStatus)
					_, _ = io.WriteString(w, tt.userinfoBody)
				default:
					http.NotFound(w, r)
				}
			})

			ts = httptest.NewServer(handler)
			defer ts.Close()

			token := makeToken(ts.URL)
			groups, err := verifier.ExtractGroups(ctx, s.ClusterSettings(), token)

			if tt.expectContains == "" {
				require.NoError(t, err)
				require.Len(t, groups, 0)
				return
			}
			require.ErrorContains(t, err, tt.expectContains)
			require.NotContains(t, err.Error(), tt.expectHide)
		})
	}
}
