// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
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

	JWTAuthZEnabled.Override(ctx, &s.ClusterSettings().SV, true)
	JWTAuthGroupClaim.Override(ctx, &s.ClusterSettings().SV, "groups")

	verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

	type testCase struct {
		name            string
		claims          map[string]any // claims to embed in the JWT
		userinfoBody    string         // JSON response from the mock userinfo endpoint
		expect          []string       // expected ExtractGroups() result
		expectErrSubstr string
	}

	cases := []testCase{
		// Groups already present in the token – no userinfo call needed.
		{"json_array", map[string]any{"groups": []any{"OwnerS", " userS "}}, "", []string{"owners", "users"}, ""},
		{"comma_separated", map[string]any{"groups": "A,  b ,a"}, "", []string{"a", "b"}, ""},
		{"space_separated", map[string]any{"groups": "Foo Bar baz"}, "", []string{"bar", "baz", "foo"}, ""},

		// Claim missing or malformed – fall back to userinfo.
		{
			name:         "userinfo_success",
			claims:       map[string]any{jwt.SubjectKey: "alice"}, // No 'groups' claim
			userinfoBody: `{"groups": ["team1", "team2"]}`,
			expect:       []string{"team1", "team2"},
		},
		{
			name:         "userinfo_no_groups_key",
			claims:       map[string]any{jwt.SubjectKey: "alice"},
			userinfoBody: `{"sub": "alice"}`, // Valid JSON, but no 'groups' key
			expect:       []string{},         // Expect an empty list, not an error
		},
		{
			name:            "claim_wrong_type_fallback",
			claims:          map[string]any{"groups": 17}, // Invalid type for groups claim
			userinfoBody:    `{"groups": ["fallback_group"]}`,
			expect:          []string{"fallback_group"},
			expectErrSubstr: "", // The error from parsing the token claim should be handled, triggering the userinfo fallback.
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ts *httptest.Server
			issuerURL := "static-issuer-for-local-validation"

			// If the test case expects a userinfo fallback, set up a mock OIDC server.
			if tc.userinfoBody != "" {
				handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/.well-known/openid-configuration":
						w.Header().Set("Content-Type", "application/json")
						// The mock discovery document points to our mock userinfo endpoint.
						_, _ = fmt.Fprintf(w, `{"issuer": "%s", "userinfo_endpoint": "%s/userinfo"}`, ts.URL, ts.URL)
					case "/userinfo":
						w.Header().Set("Content-Type", "application/json")
						_, _ = io.WriteString(w, tc.userinfoBody)
					default:
						http.NotFound(w, r)
					}
				})
				ts = httptest.NewServer(handler)
				defer ts.Close()
				issuerURL = ts.URL // The JWT's issuer must match the mock server's URL.
			}

			// Build the token with the correct issuer.
			tc.claims[jwt.IssuerKey] = issuerURL
			tok := makeTokenWithClaims(t, tc.claims)
			// Sign with a dummy key. The signature isn't verified in the ExtractGroups path.
			tokenBytes, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256, []byte("secret")))
			require.NoError(t, err)

			// The verifier needs to know about the issuer to initialize the OIDC provider.
			JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, issuerURL)

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
		discoveryDoc   string
		userinfoStatus int
		userinfoBody   string
		expectContains string
		expectHide     string
	}

	tests := []tc{
		{
			name:           "userinfo_endpoint_absent",
			discoveryDoc:   `{"issuer":"{{url}}"}`,
			expectContains: "JWT authorization: userinfo lookup failed",
			expectHide:     "issuer=",
		},
		{
			name:           "userinfo_key_missing",
			discoveryDoc:   `{"issuer":"{{url}}","userinfo_endpoint":"{{url}}/userinfo"}`,
			userinfoStatus: http.StatusOK,
			userinfoBody:   `{}`,
			expectHide:     "userinfo",
		},
		{
			name:           "userinfo_network_error",
			discoveryDoc:   `{"issuer":"{{url}}","userinfo_endpoint":"http://1.1.1.1:4444/userinfo"}`,
			expectContains: "JWT authorization: userinfo lookup failed",
			expectHide:     "1.1.1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			JWTAuthIssuersConfig.Override(ctx, &s.ClusterSettings().SV, ts.URL)

			token := makeToken(ts.URL)
			groups, err := verifier.ExtractGroups(ctx, s.ClusterSettings(), token)

			if tt.expectContains == "" {
				require.NoError(t, err)
				require.Len(t, groups, 0)
				return
			}
			require.ErrorContains(t, err, tt.expectContains)
			if tt.expectHide != "" {
				require.NotContains(t, err.Error(), tt.expectHide)
			}
		})
	}
}
