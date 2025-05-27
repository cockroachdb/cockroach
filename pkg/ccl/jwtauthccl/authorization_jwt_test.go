// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

	// Build a JWT that has no groups claim so ExtractGroups must hit the
	// userinfo code path.
	tok := jwt.New()
	require.NoError(t, tok.Set(jwt.SubjectKey, "alice"))
	tokenBytes, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256, []byte("secret")))
	require.NoError(t, err)

	type hookFn = func(
		context.Context, *cluster.Settings, string, []byte, *jwtAuthenticator,
	) ([]string, error)

	for _, tc := range []struct {
		name           string
		hook           hookFn // overrides fetchGroupsFromUserinfo
		expectContains string // substring expected in *client* error
		expectHide     string // substring that must NOT appear
	}{
		{
			name: "userinfo_endpoint_absent",
			hook: func(context.Context, *cluster.Settings, string, []byte, *jwtAuthenticator) ([]string, error) {
				return nil, nil // groups == nil -> no endpoint
			},
			expectContains: "JWT authorization: No userinfo endpoint",
			expectHide:     "issuer=", // issuer URL comes from internalErr, must be hidden
		},
		{
			name: "userinfo_key_missing",
			hook: func(context.Context, *cluster.Settings, string, []byte, *jwtAuthenticator) ([]string, error) {
				return []string{}, nil // empty slice  -> key missing / empty
			},
			expectContains: "",         // no error
			expectHide:     "userinfo", // no URI leakage
		},
		{
			name: "userinfo_network_error",
			hook: func(context.Context, *cluster.Settings, string, []byte, *jwtAuthenticator) ([]string, error) {
				return nil, errors.New("dial tcp 1.1.1.1:443: connect: permission denied")
			},
			expectContains: "JWT authorization: userinfo lookup failed",
			expectHide:     "1.1.1.1", // internal address must not leak
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			restore := testutils.TestingHook(&fetchGroupsFromUserinfo, tc.hook)
			defer restore()

			groups, err := verifier.ExtractGroups(ctx, s.ClusterSettings(), tokenBytes)
			if tc.expectContains == "" {
				require.NoError(t, err)
				require.Len(t, groups, 0)
				return
			}
			require.ErrorContains(t, err, tc.expectContains)
			require.NotContains(t, err.Error(), tc.expectHide)
		})
	}
}
