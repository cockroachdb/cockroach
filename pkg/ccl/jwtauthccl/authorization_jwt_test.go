// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/jwthelper"
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

	t.Run("GroupsPresentInToken", func(t *testing.T) {
		// When groups are embedded in the JWT, ExtractGroups should return them
		// without consulting the userinfo endpoint.
		cases := []struct {
			name string
			raw  any
			exp  []string
		}{
			{"json_array", []any{"OwnerS", " userS "}, []string{"owners", "users"}},
			{"comma_separated", "A,  b ,a", []string{"a", "b"}},
			{"space_separated", "Foo Bar baz", []string{"bar", "baz", "foo"}},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				tok := makeTokenWithClaims(t, map[string]any{"groups": tc.raw})
				tokenBytes, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256, []byte("secret")))
				require.NoError(t, err)

				// Stub fetchGroupsFromUserinfo to prove it is *not* called.
				var calls int
				restore := testutils.TestingHook(&fetchGroupsFromUserinfo, func(
					context.Context, *cluster.Settings, string, []byte, *jwtAuthenticator,
				) ([]string, error) {
					calls++
					return nil, nil
				})
				defer restore()

				groups, err := verifier.ExtractGroups(ctx, s.ClusterSettings(), tokenBytes)
				require.NoError(t, err)
				require.ElementsMatch(t, tc.exp, groups)
				require.Zero(t, calls, "userinfo should not be consulted when groups claim exists")
			})
		}
	})

	t.Run("FallbackToUserinfo", func(t *testing.T) {
		// When the groups claim is absent or malformed, ExtractGroups should
		// delegate to the user‑info endpoint.
		tok := makeTokenWithClaims(t, map[string]any{jwt.SubjectKey: "alice"})
		tokenBytes, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256, []byte("secret")))
		require.NoError(t, err)

		var calls int
		restore := testutils.TestingHook(&fetchGroupsFromUserinfo, func(
			context.Context, *cluster.Settings, string, []byte, *jwtAuthenticator,
		) ([]string, error) {
			calls++
			return []string{"team1"}, nil
		})
		defer restore()

		groups, err := verifier.ExtractGroups(ctx, s.ClusterSettings(), tokenBytes)
		require.NoError(t, err)
		require.Equal(t, []string{"team1"}, groups)
		require.Equal(t, 1, calls, "userinfo should be consulted exactly once")
	})

	t.Run("JWTHelperExtractGroups", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		JWTAuthGroupClaim.Override(ctx, &st.SV, "groups")

		cases := []struct {
			name    string
			raw     any
			exp     []string
			wantErr bool
		}{
			{"valid_json_array", []any{"OwnerS", " userS "}, []string{"owners", "users"}, false},
			{"valid_comma_sep", "A,  b ,a", []string{"a", "b"}, false},
			{"valid_space_sep", "Foo Bar baz", []string{"bar", "baz", "foo"}, false},
			{"invalid_wrong_type", 17, nil, true},
			{"invalid_missing_claim", nil, nil, true},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				var tok jwt.Token
				if tc.raw != nil {
					tok = makeTokenWithClaims(t, map[string]any{"groups": tc.raw})
				} else {
					tok = jwt.New() // claim absent
				}

				got, err := jwthelper.ExtractGroups(tok, JWTAuthGroupClaim.Get(&st.SV))
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.ElementsMatch(t, tc.exp, got)
			})
		}
	})

	t.Run("MalformedTokens", func(t *testing.T) {
		ctx := context.Background()
		s := serverutils.StartServerOnly(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		JWTAuthZEnabled.Override(ctx, &s.ClusterSettings().SV, true)

		verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

		cases := []struct {
			name  string
			token []byte
		}{
			{"empty", []byte("")},
			{"invalid_format", []byte("not.a.jwt.token")},
			{"missing_signature", []byte("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIiwibmFtZSI6IkpvaG4gRG9lIiwiaWF0IjoxNTE2MjM5MDIyfQ")},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				_, err := verifier.ExtractGroups(ctx, s.ClusterSettings(), tc.token)
				require.Error(t, err)
			})
		}
	})
}
