// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/securityccl/jwthelper"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lestrrat-go/jwx/v2/jwa"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
)

// makeToken creates a *jwt.Token with claim=val.
func makeToken(t *testing.T, claim string, val any) jwt.Token {
	tok := jwt.New()
	require.NoError(t, tok.Set(claim, val))
	return tok
}

func TestAuthorizationJWT(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("ExtractGroupsFromJWT", func(t *testing.T) {
		st := cluster.MakeTestingClusterSettings()
		JWTAuthGroupClaim.Override(context.Background(), &st.SV, "groups")

		cases := []struct {
			name    string
			raw     any      // value stored in the groups claim
			exp     []string // expected (deduped, sorted, lower-cased)
			wantErr bool
		}{
			{"json-array", []any{"AdminS", " interns "}, []string{"admins", "interns"}, false},
			{"comma-sep", "A,  b ,a", []string{"a", "b"}, false},
			{"space-sep", "Foo Bar baz", []string{"bar", "baz", "foo"}, false},
			{"non-string-slice", []any{1, "X"}, []string{"1", "x"}, false},
			{"wrong-type", 17, nil, true},
			{"missing-claim", nil, nil, true},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				var tok jwt.Token
				if tc.raw != nil {
					tok = makeToken(t, "groups", tc.raw)
				} else {
					tok = jwt.New() // claim absent
				}
				claimName := JWTAuthGroupClaim.Get(&st.SV)

				got, err := jwthelper.ExtractGroups(st, tok, claimName)
				if tc.wantErr {
					require.Error(t, err)
					return
				}
				require.NoError(t, err)
				require.ElementsMatch(t, tc.exp, got)
				if len(got) > 1 {
					require.True(t, sort.StringsAreSorted(got))
				}
			})
		}
	})

	t.Run("FetchGroupsFromUserinfo", func(t *testing.T) {
		ctx := context.Background()
		st := cluster.MakeTestingClusterSettings()
		JWTAuthUserinfoGroupKey.Override(ctx, &st.SV, "groups")

		// Mock IdP.
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch {
			case strings.HasSuffix(r.URL.Path, "/.well-known/openid-configuration"):
				_, _ = fmt.Fprintf(w, `{"userinfo_endpoint":"http://%s/v1/userinfo"}`, r.Host)

			case strings.HasSuffix(r.URL.Path, "/v1/userinfo"):
				auth := r.Header.Get("Authorization")
				require.True(t, strings.HasPrefix(auth, "Bearer "))

				if auth == "Bearer tok-nogroups" {
					_, _ = w.Write([]byte(`{"sub":"someone"}`))
				} else {
					_, _ = w.Write([]byte(`{"groups":["one","Two"]}`))
				}

			default:
				w.WriteHeader(http.StatusNotFound)
			}
		}))
		defer srv.Close()

		httpClient := httputil.NewClient()
		issuer := strings.TrimSuffix(srv.URL, "/")

		// Happy path.
		g, err := fetchGroupsFromUserinfo(ctx, st, issuer, []byte("tok"), httpClient)
		require.NoError(t, err)
		require.Equal(t, []string{"one", "two"}, g)

		// No groups = nil slice + nil error.
		g, err = fetchGroupsFromUserinfo(ctx, st, issuer, []byte("tok-nogroups"), httpClient)
		require.NoError(t, err)
		require.Nil(t, g)
	})

	t.Run("ExtractGroupsFallsBackToUserinfo", func(t *testing.T) {
		ctx := context.Background()
		s := serverutils.StartServerOnly(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		JWTAuthEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		JWTAuthZEnabled.Override(ctx, &s.ClusterSettings().SV, true)
		JWTAuthUserinfoGroupKey.Override(ctx, &s.ClusterSettings().SV, "groups")

		verifier := ConfigureJWTAuth(ctx, s.AmbientCtx(), s.ClusterSettings(), s.StorageClusterID())

		// Token without groups.
		tok := jwt.New()
		require.NoError(t, tok.Set(jwt.SubjectKey, "alice"))
		tokenBytes, err := jwt.Sign(tok, jwt.WithKey(jwa.HS256, []byte("secret")))
		require.NoError(t, err)

		// Stub user-info fetcher.
		httpCalls := 0
		orig := fetchGroupsFromUserinfo
		fetchGroupsFromUserinfo = func(
			_ context.Context, _ *cluster.Settings, _ string, _ []byte, _ *httputil.Client,
		) ([]string, error) {
			httpCalls++
			return []string{"team1"}, nil
		}
		defer func() { fetchGroupsFromUserinfo = orig }()

		groups, err := verifier.ExtractGroups(s.ClusterSettings(), tokenBytes)
		require.NoError(t, err)
		require.Equal(t, []string{"team1"}, groups)
		require.Equal(t, 1, httpCalls)
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
			{"missing_signature", []byte("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0")},
		}

		for _, tc := range cases {
			tc := tc
			t.Run(tc.name, func(t *testing.T) {
				_, err := verifier.ExtractGroups(s.ClusterSettings(), tc.token)
				require.Error(t, err)
			})
		}
	})
}
