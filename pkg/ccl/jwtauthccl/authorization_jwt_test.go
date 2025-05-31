// Copyright 2024 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/lestrrat-go/jwx/v2/jwt"
	"github.com/stretchr/testify/require"
)

// makeToken is a helper that returns a *jwt.Token with the supplied claim value.
func makeToken(t *testing.T, claim string, val any) jwt.Token {
	tok := jwt.New()
	require.NoError(t, tok.Set(claim, val))
	return tok
}

func TestExtractGroupsFromJWT(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	st := cluster.MakeTestingClusterSettings()
	// Pretend the cluster uses claim "groups"
	JWTAuthGroupClaim.Override(context.Background(), &st.SV, "groups")

	tests := []struct {
		name    string
		raw     any      // value populated in the token
		exp     []string // expected slice: alpha-sorted, deduped, lowercased
		wantErr bool
	}{
		{"json-array", []any{"AdminS", " interns "}, []string{"admins", "interns"}, false},
		{"comma-sep", "A,  b ,a", []string{"a", "b"}, false},
		{"space-sep", "Foo Bar baz", []string{"bar", "baz", "foo"}, false},
		{"non-string-slice", []any{1, "X"}, []string{"1", "x"}, false},
		{"wrong-type", 17, nil, true},
		{"missing-claim", nil, nil, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tok jwt.Token
			if tc.raw != nil {
				tok = makeToken(t, "groups", tc.raw)
			} else {
				tok = jwt.New() // empty token, claim missing
			}
			got, err := extractGroupsFromJWT(st, tok)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.ElementsMatch(t, tc.exp, got)
			// slice should come out sorted for deterministic GRANT order
			if len(got) > 1 {
				require.True(t, sort.StringsAreSorted(got))
			}
		})
	}
}

func TestFetchGroupsFromUserinfo(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	JWTAuthUserinfoGroupKey.Override(ctx, &st.SV, "groups")

	// Mock OIDC userinfo service
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/.well-known/openid-configuration"):
			_, _ = fmt.Fprintf(w, `{"userinfo_endpoint":"http://%s/v1/userinfo"}`, r.Host)

		case strings.HasSuffix(r.URL.Path, "/v1/userinfo"):
			auth := r.Header.Get("Authorization")
			require.True(t, strings.HasPrefix(auth, "Bearer "))

			// Different payload depending on the bearer token.
			if auth == "Bearer tok-nogroups" {
				_, _ = w.Write([]byte(`{"sub":"someone"}`)) // no groups
			} else {
				_, _ = w.Write([]byte(`{"groups":["one","Two"]}`)) // has groups
			}

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	httpClient := httputil.NewClient()
	issuer := strings.TrimSuffix(srv.URL, "/") // mimic issuer base

	// Happy path
	g, err := fetchGroupsFromUserinfo(ctx, st, issuer, []byte("tok"), httpClient)
	require.NoError(t, err)
	require.Equal(t, []string{"one", "two"}, g)

	// No groups -> nil slice, nil error (caller decides whether to error out)
	_, err = fetchGroupsFromUserinfo(ctx, st, issuer, []byte("tok-nogroups"), httpClient)
	require.NoError(t, err)
}
