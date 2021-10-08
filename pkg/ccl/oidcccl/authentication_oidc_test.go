// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	defer utilccl.TestingEnableEnterprise()()
	security.SetAssetLoader(securitytest.EmbeddedAssets)
	randutil.SeedForTests()
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

func TestOIDCBadRequestIfDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	newRPCContext := func(cfg *base.Config) *rpc.Context {
		return rpc.NewContext(rpc.ContextOptions{
			TenantID: roachpb.SystemTenantID,
			Config:   cfg,
			Clock:    hlc.NewClock(hlc.UnixNano, 1),
			Stopper:  s.Stopper(),
			Settings: s.ClusterSettings(),
		})
	}

	plainHTTPCfg := testutils.NewTestBaseContext(security.TestUserName())
	testCertsContext := newRPCContext(plainHTTPCfg)

	client, err := testCertsContext.GetHTTPClient()
	require.NoError(t, err)

	resp, err := client.Get(s.AdminURL() + "/oidc/v1/login")
	if err != nil {
		t.Fatalf("could not issue GET request to admin server: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 status code but got: %d", resp.StatusCode)
	}
}

func TestOIDCEnabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	newRPCContext := func(cfg *base.Config) *rpc.Context {
		return rpc.NewContext(rpc.ContextOptions{
			TenantID: roachpb.SystemTenantID,
			Config:   cfg,
			Clock:    hlc.NewClock(hlc.UnixNano, 1),
			Stopper:  s.Stopper(),
			Settings: s.ClusterSettings(),
		})
	}

	// Set up a test OIDC server that serves the JSON discovery document
	var issuer string
	oidcHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			w.Header().Set("content-type", "application/json")
			fakeDiscoveryDocument := `
{
 "issuer": "` + issuer + `",
 "authorization_endpoint": "https://accounts.cockroachlabs.com/o/oauth2/v2/auth",
 "device_authorization_endpoint": "https://oauth2.cockroachlabsapis.com/device/code",
 "token_endpoint": "https://oauth2.cockroachlabsapis.com/token",
 "userinfo_endpoint": "https://openidconnect.cockroachlabsapis.com/v1/userinfo",
 "revocation_endpoint": "https://oauth2.cockroachlabsapis.com/revoke",
 "jwks_uri": "https://www.cockroachlabsapis.com/oauth2/v3/certs",
 "response_types_supported": [
  "code",
  "token",
  "id_token",
  "code token",
  "code id_token",
  "token id_token",
  "code token id_token",
  "none"
 ],
 "subject_types_supported": [
  "public"
 ],
 "id_token_signing_alg_values_supported": [
  "RS256"
 ],
 "scopes_supported": [
  "openid",
  "email",
  "profile"
 ],
 "token_endpoint_auth_methods_supported": [
  "client_secret_post",
  "client_secret_basic"
 ],
 "claims_supported": [
  "aud",
  "email",
  "email_verified",
  "exp",
  "family_name",
  "given_name",
  "iat",
  "iss",
  "locale",
  "name",
  "picture",
  "sub"
 ],
 "code_challenge_methods_supported": [
  "plain",
  "S256"
 ],
 "grant_types_supported": [
  "authorization_code",
  "refresh_token",
  "urn:ietf:params:oauth:grant-type:device_code",
  "urn:ietf:params:oauth:grant-type:jwt-bearer"
 ]
}`
			_, _ = fmt.Fprint(w, fakeDiscoveryDocument)
			return
		}
		http.Error(w, "wrong path", http.StatusBadRequest)
	}
	testOIDCServer := httptest.NewServer(http.HandlerFunc(oidcHandler))
	defer testOIDCServer.Close()
	issuer = testOIDCServer.URL

	// Set minimum settings to successfully enable the OIDC client
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.provider_url = "`+testOIDCServer.URL+`"`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.client_id = "fake_client_id"`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.client_secret = "fake_client_secret"`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.redirect_url = "https://cockroachlabs.com/oidc/v1/callback"`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.enabled = "true"`)

	plainHTTPCfg := testutils.NewTestBaseContext(security.TestUserName())
	testCertsContext := newRPCContext(plainHTTPCfg)

	client, err := testCertsContext.GetHTTPClient()
	require.NoError(t, err)

	// Don't follow redirects so we can inspect the 302
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	t.Run("login redirect", func(t *testing.T) {
		resp, err := client.Get(s.AdminURL() + "/oidc/v1/login")
		if err != nil {
			t.Fatalf("could not issue GET request to admin server: %s", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != 302 {
			t.Fatalf("expected 302 status code but got: %d", resp.StatusCode)
		}
		if resp.Cookies()[0].Name != secretCookieName {
			t.Fatal("Missing cookie")
		}
		authURL, err := url.Parse(resp.Header.Get("Location"))
		require.NoError(t, err)
		if authURL.Query().Get("client_id") != "fake_client_id" {
			t.Fatal("expected fake client_id", authURL)
		}
		const expectedRedirectURL = "https://cockroachlabs.com/oidc/v1/callback"
		if authURL.Query().Get("redirect_uri") != expectedRedirectURL {
			t.Fatal("expected fake redirect_url", authURL)
		}

		state, err := decodeOIDCState(authURL.Query().Get("state"))
		require.NoError(t, err)
		// If we use hmac.Sum with the Message, it gets prepended to the hash.
		if strings.Contains(string(state.TokenMAC), string(state.Token)) {
			t.Fatal("HMAC generated incorrectly.")
		}

		key, err := base64.URLEncoding.DecodeString(resp.Cookies()[0].Value)
		require.NoError(t, err)
		mac := hmac.New(sha256.New, key)
		mac.Write(state.Token)
		if !hmac.Equal(mac.Sum(nil), state.TokenMAC) {
			t.Fatal("HMAC hash doesn't match TokenMAC")
		}
	})
}

func TestKeyAndSignedTokenIsValid(t *testing.T) {
	kastValid, err := newKeyAndSignedToken(32, 32)
	require.NoError(t, err)
	kastModifiedCookie, err := newKeyAndSignedToken(32, 32)
	require.NoError(t, err)
	kastModifiedCookie.secretKeyCookie.Value = kastModifiedCookie.secretKeyCookie.Value + "Z"
	kastModifiedTokenPayload, err := newKeyAndSignedToken(32, 32)
	require.NoError(t, err)
	kastModifiedTokenPayload.signedTokenEncoded = kastModifiedCookie.signedTokenEncoded + "Z"
	kastEmptyCookie, err := newKeyAndSignedToken(32, 32)
	require.NoError(t, err)
	kastEmptyCookie.secretKeyCookie.Value = ""
	kastEmptyToken, err := newKeyAndSignedToken(32, 32)
	require.NoError(t, err)
	kastEmptyToken.signedTokenEncoded = ""

	for _, tc := range []struct {
		desc        string
		kast        *keyAndSignedToken
		expectValid bool
		expectErr   bool
	}{
		{"randomly generated cookie and token", kastValid, true, false},
		{"bad cookie value and token", kastModifiedCookie, false, true},
		{"valid cookie value and bad token", kastModifiedTokenPayload, false, true},
		{"empty cookie, valid token", kastEmptyCookie, false, false},
		{"valid cookie, empty token", kastEmptyToken, false, false},
	} {
		t.Run(tc.desc, func(t *testing.T) {
			valid, err := tc.kast.validate()
			if tc.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tc.expectValid, valid)
		})
	}
}

func TestOIDCStateEncodeDecode(t *testing.T) {
	testString := "abc-123-@~~" // This string produces discrepancy when base46 URL is used vs Std
	encoded, err := encodeOIDCState(serverpb.OIDCState{
		Token:    []byte(testString),
		TokenMAC: []byte(testString),
	})
	require.NoError(t, err)
	state, err := decodeOIDCState(encoded)
	require.NoError(t, err)

	if string(state.Token) != testString || string(state.TokenMAC) != testString {
		t.Fatal("state didn't match when decoded")
	}
}

func Test_getRegionSpecificRedirectURL(t *testing.T) {
	type args struct {
		locality roachpb.Locality
		conf     redirectURLConf
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		// Single redirect configurations
		{"single redirect: empty locality", args{
			locality: roachpb.Locality{},
			conf:     redirectURLConf{sru: &singleRedirectURL{RedirectURL: "correct.example.com"}},
		}, "correct.example.com", false},
		{"single redirect: locality with no region", args{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "who", Value: "knows"}}},
			conf:     redirectURLConf{sru: &singleRedirectURL{RedirectURL: "correct.example.com"}},
		}, "correct.example.com", false},
		{"single redirect: locality with region", args{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east-1"}}},
			conf:     redirectURLConf{sru: &singleRedirectURL{RedirectURL: "correct.example.com"}},
		}, "correct.example.com", false},
		// Multi-region configurations
		{"multi-region config: empty locality", args{
			locality: roachpb.Locality{},
			conf:     redirectURLConf{mrru: &multiRegionRedirectURLs{RedirectURLs: nil}},
		}, "", true},
		{"multi-region config: locality with no region", args{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "who", Value: "knows"}}},
			conf:     redirectURLConf{mrru: &multiRegionRedirectURLs{RedirectURLs: nil}},
		}, "", true},
		{"multi-region config: locality with region but no corresponding URL in config", args{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east-1"}}},
			conf:     redirectURLConf{mrru: &multiRegionRedirectURLs{RedirectURLs: nil}},
		}, "", true},
		{"multi-region config: locality with region and corresponding url", args{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-east-1"}}},
			conf: redirectURLConf{mrru: &multiRegionRedirectURLs{
				RedirectURLs: map[string]string{
					"us-east-1": "correct.example.com",
					"us-west-2": "incorrect.example.com",
				},
			}},
		}, "correct.example.com", false},
		{"multi-region config: locality with region and corresponding url 2", args{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-west-2"}}},
			conf: redirectURLConf{mrru: &multiRegionRedirectURLs{
				RedirectURLs: map[string]string{
					"us-east-1": "incorrect.example.com",
					"us-west-2": "correct.example.com",
				},
			}},
		}, "correct.example.com", false},
		{"multi-region config: locality with unexpected region", args{
			locality: roachpb.Locality{Tiers: []roachpb.Tier{{Key: "region", Value: "us-central-2"}}},
			conf: redirectURLConf{mrru: &multiRegionRedirectURLs{
				RedirectURLs: map[string]string{
					"us-east-1": "incorrect.example.com",
					"us-west-2": "correct.example.com",
				},
			}},
		}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getRegionSpecificRedirectURL(tt.args.locality, tt.args.conf)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, got, tt.want)
		})
	}
}
