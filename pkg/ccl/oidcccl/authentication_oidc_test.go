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
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"os"
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
	if err != nil {
		t.Fatal(err)
	}

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
	if err != nil {
		t.Fatal(err)
	}

	// Add `oidc_state` cookie to our client
	cookies, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("unable to create cookiejar: %v", err)
	}
	adminURL, err := url.Parse(s.AdminURL())
	if err != nil {
		t.Fatalf("unable to parse admin url: %v", err)
	}
	cookies.SetCookies(adminURL, []*http.Cookie{{Name: "oidc_state", Value: "blahblah"}})
	client.Jar = cookies

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
		authURL, err := url.Parse(resp.Header.Get("Location"))
		if err != nil {
			t.Fatal(err)
		}
		if authURL.Query().Get("client_id") != "fake_client_id" {
			t.Fatal("expected fake client_id", authURL)
		}
		const expectedRedirectURL = "https://cockroachlabs.com/oidc/v1/callback"
		if authURL.Query().Get("redirect_uri") != expectedRedirectURL {
			t.Fatal("expected fake redirect_url", authURL)
		}
	})
}

func TestOIDCStateEncodeDecode(t *testing.T) {
	testString := "abc-123-@~~" // This string produces discrepancy when base46 URL is used vs Std
	encoded, err := encodeOIDCState(serverpb.OIDCState{Secret: []byte(testString), NodeID: 3})
	if err != nil {
		t.Fatal(err)
	}

	state, err := decodeOIDCState(encoded)
	if err != nil {
		t.Fatal(err)
	}

	if string(state.Secret) != testString || state.NodeID != 3 {
		t.Fatal("state didn't match when decoded")
	}
}
