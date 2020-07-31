// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestOIDCBadRequestIfDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

	newRPCContext := func(cfg *base.Config) *rpc.Context {
		return rpc.NewContext(rpc.ContextOptions{
			TenantID: roachpb.SystemTenantID,
			Config:   cfg,
			Clock:    hlc.NewClock(hlc.UnixNano, 1),
			Stopper:  s.Stopper(),
			Settings: s.ClusterSettings(),
		})
	}

	plainHTTPCfg := testutils.NewTestBaseContext(TestUser)
	testCertsContext := newRPCContext(plainHTTPCfg)

	client, err := testCertsContext.GetHTTPClient()

	resp, err := client.Get(ts.AdminURL() + "/oidc/login")
	if err != nil {
		t.Fatalf("could not issue GET request to admin server: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 status code but got: %d", resp.StatusCode)
	}
}

func TestOIDCEnableAndLogin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())
	ts := s.(*TestServer)

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
	testOIDCServer := httptest.NewServer(http.HandlerFunc(oidcHandler));
	defer testOIDCServer.Close()
	issuer = testOIDCServer.URL

	// Set minimum settings to successfully enable the OIDC client
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.provider_url = "` + testOIDCServer.URL + `"`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.client_id = "fake_client_id"`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.client_secret = "fake_client_secret"`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.redirect_url = "https://cockroachlabs.com/fake/oidc"`)
	sqlDB.Exec(t, `SET CLUSTER SETTING server.oidc_authentication.enabled = "true"`)

	plainHTTPCfg := testutils.NewTestBaseContext(TestUser)
	testCertsContext := newRPCContext(plainHTTPCfg)

	client, err := testCertsContext.GetHTTPClient()

	// Add `oidc_state` cookie to our client
	cookies, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("unable to create cookiejar: %v", err)
	}
	adminURL, err := url.Parse(ts.AdminURL())
	if err != nil {
		t.Fatalf("unable to parse admin url: %v", err)
	}
	cookies.SetCookies(adminURL, []*http.Cookie{&http.Cookie{Name: "oidc_state", Value: "blahblah"}})
	client.Jar = cookies

	// Don't follow redirects so we can inspect the 302
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	resp, err := client.Get(ts.AdminURL() + "/oidc/login")
	if err != nil {
		t.Fatalf("could not issue GET request to admin server: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 302 {
		t.Fatalf("expected 302 status code but got: %d", resp.StatusCode)
	}
	redirectURL, err := url.Parse(resp.Header.Get("Location"))
	if err != nil {
		//blah
	}
	if redirectURL.Query().Get("client_id") != "fake_client_id" {
		t.Fatalf("expected fake client_id", redirectURL)
	}
	if redirectURL.Query().Get("redirec_T") != "fake_client_id" {
		t.Fatalf("expected fake client_id", redirectURL)
	}
}
