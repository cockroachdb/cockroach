// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/certnames"
	"github.com/cockroachdb/cockroach/pkg/security/provisioning"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/coreos/go-oidc"
	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
)

func TestOIDCBadRequestIfDisabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	testCertsContext := s.NewClientRPCContext(ctx, username.TestUserName())

	client, err := testCertsContext.GetHTTPClient()
	require.NoError(t, err)

	resp, err := client.Get(s.AdminURL().WithPath("/oidc/v1/login").String())
	if err != nil {
		t.Fatalf("could not issue GET request to admin server: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 400 {
		t.Fatalf("expected 400 status code but got: %d", resp.StatusCode)
	}
}

type mockOidcManager struct {
	oauth2Config         *oauth2.Config
	claimEmail           string
	forceIssuerMismatch  bool
	forceExchangeFailure bool
}

func (m mockOidcManager) Verify(ctx context.Context, s string) (*oidc.IDToken, error) {
	return nil, nil
}

func (m mockOidcManager) Exchange(
	ctx context.Context, s string, option ...oauth2.AuthCodeOption,
) (*oauth2.Token, error) {
	return nil, nil
}

func (m mockOidcManager) AuthCodeURL(s string, option ...oauth2.AuthCodeOption) string {
	return m.oauth2Config.AuthCodeURL(s, option...)
}

func (m mockOidcManager) ExchangeVerifyGetTokenInfo(
	ctx context.Context, code, idTokenKey string, _ bool,
) (map[string]json.RawMessage, map[string]json.RawMessage, string, string, error) {
	if m.forceIssuerMismatch {
		return nil, nil, "", "", fmt.Errorf("oidc: token issuer mismatch")
	}
	if m.forceExchangeFailure {
		return nil, nil, "", "", fmt.Errorf("token verification failed")
	}

	emailClaimJSON, err := json.Marshal(m.claimEmail)
	if err != nil {
		return nil, nil, "", "", err
	}
	claims := map[string]json.RawMessage{
		"email": emailClaimJSON,
	}

	// Return nil for access token claims, and the raw token strings.
	return claims, nil, "dummy-id-token", "dummy-access-token", nil
}

func (m mockOidcManager) UserInfo(
	ctx context.Context, ts oauth2.TokenSource,
) (*oidc.UserInfo, error) {
	// This mock is not used in this test file, so it can return nil.
	return nil, nil
}

var _ IOIDCManager = &mockOidcManager{}

func TestOIDCEnabled(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	usernameUnderTest := "test"
	basePath := "/some/random/path"

	realNewManager := NewOIDCManager
	NewOIDCManager = func(ctx context.Context, conf oidcAuthenticationConf, redirectURL string, scopes []string) (IOIDCManager, error) {
		c := &oauth2.Config{
			ClientID:     conf.clientID,
			ClientSecret: conf.clientSecret,
			RedirectURL:  redirectURL,

			Endpoint: oauth2.Endpoint{
				AuthURL: "https://provider.example.com/endpoint",
			},
			Scopes: scopes,
		}
		return &mockOidcManager{oauth2Config: c, claimEmail: fmt.Sprintf("%s@example.com", usernameUnderTest)}, nil
	}
	defer func() {
		NewOIDCManager = realNewManager
	}()

	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, fmt.Sprintf(`CREATE USER %s with password 'unused'`, usernameUnderTest))

	// Set minimum settings to successfully enable the OIDC client
	OIDCProviderURL.Override(ctx, &s.ClusterSettings().SV, "providerURL")
	OIDCClientID.Override(ctx, &s.ClusterSettings().SV, "fake_client_id")
	OIDCClientSecret.Override(ctx, &s.ClusterSettings().SV, "fake_client_secret")
	OIDCRedirectURL.Override(ctx, &s.ClusterSettings().SV, "https://cockroachlabs.com/oidc/v1/callback")
	OIDCClaimJSONKey.Override(ctx, &s.ClusterSettings().SV, "email")
	OIDCPrincipalRegex.Override(ctx, &s.ClusterSettings().SV, "^([^@]+)@[^@]+$")
	server.ServerHTTPBasePath.Override(ctx, &s.ClusterSettings().SV, basePath)
	OIDCEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	testCertsContext := s.NewClientRPCContext(ctx, username.TestUserName())
	client, err := testCertsContext.GetHTTPClient()
	require.NoError(t, err)

	// Set a reasonable timeout for the client to prevent flakiness under stress.
	client.Timeout = 30 * time.Second

	// Don't follow redirects so we can inspect the 302
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	resp, err := client.Get(s.AdminURL().WithPath("/oidc/v1/login").String())
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
	cookie := resp.Cookies()[0]

	authURL, err := url.Parse(resp.Header.Get("Location"))
	require.NoError(t, err)
	if authURL.Query().Get("client_id") != "fake_client_id" {
		t.Fatal("expected fake client_id", authURL)
	}
	const expectedRedirectURL = "https://cockroachlabs.com/oidc/v1/callback"
	if authURL.Query().Get("redirect_uri") != expectedRedirectURL {
		t.Fatal("expected fake redirect_url", authURL)
	}

	stateParam := authURL.Query().Get("state")
	state, err := decodeOIDCState(stateParam)
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

	// Simulate the OIDC provider invoking our callback.
	req, err := http.NewRequest("GET", s.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
	require.NoError(t, err)

	// The cookie set on the login request must be retained by the
	// browser when the callback is invoked in order to verify that the
	// `state` param matches the one that was sent during login.
	req.AddCookie(cookie)
	q := req.URL.Query()
	q.Add("state", stateParam)
	req.URL.RawQuery = q.Encode()

	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("could not issue GET request to callback endpoint: %s", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 307 {
		t.Fatalf("expected 307 status code but got: %d", resp.StatusCode)
	}
	if resp.Header.Get("Location") != basePath {
		t.Fatalf("expected to be redirected to %s", basePath)
	}
	foundCookie := false
	for _, c := range resp.Cookies() {
		if c.Name == authserver.SessionCookieName {
			foundCookie = true
		}
	}
	if !foundCookie {
		t.Fatalf("no session cookie found in callback response")
	}
}

func TestKeyAndSignedTokenIsValid(t *testing.T) {
	kastValid, err := newKeyAndSignedToken(32, 32, serverpb.OIDCState_MODE_LOG_IN)
	require.NoError(t, err)
	kastModifiedCookie, err := newKeyAndSignedToken(32, 32, serverpb.OIDCState_MODE_LOG_IN)
	require.NoError(t, err)
	kastModifiedCookie.secretKeyCookie.Value = kastModifiedCookie.secretKeyCookie.Value + "Z"
	kastModifiedTokenPayload, err := newKeyAndSignedToken(32, 32, serverpb.OIDCState_MODE_LOG_IN)
	require.NoError(t, err)
	kastModifiedTokenPayload.signedTokenEncoded = kastModifiedCookie.signedTokenEncoded + "Z"
	kastEmptyCookie, err := newKeyAndSignedToken(32, 32, serverpb.OIDCState_MODE_LOG_IN)
	require.NoError(t, err)
	kastEmptyCookie.secretKeyCookie.Value = ""
	kastEmptyToken, err := newKeyAndSignedToken(32, 32, serverpb.OIDCState_MODE_LOG_IN)
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
			valid, _, err := tc.kast.validate()
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

func TestOIDCClaimMatch(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		testName       string
		claimKey       string
		principalRegex string
		claims         map[string]json.RawMessage
		wantError      bool
	}{
		{
			testName:       "string valued claim",
			claimKey:       "email",
			principalRegex: "^([^@]+)@[^@]+$",
			claims: map[string]json.RawMessage{
				"email": json.RawMessage(`"myfakeemail@example.com"`),
			},
		},
		{
			testName:       "string valued claim with no match",
			claimKey:       "email",
			principalRegex: "^([^@]+)@[^@]+$",
			claims: map[string]json.RawMessage{
				"email": json.RawMessage(`"bademail"`),
			},
			wantError: true,
		},
		{
			testName:       "list valued claim",
			claimKey:       "groups",
			principalRegex: "^([^@]+)@[^@]+$",
			claims: map[string]json.RawMessage{
				"groups": json.RawMessage(
					`["badgroupname", "myfakeemail@example.com", "anotherbadgroupname"]`,
				),
			},
		},
		{
			testName:       "list valued claim with no matches",
			claimKey:       "groups",
			principalRegex: "^([^@]+)@[^@]+$",
			claims: map[string]json.RawMessage{
				"groups": json.RawMessage(`["badgroupname", "anotherbadgroupname"]`),
			},
			wantError: true,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			sqlUsername, err := extractUsernameFromClaims(
				ctx, tc.claims, tc.claimKey, regexp.MustCompile(tc.principalRegex),
			)
			if !tc.wantError {
				require.NoError(t, err)
				require.Equal(t, "myfakeemail", sqlUsername)
			} else {
				require.ErrorContains(t, err, "expected one group in regexp")
			}
		})
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

func TestOIDCManagerInitialisationUnderNetworkAvailability(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	basePath := "/some/random/path"
	testUserName := "testcrl"
	networkAvailable := false

	// Intercept the call to NewOIDCManager and return the mocked NewOIDCManager function
	defer testutils.TestingHook(
		&NewOIDCManager,
		func(ctx context.Context, conf oidcAuthenticationConf, redirectURL string, scopes []string) (IOIDCManager, error) {
			if !networkAvailable {
				return nil, fmt.Errorf("network unavailable, check your network connection")
			}
			c := &oauth2.Config{
				ClientID:     conf.clientID,
				ClientSecret: conf.clientSecret,
				RedirectURL:  redirectURL,
				Endpoint: oauth2.Endpoint{
					AuthURL: conf.providerURL,
				},
				Scopes: scopes,
			}
			return &mockOidcManager{oauth2Config: c, claimEmail: fmt.Sprintf("%s@example.com", testUserName)}, nil
		})()

	// Set/Override minimum settings to successfully enable the OIDC client
	OIDCProviderURL.Override(ctx, &s.ClusterSettings().SV, "providerURL")
	OIDCClientID.Override(ctx, &s.ClusterSettings().SV, "fake_client_id")
	OIDCClientSecret.Override(ctx, &s.ClusterSettings().SV, "fake_client_secret")
	OIDCRedirectURL.Override(ctx, &s.ClusterSettings().SV, "https://cockroachlabs.com/oidc/v1/callback")
	OIDCClaimJSONKey.Override(ctx, &s.ClusterSettings().SV, "email")
	OIDCPrincipalRegex.Override(ctx, &s.ClusterSettings().SV, "^([^@]+)@[^@]+$")
	server.ServerHTTPBasePath.Override(ctx, &s.ClusterSettings().SV, basePath)
	OIDCEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	for _, tc := range []struct {
		testName  string
		network   bool
		wantError bool
	}{
		{
			testName:  "network unavailable test",
			network:   false,
			wantError: true,
		},
		{
			testName:  "network available test",
			network:   true,
			wantError: false,
		},
	} {
		t.Run(tc.testName, func(t *testing.T) {
			networkAvailable = tc.network
			testOIDCManagerInitialisation := s.NewClientRPCContext(ctx, username.TestUserName())
			client, err := testOIDCManagerInitialisation.GetHTTPClient()
			require.NoError(t, err)

			// Don't follow redirects as we are only testing setting of oidc  manager, expecting 302
			// status code on success
			client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			}

			resp, err := client.Get(s.AdminURL().WithPath("/oidc/v1/login").String())
			if err != nil {
				t.Fatalf("could not issue GET request to admin server: %s", err)
			}
			defer resp.Body.Close()
			bodyBytes, _ := io.ReadAll(resp.Body)

			if !tc.wantError {
				require.Equal(t, 302, resp.StatusCode)
			} else {
				require.Contains(t, string(bodyBytes), "OIDC: auth server could not be initialized")
				require.Equal(t, 500, resp.StatusCode)
			}
		})
	}
}

func TestOIDCProviderInitialization(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Initiate a test server to handle `./well-known/openid-configuration`.
	testServer := httptest.NewUnstartedServer(nil)
	testServerURL := "http://" + testServer.Listener.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /.well-known/openid-configuration",
		func(w http.ResponseWriter, r *http.Request) {
			// Serve the response locally from testdata.
			dataBytes, err := os.ReadFile("testdata/issuer_well_known_openid_configuration")
			require.NoError(t, err)

			// We need to match the 'issuer' key in the response to the test server URL.
			// Hence, we read the test response and overwrite the 'issuer'.
			type providerJSON struct {
				Issuer      string   `json:"issuer"`
				AuthURL     string   `json:"authorization_endpoint"`
				TokenURL    string   `json:"token_endpoint"`
				JWKSURI     string   `json:"jwks_uri"`
				UserInfoURL string   `json:"userinfo_endpoint"`
				Algorithms  []string `json:"id_token_signing_alg_values_supported"`
			}

			var p providerJSON
			err = json.Unmarshal(dataBytes, &p)
			require.NoError(t, err)

			p.Issuer = testServerURL

			updatedBytes, err := json.Marshal(p)
			require.NoError(t, err)

			_, err = w.Write(updatedBytes)
			require.NoError(t, err)
		},
	)

	testServer.Config = &http.Server{
		Handler: mux,
	}
	testServer.Start()
	defer testServer.Close()

	// Initialize the OIDC manager.
	clientTimeout := 10 * time.Second
	oidcConf := oidcAuthenticationConf{
		providerURL: testServerURL,
		httpClient:  httputil.NewClientWithTimeout(clientTimeout),
	}
	iOIDCMgr, err := NewOIDCManager(context.Background(), oidcConf, "redirectURL", []string{})
	require.NoError(t, err)
	require.NotNil(t, iOIDCMgr)

	oidcMgr, _ := iOIDCMgr.(*oidcManager)
	require.NotNil(t, oidcMgr.verifier)
	require.NotNil(t, oidcMgr.oauth2Config)
	require.NotNil(t, oidcMgr.httpClient)
	require.Equal(t, clientTimeout, oidcMgr.httpClient.Timeout)
}

func TestOIDCProviderInitializationTimeout(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Initiate a test server to handle `./well-known/openid-configuration`.
	testServer := httptest.NewUnstartedServer(nil)
	testServerURL := "http://" + testServer.Listener.Addr().String()

	waitChan := make(chan struct{}, 1)

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /.well-known/openid-configuration",
		func(w http.ResponseWriter, r *http.Request) {
			// Hang the request handler so as to enforce HTTP client timeout.
			<-waitChan
		},
	)

	testServer.Config = &http.Server{
		Handler: mux,
	}
	testServer.Start()
	defer func() {
		waitChan <- struct{}{}
		close(waitChan)
		testServer.Close()
	}()

	// Initialize the OIDC manager.
	oidcConf := oidcAuthenticationConf{
		providerURL: testServerURL,
		// Set a small client timeout and assert that the initialization times out.
		httpClient: httputil.NewClientWithTimeout(time.Millisecond),
	}
	_, err := NewOIDCManager(context.Background(), oidcConf, "redirectURL", []string{})
	var urlError *url.Error
	require.ErrorAs(t, err, &urlError)
	require.True(t, urlError.Timeout())
}

func TestOIDCProviderCustomCACert(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Initiate a test server to handle `./well-known/openid-configuration`.
	testServer := httptest.NewUnstartedServer(nil)
	testServerURL := "https://" + testServer.Listener.Addr().String()

	mux := http.NewServeMux()
	mux.HandleFunc(
		"GET /.well-known/openid-configuration",
		func(w http.ResponseWriter, r *http.Request) {
			// Serve the response locally from testdata.
			dataBytes, err := os.ReadFile("testdata/issuer_well_known_openid_configuration")
			require.NoError(t, err)

			// We need to match the 'issuer' key in the response to the test server URL.
			// Hence, we read the test response and overwrite the 'issuer'.
			type providerJSON struct {
				Issuer      string   `json:"issuer"`
				AuthURL     string   `json:"authorization_endpoint"`
				TokenURL    string   `json:"token_endpoint"`
				JWKSURI     string   `json:"jwks_uri"`
				UserInfoURL string   `json:"userinfo_endpoint"`
				Algorithms  []string `json:"id_token_signing_alg_values_supported"`
			}

			var p providerJSON
			err = json.Unmarshal(dataBytes, &p)
			require.NoError(t, err)

			p.Issuer = testServerURL

			updatedBytes, err := json.Marshal(p)
			require.NoError(t, err)

			_, err = w.Write(updatedBytes)
			require.NoError(t, err)
		},
	)

	testServer.Config = &http.Server{
		Handler: mux,
	}

	certPEMBlock, err := securityassets.GetLoader().ReadFile(
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedNodeCert))
	require.NoError(t, err)
	keyPEMBlock, err := securityassets.GetLoader().ReadFile(
		filepath.Join(certnames.EmbeddedCertsDir, certnames.EmbeddedNodeKey))
	require.NoError(t, err)
	tlsCert, err := tls.X509KeyPair(certPEMBlock, keyPEMBlock)
	require.NoError(t, err)

	testServer.TLS = &tls.Config{Certificates: []tls.Certificate{tlsCert}}
	testServer.StartTLS()
	defer testServer.Close()

	for _, testCase := range []struct {
		testName string
		// caCertName is the name of the certificate looked up within `test_certs`.
		// Empty value is treated as no certificate.
		caCertName string
		assertFn   func(t require.TestingT, err error, msgAndArgs ...interface{})
	}{
		{
			testName:   "fail if no CA certificate is provided",
			caCertName: "",
			assertFn:   require.Error,
		},
		{
			testName:   "fail if an incorrect CA certificate is provided",
			caCertName: certnames.EmbeddedTestUserCert,
			assertFn:   require.Error,
		},
		{
			testName:   "success if the correct CA certificate is provided",
			caCertName: certnames.EmbeddedCACert,
			assertFn:   require.NoError,
		},
	} {
		t.Run(testCase.testName, func(t *testing.T) {
			clientOpts := []httputil.ClientOption{httputil.WithClientTimeout(10 * time.Second)}

			if testCase.caCertName != "" {
				publicKeyPEM, err := securityassets.GetLoader().ReadFile(
					filepath.Join(certnames.EmbeddedCertsDir, testCase.caCertName))
				require.NoError(t, err)

				clientOpts = append(clientOpts, httputil.WithCustomCAPEM(string(publicKeyPEM)))
			}

			// Initialize the OIDC manager.
			oidcConf := oidcAuthenticationConf{
				providerURL: testServerURL,
				httpClient:  httputil.NewClient(clientOpts...),
			}
			_, err := NewOIDCManager(context.Background(), oidcConf, "redirectURL", []string{})
			testCase.assertFn(t, err)
		})
	}
}

// TestOIDCProvisioning verifies the automatic user provisioning flow.
func TestOIDCProvisioning(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	// Start a full server to get access to the ExecutorConfig, which is
	// necessary for the provisioning logic to execute.
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Server: &server.TestingKnobs{},
		},
	})
	defer s.Stopper().Stop(ctx)
	ts := s.ApplicationLayer()
	sqlDB := sqlutils.MakeSQLRunner(db)

	usernameUnderTest := "oidcprovisioneduser"
	basePath := "/base/path/for/provisioning"

	// Mock the OIDC manager to simulate responses from an Identity Provider.
	realNewManager := NewOIDCManager
	NewOIDCManager = func(ctx context.Context, conf oidcAuthenticationConf, redirectURL string, scopes []string) (IOIDCManager, error) {
		c := &oauth2.Config{
			ClientID:     conf.clientID,
			ClientSecret: conf.clientSecret,
			RedirectURL:  redirectURL,
			Endpoint: oauth2.Endpoint{
				AuthURL: "https://provider.example.com/endpoint",
			},
			Scopes: scopes,
		}
		// The mockOidcManager will extract `usernameUnderTest` from this email claim.
		return &mockOidcManager{oauth2Config: c, claimEmail: fmt.Sprintf("%s@example.com", usernameUnderTest)}, nil
	}
	defer func() {
		NewOIDCManager = realNewManager
	}()

	// Configure the necessary OIDC cluster settings for the test.
	OIDCProviderURL.Override(ctx, &ts.ClusterSettings().SV, "https://provider.example.com")
	OIDCClientID.Override(ctx, &ts.ClusterSettings().SV, "fake_client_id_for_provisioning")
	OIDCClientSecret.Override(ctx, &ts.ClusterSettings().SV, "fake_client_secret_for_provisioning")
	OIDCRedirectURL.Override(ctx, &ts.ClusterSettings().SV, "https://cockroachlabs.com/oidc/v1/callback")
	OIDCClaimJSONKey.Override(ctx, &ts.ClusterSettings().SV, "email")
	OIDCPrincipalRegex.Override(ctx, &ts.ClusterSettings().SV, "^([^@]+)@[^@]+$")
	server.ServerHTTPBasePath.Override(ctx, &ts.ClusterSettings().SV, basePath)
	OIDCEnabled.Override(ctx, &ts.ClusterSettings().SV, true)

	// Setup an HTTP client to make requests to the server.
	testCertsContext := ts.NewClientRPCContext(ctx, username.TestUserName())
	client, err := testCertsContext.GetHTTPClient()
	require.NoError(t, err)
	client.Timeout = 30 * time.Second

	// Sub-test for successful provisioning of a new user.
	t.Run("provisioning enabled, new user", func(t *testing.T) {
		// Ensure the user does not exist before the test and is dropped after.
		sqlDB.Exec(t, fmt.Sprintf("DROP USER IF EXISTS %s", usernameUnderTest))
		defer sqlDB.Exec(t, fmt.Sprintf("DROP USER IF EXISTS %s", usernameUnderTest))

		// Enable OIDC provisioning.
		provisioning.OIDCProvisioningEnabled.Override(ctx, &ts.ClusterSettings().SV, true)
		defer provisioning.OIDCProvisioningEnabled.Override(ctx, &ts.ClusterSettings().SV, false)

		// Hit the /login endpoint to get the state token and cookie.
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
		resp, err := client.Get(ts.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusFound, resp.StatusCode)

		cookie := resp.Cookies()[0]
		require.Equal(t, secretCookieName, cookie.Name)

		authURL, err := url.Parse(resp.Header.Get("Location"))
		require.NoError(t, err)
		stateParam := authURL.Query().Get("state")

		// Simulate the IdP redirect to the /callback endpoint.
		client.CheckRedirect = nil // Allow redirects to follow through to the final page.
		req, err := http.NewRequest("GET", ts.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
		require.NoError(t, err)
		req.AddCookie(cookie)
		q := req.URL.Query()
		q.Add("state", stateParam)
		q.Add("code", "some-auth-code-for-provisioning")
		req.URL.RawQuery = q.Encode()

		resp, err = client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Assert a successful login, indicated by a 200 OK status after the redirect.
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, basePath, resp.Request.URL.Path)

		// Verify that the user was created in the database.
		rows := sqlDB.Query(t, "SELECT username FROM [SHOW USERS] WHERE username = $1", usernameUnderTest)
		require.True(t, rows.Next(), "user should have been created by provisioning")
		require.NoError(t, rows.Close())
	})

	// Sub-test for a successful login for an existing user while provisioning is enabled.
	t.Run("provisioning enabled, existing user", func(t *testing.T) {
		// Create the user beforehand.
		sqlDB.Exec(t, fmt.Sprintf("DROP USER IF EXISTS %s", usernameUnderTest))
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER %s", usernameUnderTest))
		defer sqlDB.Exec(t, fmt.Sprintf("DROP USER IF EXISTS %s", usernameUnderTest))

		// Enable OIDC provisioning.
		provisioning.OIDCProvisioningEnabled.Override(ctx, &ts.ClusterSettings().SV, true)
		defer provisioning.OIDCProvisioningEnabled.Override(ctx, &ts.ClusterSettings().SV, false)

		// Simulate the login flow.
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := client.Get(ts.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusFound, resp.StatusCode)
		cookie := resp.Cookies()[0]
		authURL, err := url.Parse(resp.Header.Get("Location"))
		require.NoError(t, err)
		stateParam := authURL.Query().Get("state")

		client.CheckRedirect = nil
		req, err := http.NewRequest("GET", ts.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
		require.NoError(t, err)
		req.AddCookie(cookie)
		q := req.URL.Query()
		q.Add("state", stateParam)
		q.Add("code", "some-auth-code-for-existing-user")
		req.URL.RawQuery = q.Encode()

		resp, err = client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Assert a successful login.
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify the user still exists.
		rows := sqlDB.Query(t, "SELECT username FROM [SHOW USERS] WHERE username = $1", usernameUnderTest)
		require.True(t, rows.Next(), "user should still exist")
		require.NoError(t, rows.Close())
	})

	// Sub-test for a successful login for an existing user while provisioning is disabled.
	t.Run("provisioning disabled, existing user", func(t *testing.T) {
		// Create the user beforehand.
		sqlDB.Exec(t, fmt.Sprintf("DROP USER IF EXISTS %s", usernameUnderTest))
		sqlDB.Exec(t, fmt.Sprintf("CREATE USER %s", usernameUnderTest))
		defer sqlDB.Exec(t, fmt.Sprintf("DROP USER IF EXISTS %s", usernameUnderTest))

		// Disable OIDC provisioning.
		provisioning.OIDCProvisioningEnabled.Override(ctx, &ts.ClusterSettings().SV, false)

		// Simulate the login flow.
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := client.Get(ts.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusFound, resp.StatusCode)
		cookie := resp.Cookies()[0]
		authURL, err := url.Parse(resp.Header.Get("Location"))
		require.NoError(t, err)
		stateParam := authURL.Query().Get("state")

		client.CheckRedirect = nil
		req, err := http.NewRequest("GET", ts.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
		require.NoError(t, err)
		req.AddCookie(cookie)
		q := req.URL.Query()
		q.Add("state", stateParam)
		q.Add("code", "some-auth-code-for-existing-user-provisioning-disabled")
		req.URL.RawQuery = q.Encode()

		resp, err = client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Assert a successful login.
		require.Equal(t, http.StatusOK, resp.StatusCode)

		// Verify the user still exists.
		rows := sqlDB.Query(t, "SELECT username FROM [SHOW USERS] WHERE username = $1", usernameUnderTest)
		require.True(t, rows.Next(), "user should still exist")
		require.NoError(t, rows.Close())
	})

	// Sub-test to ensure no user is created when provisioning is disabled.
	t.Run("provisioning disabled, new user", func(t *testing.T) {
		// Ensure the user does not exist.
		sqlDB.Exec(t, fmt.Sprintf("DROP USER IF EXISTS %s", usernameUnderTest))
		defer sqlDB.Exec(t, fmt.Sprintf("DROP USER IF EXISTS %s", usernameUnderTest))

		// Disable OIDC provisioning.
		provisioning.OIDCProvisioningEnabled.Override(ctx, &ts.ClusterSettings().SV, false)

		// Simulate the login flow.
		client.CheckRedirect = func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }
		resp, err := client.Get(ts.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusFound, resp.StatusCode)
		cookie := resp.Cookies()[0]
		authURL, err := url.Parse(resp.Header.Get("Location"))
		require.NoError(t, err)
		stateParam := authURL.Query().Get("state")

		client.CheckRedirect = nil
		req, err := http.NewRequest("GET", ts.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
		require.NoError(t, err)
		req.AddCookie(cookie)
		q := req.URL.Query()
		q.Add("state", stateParam)
		q.Add("code", "some-auth-code-for-disabled-provisioning")
		req.URL.RawQuery = q.Encode()

		resp, err = client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()

		// Assert a failed login because the user does not exist and will not be created.
		require.Equal(t, http.StatusForbidden, resp.StatusCode)
		body, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), genericCallbackHTTPError)

		// Verify the user was NOT created.
		rows := sqlDB.Query(t, "SELECT username FROM [SHOW USERS] WHERE username = $1", usernameUnderTest)
		require.False(t, rows.Next(), "user should not have been created")
		require.NoError(t, rows.Close())
	})
}

// TestOIDCExchangeVerifyFailure ensures that a verification error inside
// ExchangeVerifyGetTokenInfo surfaces as an HTTP 500 at /oidc/v1/callback.
func TestOIDCExchangeVerifyFailure(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	usernameUnderTest := "verifyfailuser"
	basePath := "/some/oidc/path"

	// Intercept NewOIDCManager and return the failing mock.
	realNewManager := NewOIDCManager
	NewOIDCManager = func(
		ctx context.Context,
		conf oidcAuthenticationConf,
		redirectURL string,
		scopes []string,
	) (IOIDCManager, error) {
		c := &oauth2.Config{
			ClientID:     conf.clientID,
			ClientSecret: conf.clientSecret,
			RedirectURL:  redirectURL,
			Endpoint:     oauth2.Endpoint{AuthURL: "https://provider.example.com/endpoint"},
			Scopes:       scopes,
		}
		return &mockOidcManager{
			oauth2Config:         c,
			claimEmail:           fmt.Sprintf("%s@example.com", usernameUnderTest),
			forceExchangeFailure: true,
		}, nil
	}
	defer func() { NewOIDCManager = realNewManager }()

	// Minimal cluster‑setting wiring to enable OIDC.
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, fmt.Sprintf(`CREATE USER %s WITH PASSWORD 'placeholder'`, usernameUnderTest))

	OIDCProviderURL.Override(ctx, &s.ClusterSettings().SV, "providerURL")
	OIDCClientID.Override(ctx, &s.ClusterSettings().SV, "fake_client_id")
	OIDCClientSecret.Override(ctx, &s.ClusterSettings().SV, "fake_client_secret")
	OIDCRedirectURL.Override(ctx, &s.ClusterSettings().SV, "https://cockroachlabs.com/oidc/v1/callback")
	OIDCClaimJSONKey.Override(ctx, &s.ClusterSettings().SV, "email")
	OIDCPrincipalRegex.Override(ctx, &s.ClusterSettings().SV, "^([^@]+)@[^@]+$")
	server.ServerHTTPBasePath.Override(ctx, &s.ClusterSettings().SV, basePath)
	OIDCEnabled.Override(ctx, &s.ClusterSettings().SV, true)

	testCtx := s.NewClientRPCContext(ctx, username.TestUserName())
	client, err := testCtx.GetHTTPClient()
	require.NoError(t, err)
	client.Timeout = 30 * time.Second
	client.CheckRedirect = func(req *http.Request, via []*http.Request) error { return http.ErrUseLastResponse }

	// hit /login to get state & cookie.
	loginResp, err := client.Get(s.AdminURL().WithPath("/oidc/v1/login").String())
	require.NoError(t, err)
	defer loginResp.Body.Close()
	require.Equal(t, http.StatusFound, loginResp.StatusCode)

	cookie := loginResp.Cookies()[0]
	authURL, err := url.Parse(loginResp.Header.Get("Location"))
	require.NoError(t, err)
	stateParam := authURL.Query().Get("state")

	// simulate IdP redirect to /callback with same state.
	req, err := http.NewRequest("GET", s.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
	require.NoError(t, err)
	req.AddCookie(cookie)
	q := req.URL.Query()
	q.Add("state", stateParam)
	q.Add("code", "irrelevant-auth-code")
	req.URL.RawQuery = q.Encode()

	callbackResp, err := client.Do(req)
	require.NoError(t, err)
	defer callbackResp.Body.Close()

	// Verification failure in the manager should surface as 500 + generic error.
	require.Equal(t, http.StatusInternalServerError, callbackResp.StatusCode)
	body, err := io.ReadAll(callbackResp.Body)
	require.NoError(t, err)
	require.Contains(t, string(body), genericCallbackHTTPError)
}

func TestOIDCIssuerValidation(t *testing.T) {
	// Sub-test 1:  This test ensures that an ID token with an untrusted,
	// malicious, or misconfigured issuer (`iss` claim) is rejected.
	t.Run("fails when token's issuer differs from the configured provider", func(t *testing.T) {

		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)

		ctx := context.Background()
		srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
		defer srv.Stopper().Stop(ctx)
		s := srv.ApplicationLayer()

		// Replace NewOIDCManager with manager that errors on Verify.
		restore := testutils.TestingHook(
			&NewOIDCManager,
			func(ctx context.Context, c oidcAuthenticationConf, redirectURL string, scopes []string) (IOIDCManager, error) {
				conf := &oauth2.Config{
					ClientID:    c.clientID,
					RedirectURL: redirectURL,
					Endpoint:    oauth2.Endpoint{AuthURL: c.providerURL},
					Scopes:      scopes,
				}
				return &mockOidcManager{oauth2Config: conf, forceIssuerMismatch: true}, nil
			},
		)
		defer restore()

		// Minimal cluster settings to enable OIDC.
		OIDCProviderURL.Override(ctx, &s.ClusterSettings().SV, "https://provider.example.com")
		OIDCClientID.Override(ctx, &s.ClusterSettings().SV, "cid")
		OIDCClientSecret.Override(ctx, &s.ClusterSettings().SV, "sec")
		OIDCRedirectURL.Override(ctx, &s.ClusterSettings().SV, "https://cockroachlabs.com/oidc/v1/callback")
		OIDCEnabled.Override(ctx, &s.ClusterSettings().SV, true)

		httpClient, err := s.NewClientRPCContext(ctx, username.TestUserName()).GetHTTPClient()
		require.NoError(t, err)
		httpClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }

		// Start login (302 expected).
		loginResp, err := httpClient.Get(s.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		defer loginResp.Body.Close()
		require.Equal(t, http.StatusFound, loginResp.StatusCode)

		// Extract state from the login redirect to use in the callback.
		authURL, err := url.Parse(loginResp.Header.Get("Location"))
		require.NoError(t, err)
		stateParam := authURL.Query().Get("state")

		// Simulate callback; expect 500 because the mock manager will fail verification.
		cbReq, _ := http.NewRequest("GET", s.AdminURL().WithPath("/oidc/v1/callback").String(), nil)
		// Use the extracted stateParam
		q := cbReq.URL.Query()
		q.Add("state", stateParam)
		q.Add("code", "bad-code")
		cbReq.URL.RawQuery = q.Encode()

		for _, c := range loginResp.Cookies() {
			cbReq.AddCookie(c)
		}
		cbResp, err := httpClient.Do(cbReq)
		require.NoError(t, err)
		defer cbResp.Body.Close()
		require.Equal(t, http.StatusInternalServerError, cbResp.StatusCode)

		// Validate that the response body contains the generic error message.
		body, err := io.ReadAll(cbResp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), genericCallbackHTTPError)
	})

	// Sub‑test 2: provider is not issuer; external issuer must be rejected.
	t.Run("external issuer rejected when provider not issuer", func(t *testing.T) {
		defer leaktest.AfterTest(t)()
		defer log.Scope(t).Close(t)

		ctx := context.Background()
		srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer srv.Stopper().Stop(ctx)
		s := srv.ApplicationLayer()

		const usernameUnderTest = "externissuer"
		sqlutils.MakeSQLRunner(db).Exec(t, fmt.Sprintf(`CREATE USER "%s"`, usernameUnderTest))

		// Inject manager that returns a token signed by an issuer != provider.
		restore := testutils.TestingHook(
			&NewOIDCManager,
			func(ctx context.Context, c oidcAuthenticationConf, redirectURL string, scopes []string) (IOIDCManager, error) {
				conf := &oauth2.Config{
					ClientID:    c.clientID,
					RedirectURL: redirectURL,
					Endpoint:    oauth2.Endpoint{AuthURL: c.providerURL},
					Scopes:      scopes,
				}

				return &mockOidcManager{
					oauth2Config:        conf,
					claimEmail:          fmt.Sprintf("%s@example.com", usernameUnderTest),
					forceIssuerMismatch: true,
				}, nil
			},
		)
		defer restore()

		// Enable OIDC with a provider that differs from the token's issuer.
		OIDCProviderURL.Override(ctx, &s.ClusterSettings().SV, "https://proxy-provider.example.com")
		OIDCClientID.Override(ctx, &s.ClusterSettings().SV, "cid")
		OIDCClientSecret.Override(ctx, &s.ClusterSettings().SV, "sec")
		OIDCRedirectURL.Override(ctx, &s.ClusterSettings().SV, "https://cockroachlabs.com/oidc/v1/callback")
		OIDCClaimJSONKey.Override(ctx, &s.ClusterSettings().SV, "email")
		OIDCPrincipalRegex.Override(ctx, &s.ClusterSettings().SV, "^([^@]+)@[^@]+$")
		OIDCEnabled.Override(ctx, &s.ClusterSettings().SV, true)

		httpClient, err := s.NewClientRPCContext(ctx, username.TestUserName()).GetHTTPClient()
		require.NoError(t, err)
		httpClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }

		// Begin auth flow (expect 302).
		loginResp, err := httpClient.Get(s.AdminURL().WithPath("/oidc/v1/login").String())
		require.NoError(t, err)
		defer loginResp.Body.Close()
		require.Equal(t, http.StatusFound, loginResp.StatusCode)

		// Extract the real state parameter from the login redirect.
		authURL, err := url.Parse(loginResp.Header.Get("Location"))
		require.NoError(t, err)
		stateParam := authURL.Query().Get("state")

		// Complete callback; CockroachDB must reject the mismatched issuer -> 500.
		cbReq, _ := http.NewRequest("GET", s.AdminURL().WithPath("/oidc/v1/callback").String(), nil)

		// Use the extracted stateParam
		q := cbReq.URL.Query()
		q.Add("state", stateParam)
		q.Add("code", "good-code")
		cbReq.URL.RawQuery = q.Encode()

		for _, c := range loginResp.Cookies() {
			cbReq.AddCookie(c)
		}
		cbResp, err := httpClient.Do(cbReq)
		require.NoError(t, err)
		defer cbResp.Body.Close()
		require.Equal(t, http.StatusInternalServerError, cbResp.StatusCode)

		// Validate that the response body contains the generic error message.
		body, err := io.ReadAll(cbResp.Body)
		require.NoError(t, err)
		require.Contains(t, string(body), genericCallbackHTTPError)
	})
}
