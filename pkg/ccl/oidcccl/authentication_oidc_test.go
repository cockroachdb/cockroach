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
	oauth2Config *oauth2.Config
	claimEmail   string
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

func (m mockOidcManager) ExchangeVerifyGetClaims(
	ctx context.Context, s string, s2 string,
) (map[string]json.RawMessage, error) {
	x := map[string]json.RawMessage{}
	// The email is surrounded by double quotes because it's a serialized JSON string.
	x["email"] = json.RawMessage([]byte(fmt.Sprintf(`"%s"`, m.claimEmail)))
	return x, nil
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
