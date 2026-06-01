// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kafkaauth

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/security/secretdir"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProprietaryTokenSource(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	start := time.Now()

	tokResp := proprietaryOAuthResp{
		AccessToken: "MY TOKEN",
		TokenType:   "Bearer",
		ExpiresIn:   3600,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("unexpected request: %s", r.URL)
		w.WriteHeader(http.StatusNotFound)
		t.FailNow()
	})
	mux.HandleFunc("/tokenpls", func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST", r.Method)
		assert.Equal(t, "application/www-url-encoded", r.Header.Get("Content-Type"))
		// Since this is a nonstandard content type, we can't just call r.PostForm().
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		formVals, err := url.ParseQuery(string(body))
		require.NoError(t, err)

		assert.Equal(t, "client_credentials", formVals.Get("grant_type"))
		assert.Equal(t, "my client id", formVals.Get("client_id"))
		assert.Equal(t, "urn:ietf:params:oauth:client-assertion-type:jwt-bearer", formVals.Get("client_assertion_type"))
		assert.Equal(t, "bXkgYXNzZXJ0aW9u", formVals.Get("client_assertion"))
		assert.Equal(t, "my resource", formVals.Get("resource"))
		assert.Len(t, formVals, 5)

		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(tokResp))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	baseURL := srv.URL
	tokenURL, err := url.JoinPath(baseURL, "/tokenpls")
	require.NoError(t, err)

	ctx := context.Background()

	ts := &proprietaryTokenSource{
		tokenURL:            tokenURL,
		clientID:            "my client id",
		getClientAssertion:  func() (string, error) { return "bXkgYXNzZXJ0aW9u", nil }, // "my assertion"
		clientAssertionType: "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
		resource:            "my resource",
		ctx:                 ctx,
		client:              &http.Client{},
	}

	tok, err := ts.Token()
	require.NoError(t, err)
	assert.Equal(t, tokResp.TokenType, tok.TokenType)
	assert.Equal(t, tokResp.AccessToken, tok.AccessToken)
	assert.WithinRange(t, tok.Expiry, start.Add(3600*time.Second), start.Add(3700*time.Second))
}

func TestProprietaryOAuthRegistration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	u, err := url.Parse(`kafka://idk?sasl_enabled=true&sasl_mechanism=PROPRIETARY_OAUTH&sasl_client_id=cl&sasl_token_url=localhost&sasl_proprietary_resource=r&sasl_proprietary_client_assertion_type=at&sasl_proprietary_client_assertion=as`)
	require.NoError(t, err)
	su := &changefeedbase.SinkURL{URL: u}
	mech, ok, err := Pick(su, nil)
	require.NoError(t, err)
	require.True(t, ok)
	require.NotNil(t, mech)
	om, ok := mech.(*saslProprietaryOAuth)
	require.True(t, ok)
	require.Empty(t, su.RemainingQueryParams())
	require.Equal(t, "cl", om.clientID)
	require.Equal(t, "localhost", om.tokenURL)
	require.Equal(t, "r", om.resource)
	require.Equal(t, "at", om.clientAssertionType)
	require.Equal(t, "as", om.clientAssertion)
}

func TestProprietaryOAuthValidateParams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const base = `kafka://b?sasl_enabled=true&sasl_mechanism=PROPRIETARY_OAUTH` +
		`&sasl_client_id=c&sasl_token_url=u` +
		`&sasl_proprietary_resource=r&sasl_proprietary_client_assertion_type=at`

	tests := []struct {
		name        string
		uri         string
		expectedErr string
	}{
		{
			name: "inline-assertion happy path",
			uri:  base + `&sasl_proprietary_client_assertion=as`,
		},
		{
			name: "file-mode happy path",
			uri:  base + `&sasl_proprietary_client_assertion_location=/p/oidc.jwt`,
		},
		{
			name:        "both assertion and location rejected",
			uri:         base + `&sasl_proprietary_client_assertion=as&sasl_proprietary_client_assertion_location=/p/oidc.jwt`,
			expectedErr: "sasl_proprietary_client_assertion and sasl_proprietary_client_assertion_location cannot be used together",
		},
		{
			name:        "neither assertion nor location rejected",
			uri:         base,
			expectedErr: "one of sasl_proprietary_client_assertion or sasl_proprietary_client_assertion_location must be provided",
		},
		{
			name:        "missing client_id",
			uri:         `kafka://b?sasl_enabled=true&sasl_mechanism=PROPRIETARY_OAUTH&sasl_token_url=u&sasl_proprietary_resource=r&sasl_proprietary_client_assertion_type=at&sasl_proprietary_client_assertion=as`,
			expectedErr: "sasl_client_id must be provided",
		},
		{
			name:        "missing token_url",
			uri:         `kafka://b?sasl_enabled=true&sasl_mechanism=PROPRIETARY_OAUTH&sasl_client_id=c&sasl_proprietary_resource=r&sasl_proprietary_client_assertion_type=at&sasl_proprietary_client_assertion=as`,
			expectedErr: "sasl_token_url must be provided",
		},
		{
			name:        "missing resource",
			uri:         `kafka://b?sasl_enabled=true&sasl_mechanism=PROPRIETARY_OAUTH&sasl_client_id=c&sasl_token_url=u&sasl_proprietary_client_assertion_type=at&sasl_proprietary_client_assertion=as`,
			expectedErr: "sasl_proprietary_resource must be provided",
		},
	}

	// Use a reader rooted at /p so the file-mode happy path's location
	// (/p/oidc.jwt) survives the Pick-time injection check.
	reader, err := secretdir.NewReader("/p")
	require.NoError(t, err)

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(tc.uri)
			require.NoError(t, err)
			_, _, err = Pick(&changefeedbase.SinkURL{URL: u}, reader)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestProprietaryOAuthFileMode verifies that when sasl_proprietary_client_assertion_location
// is set, the JWT is re-read from disk on each Token() call — so an external
// rotator can resign the assertion without restarting the changefeed.
func TestProprietaryOAuthFileMode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var receivedAssertion atomic.Value
	receivedAssertion.Store("")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		vals, err := url.ParseQuery(string(body))
		require.NoError(t, err)
		receivedAssertion.Store(vals.Get("client_assertion"))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(proprietaryOAuthResp{
			AccessToken: "bearer-1", TokenType: "Bearer", ExpiresIn: 60,
		}))
	}))
	defer srv.Close()

	dir := t.TempDir()
	assertionPath := filepath.Join(dir, "assertion.jwt")
	require.NoError(t, os.WriteFile(assertionPath, []byte("jwt-v1\n"), 0o600))

	reader, err := secretdir.NewReader(dir)
	require.NoError(t, err)

	mech := &saslProprietaryOAuth{
		clientID:                "cl",
		tokenURL:                srv.URL,
		resource:                "r",
		clientAssertionType:     "at",
		clientAssertionLocation: assertionPath,
		secretReader:            reader,
	}
	ts := mech.newTokenSource(context.Background())

	tok, err := ts.Token()
	require.NoError(t, err)
	require.Equal(t, "bearer-1", tok.AccessToken)
	// TrimSpace strips the trailing newline.
	require.Equal(t, "jwt-v1", receivedAssertion.Load().(string))

	// Rotate the assertion on disk; the next Token() call must pick it up.
	require.NoError(t, os.WriteFile(assertionPath, []byte("jwt-v2"), 0o600))
	_, err = ts.Token()
	require.NoError(t, err)
	require.Equal(t, "jwt-v2", receivedAssertion.Load().(string))
}

// TestProprietaryOAuthFileModeWithoutSecretDirectory verifies that a file-mode
// URI is rejected at Pick time when --secret-directory is unset, rather than
// silently constructing a mechanism that will fail at first bearer mint.
func TestProprietaryOAuthFileModeWithoutSecretDirectory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	u, err := url.Parse(`kafka://b?sasl_enabled=true&sasl_mechanism=PROPRIETARY_OAUTH` +
		`&sasl_client_id=c&sasl_token_url=u&sasl_proprietary_resource=r` +
		`&sasl_proprietary_client_assertion_type=at` +
		`&sasl_proprietary_client_assertion_location=/p/oidc.jwt`)
	require.NoError(t, err)

	_, _, err = Pick(&changefeedbase.SinkURL{URL: u}, nil)
	require.ErrorContains(t, err, "--secret-directory is not configured")
}

func TestProprietaryOAuthFileModePathValidation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	reader, err := secretdir.NewReader("/var/secrets")
	require.NoError(t, err)

	tests := []struct {
		name        string
		location    string
		expectedErr string
	}{
		{
			name:        "non-absolute path rejected",
			location:    "oidc.jwt",
			expectedErr: "must be absolute",
		},
		{
			name:        "path outside secret directory rejected",
			location:    "/etc/passwd",
			expectedErr: `escapes --secret-directory "/var/secrets"`,
		},
		{
			name:        "path in sibling-prefixed directory rejected",
			location:    "/var/secretsX/jwt",
			expectedErr: `escapes --secret-directory "/var/secrets"`,
		},
		{
			name:        "path with .. escape rejected",
			location:    "/var/secrets/../etc/passwd",
			expectedErr: `escapes --secret-directory "/var/secrets"`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			uri := `kafka://b?sasl_enabled=true&sasl_mechanism=PROPRIETARY_OAUTH` +
				`&sasl_client_id=c&sasl_token_url=u&sasl_proprietary_resource=r` +
				`&sasl_proprietary_client_assertion_type=at` +
				`&sasl_proprietary_client_assertion_location=` + tc.location
			u, err := url.Parse(uri)
			require.NoError(t, err)
			_, _, err = Pick(&changefeedbase.SinkURL{URL: u}, reader)
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
