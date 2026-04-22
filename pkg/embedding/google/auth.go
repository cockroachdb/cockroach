// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package google

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
)

const (
	tokenEndpoint = "https://oauth2.googleapis.com/token"
	// refreshBuffer is how early we refresh before the token expires.
	refreshBuffer = 5 * time.Minute
)

// ServiceAccountKey holds the fields we need from a Google service
// account JSON key file.
type ServiceAccountKey struct {
	ClientEmail string `json:"client_email"`
	PrivateKey  string `json:"private_key"`
	ProjectID   string `json:"project_id"`
}

// ParseServiceAccountKey decodes a base64-encoded service account
// JSON key and returns the parsed key.
func ParseServiceAccountKey(b64 string) (ServiceAccountKey, error) {
	jsonBytes, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return ServiceAccountKey{}, errors.Wrap(
			err, "decoding base64 service account credentials",
		)
	}
	var key ServiceAccountKey
	if err := json.Unmarshal(jsonBytes, &key); err != nil {
		return ServiceAccountKey{}, errors.Wrap(
			err, "parsing service account JSON",
		)
	}
	if key.ClientEmail == "" || key.PrivateKey == "" {
		return ServiceAccountKey{}, errors.New(
			"service account JSON missing client_email or private_key",
		)
	}
	return key, nil
}

// tokenSource provides OAuth2 access tokens from a service account,
// caching and auto-refreshing as needed. Safe for concurrent use.
type tokenSource struct {
	key        ServiceAccountKey
	httpClient *httputil.Client

	mu          sync.Mutex
	cachedToken string
	expiry      time.Time
}

func newTokenSource(key ServiceAccountKey, httpClient *httputil.Client) *tokenSource {
	return &tokenSource{
		key:        key,
		httpClient: httpClient,
	}
}

// token returns a valid access token, refreshing if necessary.
func (ts *tokenSource) token(ctx context.Context) (string, error) {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	if ts.cachedToken != "" && time.Now().Before(ts.expiry) {
		return ts.cachedToken, nil
	}

	signedJWT, err := ts.signJWT()
	if err != nil {
		return "", err
	}

	token, expiry, err := ts.exchangeJWT(ctx, signedJWT)
	if err != nil {
		return "", err
	}

	ts.cachedToken = token
	ts.expiry = expiry
	return token, nil
}

// signJWT creates a signed JWT for the OAuth2 token exchange.
func (ts *tokenSource) signJWT() (string, error) {
	now := time.Now()
	header := base64URLEncode([]byte(`{"alg":"RS256","typ":"JWT"}`))

	claims, err := json.Marshal(map[string]interface{}{
		"iss":   ts.key.ClientEmail,
		"scope": "https://www.googleapis.com/auth/cloud-platform",
		"aud":   tokenEndpoint,
		"iat":   now.Unix(),
		"exp":   now.Add(time.Hour).Unix(),
	})
	if err != nil {
		return "", errors.Wrap(err, "marshaling JWT claims")
	}
	payload := base64URLEncode(claims)

	signingInput := header + "." + payload

	// Parse the PEM-encoded private key.
	block, _ := pem.Decode([]byte(ts.key.PrivateKey))
	if block == nil {
		return "", errors.New("failed to decode PEM block from private key")
	}

	privateKey, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		// Fall back to PKCS1 format.
		privateKey, err = x509.ParsePKCS1PrivateKey(block.Bytes)
		if err != nil {
			return "", errors.Wrap(err, "parsing private key")
		}
	}

	rsaKey, ok := privateKey.(*rsa.PrivateKey)
	if !ok {
		return "", errors.New("private key is not RSA")
	}

	hash := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, rsaKey, crypto.SHA256, hash[:])
	if err != nil {
		return "", errors.Wrap(err, "signing JWT")
	}

	return signingInput + "." + base64URLEncode(sig), nil
}

// exchangeJWT sends the signed JWT to Google's token endpoint and
// returns the access token and its expiry time.
func (ts *tokenSource) exchangeJWT(ctx context.Context, jwt string) (string, time.Time, error) {
	form := url.Values{
		"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"},
		"assertion":  {jwt},
	}

	req, err := http.NewRequestWithContext(
		ctx, http.MethodPost, tokenEndpoint,
		strings.NewReader(form.Encode()),
	)
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "creating token request")
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := ts.httpClient.Do(req)
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "token exchange request")
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", time.Time{}, errors.Wrap(err, "reading token response")
	}

	if resp.StatusCode != http.StatusOK {
		return "", time.Time{}, errors.Newf(
			"token exchange failed (HTTP %d): %s", resp.StatusCode, body,
		)
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
		TokenType   string `json:"token_type"`
	}
	if err := json.Unmarshal(body, &tokenResp); err != nil {
		return "", time.Time{}, errors.Wrap(err, "decoding token response")
	}
	if tokenResp.AccessToken == "" {
		return "", time.Time{}, errors.New("empty access_token in response")
	}

	expiry := time.Now().Add(
		time.Duration(tokenResp.ExpiresIn)*time.Second - refreshBuffer,
	)
	return tokenResp.AccessToken, expiry, nil
}

// base64URLEncode encodes data using base64url without padding.
func base64URLEncode(data []byte) string {
	return base64.RawURLEncoding.EncodeToString(data)
}

// FormatCredentialsHint returns a user-facing hint showing how to
// create the external connection with service account credentials.
func FormatCredentialsHint(region string) string {
	return fmt.Sprintf(
		"CREATE EXTERNAL CONNECTION google AS "+
			"'https://%s-aiplatform.googleapis.com/v1?project=PROJECT&credentials=BASE64_SERVICE_ACCOUNT_JSON'",
		region,
	)
}
