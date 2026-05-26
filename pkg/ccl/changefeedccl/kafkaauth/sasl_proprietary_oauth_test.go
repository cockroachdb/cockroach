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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
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
		clientAssertion:     "bXkgYXNzZXJ0aW9u", // "my assertion"
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
	mech, ok, err := Pick(su)
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
