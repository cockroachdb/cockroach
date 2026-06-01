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
	mech, ok, err := Pick(su, SASLConfig{})
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
	assertion, err := om.getClientAssertion()
	require.NoError(t, err)
	require.Equal(t, "as", assertion)
}

func TestProprietaryOAuthOnlyParamsRejected(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const base = `kafka://b?sasl_enabled=true&sasl_mechanism=PLAIN` +
		`&sasl_user=u&sasl_password=p`

	tests := []struct {
		name        string
		extraParam  string
		expectedErr string
	}{
		{
			name:        "proprietary resource rejected under PLAIN",
			extraParam:  `sasl_proprietary_resource=r`,
			expectedErr: "sasl_proprietary_resource is not a valid parameter for sasl_mechanism=PLAIN",
		},
		{
			name:        "proprietary client assertion rejected under PLAIN",
			extraParam:  `sasl_proprietary_client_assertion=as`,
			expectedErr: "sasl_proprietary_client_assertion is not a valid parameter for sasl_mechanism=PLAIN",
		},
		{
			name:        "proprietary client assertion type rejected under PLAIN",
			extraParam:  `sasl_proprietary_client_assertion_type=at`,
			expectedErr: "sasl_proprietary_client_assertion_type is not a valid parameter for sasl_mechanism=PLAIN",
		},
		{
			name:        "proprietary client assertion location rejected under PLAIN",
			extraParam:  `sasl_proprietary_client_assertion_location=jwt`,
			expectedErr: "sasl_proprietary_client_assertion_location is not a valid parameter for sasl_mechanism=PLAIN",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			u, err := url.Parse(base + "&" + tc.extraParam)
			require.NoError(t, err)
			_, _, err = Pick(&changefeedbase.SinkURL{URL: u}, SASLConfig{})
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}

func TestProprietaryOAuthClientAssertionParams(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const baseRequired = `kafka://b?sasl_enabled=true&sasl_mechanism=PROPRIETARY_OAUTH` +
		`&sasl_client_id=cl&sasl_token_url=localhost` +
		`&sasl_proprietary_resource=r&sasl_proprietary_client_assertion_type=at`

	tests := []struct {
		name        string
		extraParams string
		expectedErr string
	}{
		{
			name:        "neither assertion nor location",
			expectedErr: "one of sasl_proprietary_client_assertion or sasl_proprietary_client_assertion_location must be provided",
		},
		{
			name:        "both assertion and location",
			extraParams: `sasl_proprietary_client_assertion=inline&sasl_proprietary_client_assertion_location=jwt`,
			expectedErr: "sasl_proprietary_client_assertion and sasl_proprietary_client_assertion_location cannot be used together",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := baseRequired
			if tc.extraParams != "" {
				raw += "&" + tc.extraParams
			}
			u, err := url.Parse(raw)
			require.NoError(t, err)
			_, _, err = Pick(&changefeedbase.SinkURL{URL: u}, SASLConfig{})
			require.ErrorContains(t, err, tc.expectedErr)
		})
	}
}
