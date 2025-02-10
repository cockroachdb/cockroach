// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprodutil

import (
	"context"
	"net/http"
	"os"
	"time"

	"github.com/binxio/gcloudconfig"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
)

const (
	// CredentialsEnvironmentVariable is the environment variable that takes
	// precedence over the GCP default credentials mechanism.
	CredentialsEnvironmentVariable = "GOOGLE_EPHEMERAL_CREDENTIALS"
	cloudPlatformScope             = "https://www.googleapis.com/auth/cloud-platform"
)

// IAPTokenSource is an interface that defines the methods
// for the IAPTokenSource.
type IAPTokenSource interface {
	Token() (*oauth2.Token, error)
	GetHTTPClient() *http.Client
}

// IAPTokenSourceImpl is a struct that holds the token source and HTTP client
// authenticated with the Identity-Aware Proxy.
// This struct satisfies the oauth2.TokenSource interface.
type IAPTokenSourceImpl struct {
	tokenSource oauth2.TokenSource
	httpClient  *http.Client
}

// IAPTokenSourceOptions is a struct that holds the options for the IAPTokenSource.
type IAPTokenSourceOptions struct {
	// OAuthClientID is the OAuth client ID for the Identity-Aware Proxy.
	OAuthClientID string
	// ServiceAccountEmail is the service account email to impersonate.
	ServiceAccountEmail string
	// Force gcloud credentials to be used.
	ForceGcloud bool
}

// NewIAPTokenSource returns a new IAPTokenSource struct with the given options.
func NewIAPTokenSource(opts IAPTokenSourceOptions) (*IAPTokenSourceImpl, error) {

	if opts.OAuthClientID == "" {
		return nil, errors.New("OAuthClientID is required")
	}

	if opts.ServiceAccountEmail == "" {
		return nil, errors.New("ServiceAccountEmail is required")
	}

	ctx := context.Background()
	var err error
	var creds *google.Credentials
	if cj := os.Getenv(CredentialsEnvironmentVariable); cj != "" {
		// In case a GOOGLE_EPHEMERAL_CREDENTIALS environment variable exist,
		// it takes precedence over other sources, and we use it as our identity.
		creds, err = google.CredentialsFromJSON(ctx, []byte(cj), cloudPlatformScope)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get credentials from environment variable")
		}
	} else {
		// Unless ForceGcloud is set, we try to use ADC first.
		// ForceGcloud might be set if we were able to detect ADC, but there was
		// an error obtaining the credentials during the impersonation.
		if !opts.ForceGcloud {
			// Try and access Google's Application Default Credentials (ADC).
			// This is the default way to get credentials in a GCP environment,
			// and it checks the following sources, in order:
			// - Environment variable GOOGLE_APPLICATION_CREDENTIALS
			// - Default service account file (APP_DATA/application_default_credentials.json)
			// - App Engine standard environment
			// - GCE metadata server
			creds, err = google.FindDefaultCredentials(ctx, cloudPlatformScope)
		}

		// Either error while looking for ADC or ForceGcloud is set and ADC was not attempted.
		if err != nil || opts.ForceGcloud {
			// If we couldn't find any ADC, we default to gcloud session.
			// This is useful if `gcloud auth application-default login` was not run.
			creds, err = gcloudconfig.GetCredentials("")
			if err != nil {
				return nil, errors.Wrapf(err, "failed to get default credentials")
			}
		}
	}

	// Create a new ID token source with the impersonate config.
	// This allows us to impersonate the service account and get an OAuth token.
	// The IncludeEmail field is required for Identity-Aware Proxy.
	ts, err := impersonate.IDTokenSource(ctx, impersonate.IDTokenConfig{
		Audience:        opts.OAuthClientID,
		TargetPrincipal: opts.ServiceAccountEmail,
		IncludeEmail:    true,
	}, option.WithCredentials(creds))
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create impersonated token source")
	}

	// oauth2.ReuseTokenSourceWithExpiry caches the token and reuses it,
	// or refreshes it if it's expired. The token will be refreshed 30 seconds
	// before it expires to avoid any race condition in the token refresh.
	iapTokenSource := &IAPTokenSourceImpl{
		tokenSource: oauth2.ReuseTokenSourceWithExpiry(nil, ts, time.Second*30),
	}

	// Create a new HTTP client with the IAP token source.
	// It automatically adds the Authorization header with the OAuth token.
	// This client uses the httputil.StandardHTTPTimeout as the default timeout.
	iapTokenSource.httpClient = &http.Client{
		Timeout: httputil.StandardHTTPTimeout,
		Transport: &oauth2.Transport{
			Source: iapTokenSource,
			Base:   http.DefaultTransport,
		},
	}

	return iapTokenSource, nil
}

// Token returns the token from the IAPTokenSource.
// This methods satisfies the oauth2.TokenSource interface.
func (ts *IAPTokenSourceImpl) Token() (*oauth2.Token, error) {
	return ts.tokenSource.Token()
}

// GetHTTPClient returns the HTTP client authenticated with the IAPTokenSource.
func (ts *IAPTokenSourceImpl) GetHTTPClient() *http.Client {
	return ts.httpClient
}
