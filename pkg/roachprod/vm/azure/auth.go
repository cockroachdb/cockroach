// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"net/http"
	"os"
	"time"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/adal"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/cockroachdb/errors"
)

// We look to the environment for sdk authentication first.
var hasEnvAuth = os.Getenv("AZURE_TENANT_ID") != "" &&
	os.Getenv("AZURE_CLIENT_ID") != "" &&
	os.Getenv("AZURE_CLIENT_SECRET") != ""

// getAuthorizer returns an Authorizer, which uses the Azure CLI
// to log into the portal.
//
// It would be possible to implement an OAuth2 flow, avoiding the need
// to install the Azure CLI.
//
// The Authorizer is memoized in the Provider.
func (p *Provider) getAuthorizer() (autorest.Authorizer, error) {
	var authorizer autorest.Authorizer
	p.mu.Lock()
	authorizer = p.mu.authorizer
	p.mu.Unlock()

	if authorizer != nil {
		// Return the memoized authorizer if isn't soon to or already expired.
		shouldRefresh, err := shouldRefreshAuth(authorizer)
		if err != nil {
			return nil, err
		}

		if !shouldRefresh {
			return authorizer, nil
		}
	}
	// Use the environment or azure CLI to bootstrap our authentication.
	// https://docs.microsoft.com/en-us/go/azure/azure-sdk-go-authorization
	var err error
	if hasEnvAuth {
		authorizer, err = auth.NewAuthorizerFromEnvironment()
	} else {
		authorizer, err = auth.NewAuthorizerFromCLI()
	}

	if err != nil {
		return nil, errors.Wrap(err, "could not get Azure auth token")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	p.mu.authorizer = authorizer

	return authorizer, err
}

// shouldRefreshAuth conservatively returns true if the current token is set to expire within 5 minutes.
// CLI based tokens can have a variable length expiry of 5 - 60 minutes.
// https://learn.microsoft.com/en-us/cli/azure/account?view=azure-cli-latest#az-account-get-access-token()
func shouldRefreshAuth(authorizer autorest.Authorizer) (bool, error) {
	// Environment based auth does not expire.
	if hasEnvAuth {
		return false, nil
	}
	bearerAuthorizer, ok := authorizer.(*autorest.BearerAuthorizer)
	if !ok || bearerAuthorizer == nil {
		return false, errors.New("failed to cast authorizer to *autorest.BearerAuthorizer")
	}

	token, ok := bearerAuthorizer.TokenProvider().(*adal.Token)
	if !ok || token == nil {
		return false, errors.New("failed to cast adal.OAuthTokenProvider to *adal.Token")
	}

	return token.WillExpireIn(5 * time.Minute), nil
}

// getAuthToken extracts the JWT token from the active Authorizer.
func (p *Provider) getAuthToken() (string, error) {
	authorizer, err := p.getAuthorizer()
	if err != nil {
		return "", err
	}

	// We'll steal the auth Bearer token by creating a fake HTTP request.
	fake := &http.Request{}
	if _, err := authorizer.WithAuthorization()(&stealAuth{}).Prepare(fake); err != nil {
		return "", err
	}
	return fake.Header.Get("Authorization")[7:], nil
}

type stealAuth struct {
}

var _ autorest.Preparer = &stealAuth{}

// Prepare implements the autorest.Preparer interface.
func (*stealAuth) Prepare(r *http.Request) (*http.Request, error) {
	return r, nil
}
