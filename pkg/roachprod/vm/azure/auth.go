// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package azure

import (
	"net/http"

	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure/auth"
	"github.com/cockroachdb/errors"
)

// getAuthorizer returns an Authorizer, which uses the Azure CLI
// to log into the portal.
//
// It would be possible to implement an OAuth2 flow, avoiding the need
// to install the Azure CLI.
//
// The Authorizer is memoized in the Provider.
func (p *Provider) getAuthorizer() (ret autorest.Authorizer, err error) {
	p.mu.Lock()
	ret = p.mu.authorizer
	p.mu.Unlock()
	if ret != nil {
		return
	}

	// Use the azure CLI to bootstrap our authentication.
	// https://docs.microsoft.com/en-us/go/azure/azure-sdk-go-authorization
	ret, err = auth.NewAuthorizerFromCLI()
	if err == nil {
		p.mu.Lock()
		p.mu.authorizer = ret
		p.mu.Unlock()
	} else {
		err = errors.Wrap(err, "could got get Azure auth token")
	}
	return
}

// getAuthToken extracts the JWT token from the active Authorizer.
func (p *Provider) getAuthToken() (string, error) {
	auth, err := p.getAuthorizer()
	if err != nil {
		return "", err
	}

	// We'll steal the auth Bearer token by creating a fake HTTP request.
	fake := &http.Request{}
	if _, err := auth.WithAuthorization()(&stealAuth{}).Prepare(fake); err != nil {
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
