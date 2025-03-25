// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import "github.com/IBM/go-sdk-core/v5/core"

type Option interface {
	apply(*Provider)
}

type OptionFunc func(p *Provider)

func (o OptionFunc) apply(p *Provider) {
	o(p)
}

// WithAuthenticator assigns an authenticator to be used by the provider
// instead of creating one from the environment.
func WithAuthenticator(authenticator core.Authenticator) OptionFunc {
	return func(p *Provider) {
		p.authenticator = authenticator
	}
}

// WithRegion returns an option to add a supported region.
func WithRegion(region string) OptionFunc {
	return func(p *Provider) {
		if p.regions == nil {
			p.regions = make(map[string]struct{})
		}
		p.regions[region] = struct{}{}
	}
}
