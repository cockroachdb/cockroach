// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ibm

import (
	"github.com/IBM/go-sdk-core/v5/core"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

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

// WithDNSProvider returns an option to set the DNS provider.
func WithDNSProvider(dnsProvider vm.DNSProvider) OptionFunc {
	return func(p *Provider) {
		p.dnsProvider = dnsProvider
	}
}

// ProviderOptions holds options for configuring the IBM provider.
type ProviderOptions struct {
	DNSProvider vm.DNSProvider
	// AccountID is the IBM Cloud account ID. It is used by
	// roachprod-centralized to derive the provider's identity string
	// (e.g., "ibm-<accountID>") for environment-based authorization
	// without needing to create a live provider instance.
	AccountID string
}

// ToOptions converts ProviderOptions to a slice of Option functions to be used
// in NewProvider(opts ...Option).
func (po *ProviderOptions) ToOptions() []Option {
	var opts []Option
	if po.DNSProvider != nil {
		opts = append(opts, WithDNSProvider(po.DNSProvider))
	}
	return opts
}
