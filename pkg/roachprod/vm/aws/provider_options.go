// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package aws

import "github.com/cockroachdb/cockroach/pkg/roachprod/vm"

type Option interface {
	apply(*Provider)
}

type OptionFunc func(p *Provider)

func (o OptionFunc) apply(p *Provider) {
	o(p)
}

// WithProfile returns an option to add a profile.
func WithProfile(profile string) OptionFunc {
	return func(p *Provider) {
		p.Profile = profile
	}
}

// WithConfig returns an option to add a config.
func WithConfig(config awsConfig) OptionFunc {
	return func(p *Provider) {
		p.Config = awsConfigValue{
			awsConfig: config,
		}
	}
}

// WithAccountID returns an option to add an account ID.
func WithAccountID(accountID string) OptionFunc {
	return func(p *Provider) {
		if p.AccountIDs == nil {
			p.AccountIDs = []string{}
		}
		p.AccountIDs = append(p.AccountIDs, accountID)
	}
}

// WithAssumeSTSRole returns an option to assume an STS role.
func WithAssumeSTSRole(role string) OptionFunc {
	return func(p *Provider) {
		p.AssumeSTSRole = role
	}
}

func WithDNSProvider(dnsProvider vm.DNSProvider) OptionFunc {
	return func(p *Provider) {
		p.dnsProvider = dnsProvider
	}
}

// ProviderOptions holds options for configuring the AWS provider.
type ProviderOptions struct {
	Profile       string
	AccountID     string
	AssumeSTSRole string
	DNSProvider   vm.DNSProvider
}

// ToOptions converts ProviderOptions to a slice of Option functions to be used
// in NewProvider(opts ...Option).
func (po *ProviderOptions) ToOptions() []Option {
	var opts []Option
	if po.Profile != "" {
		opts = append(opts, WithProfile(po.Profile))
	}
	if po.AccountID != "" {
		opts = append(opts, WithAccountID(po.AccountID))
	}
	if po.AssumeSTSRole != "" {
		opts = append(opts, WithAssumeSTSRole(po.AssumeSTSRole))
	}
	if po.DNSProvider != nil {
		opts = append(opts, WithDNSProvider(po.DNSProvider))
	}
	return opts
}
