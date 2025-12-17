// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package azure

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

type Option interface {
	apply(*Provider)
}

type OptionFunc func(p *Provider)

func (o OptionFunc) apply(p *Provider) {
	o(p)
}

// WithProject returns an option to add a project.
func WithSubscriptionName(subscriptionName string) OptionFunc {
	return func(p *Provider) {
		if p.SubscriptionNames == nil {
			p.SubscriptionNames = []string{}
		}
		p.SubscriptionNames = append(p.SubscriptionNames, subscriptionName)
	}
}

// WithOperationTimeout returns an option to set the operation timeout.
func WithOperationTimeout(d time.Duration) OptionFunc {
	return func(p *Provider) {
		p.OperationTimeout = d
	}
}

// WithSyncDelete returns an option to set the sync delete flag.
func WithSyncDelete(sync bool) OptionFunc {
	return func(p *Provider) {
		p.SyncDelete = sync
	}
}

// WithDNSProvider returns an option to set the DNS provider.
func WithDNSProvider(dnsProvider vm.DNSProvider) OptionFunc {
	return func(p *Provider) {
		p.dnsProvider = dnsProvider
	}
}

// ProviderOptions holds options for configuring the Azure provider.
type ProviderOptions struct {
	SubscriptionName string
	OperationTimeout time.Duration
	SyncDelete       bool
	DNSProvider      vm.DNSProvider
}

// ToOptions converts ProviderOptions to a slice of Option functions to be used
// in NewProvider(opts ...Option).
func (po *ProviderOptions) ToOptions() []Option {
	var opts []Option
	if po.SubscriptionName != "" {
		opts = append(opts, WithSubscriptionName(po.SubscriptionName))
	}
	if po.OperationTimeout != 0 {
		opts = append(opts, WithOperationTimeout(po.OperationTimeout))
	}
	if po.SyncDelete {
		opts = append(opts, WithSyncDelete(po.SyncDelete))
	}
	if po.DNSProvider != nil {
		opts = append(opts, WithDNSProvider(po.DNSProvider))
	}
	return opts
}
