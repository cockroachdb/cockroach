// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import "github.com/cockroachdb/cockroach/pkg/roachprod/vm"

type ProviderOptions struct {
	Project          string
	MetadataProject  string
	DNSProject       string
	DNSPublicZone    string
	DNSPublicDomain  string
	DNSManagedZone   string
	DNSManagedDomain string
}

type OptionFunc func(p *Provider)

func (o OptionFunc) apply(p *Provider) {
	o(p)
}

// WithProject returns an option to add a project.
func WithProject(project string) OptionFunc {
	return func(p *Provider) {
		if p.Projects == nil {
			p.Projects = []string{}
		}
		p.Projects = append(p.Projects, project)
	}
}

// WithDefaultProject returns an option to set the default project.
func WithDefaultProject(project string) OptionFunc {
	return func(p *Provider) {
		p.defaultProject = project
	}
}

// WithMetadataProject returns an option to set the metadata project.
func WithMetadataProject(project string) OptionFunc {
	return func(p *Provider) {
		p.metadataProject = project
	}
}

// WithDNSProject returns an option to set the DNS project.
func WithDNSProject(project string) OptionFunc {
	return func(p *Provider) {
		if p.dnsProvider == nil {
			p.dnsProvider = &dnsProvider{}
		}
		p.dnsProvider.dnsProject = project
	}
}

// WithDNSPublicZone returns an option to set the public DNS zone.
func WithDNSPublicZone(zone string) OptionFunc {
	return func(p *Provider) {
		if p.dnsProvider == nil {
			p.dnsProvider = &dnsProvider{}
		}
		p.dnsProvider.publicZone = zone
	}
}

// WithDNSPublicDomain returns an option to set the public DNS domain.
func WithDNSPublicDomain(domain string) OptionFunc {
	return func(p *Provider) {
		if p.dnsProvider == nil {
			p.dnsProvider = &dnsProvider{}
		}
		p.dnsProvider.publicDomain = domain
	}
}

// WithDNSManagedZone returns an option to set the managed DNS zone.
func WithDNSManagedZone(zone string) OptionFunc {
	return func(p *Provider) {
		if p.dnsProvider == nil {
			p.dnsProvider = &dnsProvider{}
		}
		p.dnsProvider.managedZone = zone
	}
}

// WithDNSManagedDomain returns an option to set the managed DNS domain.
func WithDNSManagedDomain(domain string) OptionFunc {
	return func(p *Provider) {
		if p.dnsProvider == nil {
			p.dnsProvider = &dnsProvider{}
		}
		p.dnsProvider.managedDomain = domain
	}
}

// WithDNSProvider returns an option to set the DNS provider.
// TODO(golgeek): GCE only supports its own DNS provider, so we try and cast
// the interface before setting it. In case the cast fails, we ignore the provided
// DNS provider. This will need to be revisited if we ever want to support non-GCE
// DNS providers.
func WithDNSProvider(dnsP vm.DNSProvider) OptionFunc {
	return func(p *Provider) {
		if gceDnsProvider, isGCE := dnsP.(*dnsProvider); isGCE {
			p.dnsProvider = gceDnsProvider
		}
	}
}

// Provider is a struct that holds configuration for the GCE provider.
type Option interface {
	apply(*Provider)
}

// ToOptions converts ProviderOptions to a slice of Option functions to be used
// in NewProvider(opts ...Option).
func (po *ProviderOptions) ToOptions() []Option {
	var opts []Option
	if po.Project != "" {
		opts = append(opts, WithProject(po.Project))
	}
	if po.MetadataProject != "" {
		opts = append(opts, WithMetadataProject(po.MetadataProject))
	}
	if po.DNSProject != "" {
		opts = append(opts, WithDNSProject(po.DNSProject))
	}
	if po.DNSPublicZone != "" {
		opts = append(opts, WithDNSPublicZone(po.DNSPublicZone))
	}
	if po.DNSPublicDomain != "" {
		opts = append(opts, WithDNSPublicDomain(po.DNSPublicDomain))
	}
	if po.DNSManagedZone != "" {
		opts = append(opts, WithDNSManagedZone(po.DNSManagedZone))
	}
	if po.DNSManagedDomain != "" {
		opts = append(opts, WithDNSManagedDomain(po.DNSManagedDomain))
	}
	return opts
}
