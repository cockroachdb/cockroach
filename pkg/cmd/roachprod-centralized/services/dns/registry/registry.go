// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"log/slog"
	"strings"

	configtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/cockroachdb/errors"
)

// Registry manages DNS provider instances, creating them once and providing
// access to both clusters and public-dns services. This breaks the circular
// dependency between these services.
type Registry struct {
	// providers maps DNS domain to DNS provider instance
	dnsProviders map[string]vm.DNSProvider
}

// NewRegistry creates a new DNS provider registry from configuration.
// It initializes DNS providers based on the provided cloud provider configs
// and stores them keyed by their DNS domain.
func NewRegistry(l *logger.Logger, configs []configtypes.DNSProvider) (*Registry, error) {
	registry := &Registry{
		dnsProviders: make(map[string]vm.DNSProvider),
	}

	for i, cp := range configs {
		switch strings.ToLower(cp.Type) {
		// Only GCE is supported for now.
		case gce.ProviderName:

			provider := gce.NewDNSProvider(cp.GCE)

			// Store by DNS domain for easy lookup
			domain := provider.PublicDomain()
			if domain == "" {
				l.Debug(
					"skipping DNS provider without domain",
					slog.Int("index", i),
					slog.String("type", cp.Type),
					slog.Any("config", cp.GCE),
				)
				// Skip providers without DNS configuration
				continue
			}

			l.Debug(
				"registered DNS provider",
				slog.String("type", cp.Type),
				slog.String("domain", domain),
			)

			registry.dnsProviders[domain] = provider

		case "gce-sdk":

			provider, err := gce.NewSDKDNSProvider(cp.GCESDK)
			if err != nil {
				l.Error(
					"unable to initialize GCE DNS Provider",
					slog.Any("error", err),
					slog.Int("index", i),
					slog.String("type", cp.Type),
					slog.Any("config", cp.GCESDK),
				)
				return nil, errors.Wrap(err, "unable to initialize GCE DNS provider")
			}

			// Store by DNS domain for easy lookup
			domain := provider.PublicDomain()
			if domain == "" {
				l.Debug(
					"skipping DNS provider without domain",
					slog.Int("index", i),
					slog.String("type", cp.Type),
					slog.Any("config", cp.GCESDK),
				)
				// Skip providers without DNS configuration
				continue
			}

			l.Debug(
				"registered DNS provider",
				slog.String("type", cp.Type),
				slog.String("domain", domain),
			)

			registry.dnsProviders[domain] = provider
		default:
			// Silently skip unsupported provider types for DNS
			// (they may still be valid cloud providers for clusters service)
			continue
		}
	}

	l.Info(
		"DNS provider registry initialized",
		slog.Int("provider_count", len(registry.dnsProviders)),
		slog.Int("configured_count", len(configs)),
		slog.String("domains", strings.Join(registry.GetProviderDomains(), ", ")),
	)

	return registry, nil
}

// GetProvider returns the DNS provider for the given domain.
// Returns nil and false if no provider exists for that domain.
func (r *Registry) GetDNSProvider(domain string) (vm.DNSProvider, bool) {
	if r == nil || r.dnsProviders == nil {
		return nil, false
	}
	provider, ok := r.dnsProviders[domain]
	return provider, ok
}

// GetAllProviders returns a map of all DNS providers keyed by their domain.
// Returns an empty map if no providers are registered.
func (r *Registry) GetAllDNSProviders() map[string]vm.DNSProvider {
	if r == nil || r.dnsProviders == nil {
		return make(map[string]vm.DNSProvider)
	}
	return r.dnsProviders
}

// GetProviderDomains returns a list of all registered DNS domains.
func (r *Registry) GetProviderDomains() []string {
	if r == nil || r.dnsProviders == nil {
		return []string{}
	}
	domains := make([]string, 0, len(r.dnsProviders))
	for domain := range r.dnsProviders {
		domains = append(domains, domain)
	}
	return domains
}

// IsEmpty returns true if the registry has no providers.
func (r *Registry) IsEmpty() bool {
	return r == nil || len(r.dnsProviders) == 0
}
