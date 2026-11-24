// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package registry

import (
	"testing"

	configtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/config/types"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/stretchr/testify/require"
)

func TestNewRegistry_Empty(t *testing.T) {
	registry, err := NewRegistry(logger.DefaultLogger, []configtypes.DNSProvider{})
	require.NoError(t, err)
	require.NotNil(t, registry)
	require.True(t, registry.IsEmpty())
	require.Empty(t, registry.GetAllDNSProviders())
	require.Empty(t, registry.GetProviderDomains())
}

func TestNewRegistry_WithGCEProvider(t *testing.T) {
	configs := []configtypes.DNSProvider{
		{
			Type: gce.ProviderName,
			GCE: gce.DNSProviderOpts{
				DNSProject:    "test-dns-project",
				PublicZone:    "test-zone",
				PublicDomain:  "test.example.com",
				ManagedZone:   "test-managed-zone",
				ManagedDomain: "test-managed.example.com",
			},
		},
	}

	registry, err := NewRegistry(logger.DefaultLogger, configs)
	require.NoError(t, err)
	require.NotNil(t, registry)
	require.False(t, registry.IsEmpty())

	providers := registry.GetAllDNSProviders()
	require.Len(t, providers, 1)

	// Verify we can get the provider by public domain (DNSDomain() returns publicDomain)
	provider, ok := registry.GetDNSProvider("test.example.com")
	require.True(t, ok)
	require.NotNil(t, provider)
	// Domain() returns managedDomain, not publicDomain
	require.Equal(t, "test-managed.example.com", provider.Domain())

	// Verify domains list (keyed by publicDomain from DNSDomain())
	domains := registry.GetProviderDomains()
	require.Len(t, domains, 1)
	require.Contains(t, domains, "test.example.com")
}

func TestNewRegistry_WithoutDNSConfig(t *testing.T) {
	// Provider without DNS configuration should be skipped
	configs := []configtypes.DNSProvider{
		{
			Type: gce.ProviderName,
			GCE: gce.DNSProviderOpts{
				DNSProject: "test-dns-project",
				// No DNS config
			},
		},
	}

	registry, err := NewRegistry(logger.DefaultLogger, configs)
	require.NoError(t, err)
	require.NotNil(t, registry)
	require.True(t, registry.IsEmpty())
}

func TestNewRegistry_UnsupportedProvider(t *testing.T) {
	// Non-GCE providers should be silently skipped for DNS
	configs := []configtypes.DNSProvider{
		{
			Type: "aws",
		},
	}

	registry, err := NewRegistry(logger.DefaultLogger, configs)
	require.NoError(t, err)
	require.NotNil(t, registry)
	require.True(t, registry.IsEmpty())
}

func TestGetProvider_NonExistent(t *testing.T) {
	registry, err := NewRegistry(logger.DefaultLogger, []configtypes.DNSProvider{})
	require.NoError(t, err)

	provider, ok := registry.GetDNSProvider("nonexistent.example.com")
	require.False(t, ok)
	require.Nil(t, provider)
}

func TestNilRegistry(t *testing.T) {
	var registry *Registry

	provider, ok := registry.GetDNSProvider("test.com")
	require.False(t, ok)
	require.Nil(t, provider)

	providers := registry.GetAllDNSProviders()
	require.Empty(t, providers)

	domains := registry.GetProviderDomains()
	require.Empty(t, domains)

	require.True(t, registry.IsEmpty())
}
