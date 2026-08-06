// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"io"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
)

func TestProjectConfiguration(t *testing.T) {
	oldVMProject := defaultVMProject
	oldInfraProject := defaultInfraProject
	oldMetadataProject := defaultMetadataProject
	oldMetadataProjectExplicit := defaultMetadataProjectExplicit
	oldDNSProject := defaultDNSProject
	oldArtifactsBucket := defaultArtifactsBucket
	oldArtifactsBucketExplicit := defaultArtifactsBucketExplicit
	oldDNSProjectExplicit := defaultDNSProjectExplicit
	oldDNSZone := dnsDefaultZone
	oldDNSDomain := dnsDefaultDomain
	oldDNSDomainExplicit := dnsDefaultDomainExplicit
	oldDNSManagedZone := dnsDefaultManagedZone
	oldDNSManagedDomain := dnsDefaultManagedDomain
	oldDNSManagedDomainExplicit := dnsDefaultManagedDomainExplicit
	oldServiceAccountOverride := defaultServiceAccountOverride
	oldServiceAccountExplicit := defaultServiceAccountExplicit
	oldProjectsWithGC := append([]string(nil), projectsWithGC...)
	oldProvider, hadProvider := vm.Providers[ProviderName]
	t.Cleanup(func() {
		defaultVMProject = oldVMProject
		defaultInfraProject = oldInfraProject
		defaultMetadataProject = oldMetadataProject
		defaultMetadataProjectExplicit = oldMetadataProjectExplicit
		defaultDNSProject = oldDNSProject
		defaultArtifactsBucket = oldArtifactsBucket
		defaultArtifactsBucketExplicit = oldArtifactsBucketExplicit
		defaultDNSProjectExplicit = oldDNSProjectExplicit
		dnsDefaultZone = oldDNSZone
		dnsDefaultDomain = oldDNSDomain
		dnsDefaultDomainExplicit = oldDNSDomainExplicit
		dnsDefaultManagedZone = oldDNSManagedZone
		dnsDefaultManagedDomain = oldDNSManagedDomain
		dnsDefaultManagedDomainExplicit = oldDNSManagedDomainExplicit
		defaultServiceAccountOverride = oldServiceAccountOverride
		defaultServiceAccountExplicit = oldServiceAccountExplicit
		projectsWithGC = oldProjectsWithGC
		if hadProvider {
			vm.Providers[ProviderName] = oldProvider
		} else {
			delete(vm.Providers, ProviderName)
		}
	})

	t.Run("environment configures independent project roles", func(t *testing.T) {
		delete(vm.Providers, ProviderName)
		t.Setenv("ROACHPROD_GCE_DEFAULT_PROJECT", "legacy-project")
		t.Setenv("ROACHPROD_GCE_PROJECT", "environment-vm-project")
		t.Setenv("ROACHPROD_GCE_INFRA_PROJECT", "environment-infra-project")
		t.Setenv("ROACHPROD_GCE_METADATA_PROJECT", "environment-metadata-project")
		t.Setenv("ROACHPROD_GCE_ARTIFACTS_BUCKET", "environment-artifacts-bucket")
		t.Setenv("ROACHPROD_GCE_DNS_PROJECT", "environment-dns-project")
		t.Setenv("ROACHPROD_GCE_DEFAULT_SERVICE_ACCOUNT", "environment-service-account")
		require.NoError(t, initGCEProjectDefaults())

		provider, err := NewProvider()
		require.NoError(t, err)
		require.Equal(t, []string{"environment-vm-project"}, provider.Projects)
		require.Equal(t, "environment-vm-project", VMProject())
		require.Equal(t, "environment-infra-project", InfraProject())
		require.Equal(t, "environment-infra-project", DefaultProject())
		require.Equal(t, "environment-metadata-project", MetadataProject())
		require.Equal(t, "environment-artifacts-bucket", provider.ArtifactsBucket())
		require.Equal(t, "environment-dns-project", provider.dnsProviderOpts.DNSProject)
		require.Equal(t, "environment-service-account", DefaultServiceAccount())
	})

	t.Run("legacy environment configures all project roles", func(t *testing.T) {
		delete(vm.Providers, ProviderName)
		unsetEnv(t, "ROACHPROD_GCE_PROJECT")
		unsetEnv(t, "ROACHPROD_GCE_INFRA_PROJECT")
		unsetEnv(t, "ROACHPROD_GCE_METADATA_PROJECT")
		unsetEnv(t, "ROACHPROD_GCE_ARTIFACTS_BUCKET")
		unsetEnv(t, "ROACHPROD_GCE_DNS_PROJECT")
		unsetEnv(t, "ROACHPROD_GCE_DEFAULT_SERVICE_ACCOUNT")
		t.Setenv("ROACHPROD_GCE_DEFAULT_PROJECT", "legacy-project")
		require.NoError(t, initGCEProjectDefaults())

		provider, err := NewProvider()
		require.NoError(t, err)
		require.Equal(t, []string{"legacy-project"}, provider.Projects)
		require.Equal(t, "legacy-project", VMProject())
		require.Equal(t, "legacy-project", InfraProject())
		require.Equal(t, "legacy-project", MetadataProject())
		require.Equal(t, "cockroach-test-artifacts-legacy-project", provider.ArtifactsBucket())
		require.Equal(t, "legacy-project", provider.dnsProviderOpts.DNSProject)
		require.Equal(t, "roachprod-vm@legacy-project.iam.gserviceaccount.com", DefaultServiceAccount())
	})

	t.Run("initialized provider keeps project roles distinct", func(t *testing.T) {
		initTestGCEProjectDefaults(t)
		provider, err := NewProvider(
			WithProject("vm-project"),
			WithInfraProject("infra-project"),
			WithMetadataProject("metadata-project"),
		)
		require.NoError(t, err)
		vm.Providers[ProviderName] = provider

		require.Equal(t, []string{"vm-project"}, provider.Projects)
		require.Equal(t, "vm-project", VMProject())
		require.Equal(t, "infra-project", InfraProject())
		require.Equal(t, "cockroach-test-artifacts-infra-project", vm.ArtifactsBucket())
		require.Equal(t, "metadata-project", MetadataProject())
	})

	t.Run("infra flag updates metadata default", func(t *testing.T) {
		initTestGCEProjectDefaults(t)
		provider, err := NewProvider(
			WithProject("vm-project"),
			WithInfraProject("infra-project"),
		)
		require.NoError(t, err)
		vm.Providers[ProviderName] = provider
		createOpts := DefaultProviderOpts()
		flags := pflag.NewFlagSet("metadata-project", pflag.ContinueOnError)
		flags.SetOutput(io.Discard)
		provider.ConfigureProviderFlags(flags, vm.SingleProject)
		require.NoError(t, flags.Parse([]string{
			"--gce-project=flag-vm-project",
			"--gce-artifacts-bucket=flag-artifacts-bucket",
			"--gce-infra-project=flag-infra-project",
		}))

		require.Equal(t, []string{"flag-vm-project"}, provider.Projects)
		require.Equal(t, "flag-vm-project", VMProject())
		require.Equal(t, "flag-infra-project", InfraProject())
		require.Equal(t, "flag-infra-project", MetadataProject())
		require.Equal(t, "flag-artifacts-bucket", vm.ArtifactsBucket())
		require.Equal(t, "flag-infra-project", provider.dnsProviderOpts.DNSProject)
		require.Equal(
			t,
			"roachprod-vm@flag-infra-project.iam.gserviceaccount.com",
			provider.defaultServiceAccountFor(createOpts),
		)
	})

	t.Run("metadata flag overrides infra default", func(t *testing.T) {
		initTestGCEProjectDefaults(t)
		provider, err := NewProvider(WithInfraProject("infra-project"))
		require.NoError(t, err)
		vm.Providers[ProviderName] = provider
		flags := pflag.NewFlagSet("metadata-project", pflag.ContinueOnError)
		flags.SetOutput(io.Discard)
		provider.ConfigureProviderFlags(flags, vm.SingleProject)
		require.NoError(t, flags.Parse([]string{
			"--gce-metadata-project=flag-metadata-project",
			"--gce-infra-project=flag-infra-project",
		}))

		require.Equal(t, "flag-infra-project", InfraProject())
		require.Equal(t, "flag-metadata-project", MetadataProject())
	})

	t.Run("deprecated default project flag aliases infra project", func(t *testing.T) {
		initTestGCEProjectDefaults(t)
		provider, err := NewProvider(WithInfraProject("infra-project"))
		require.NoError(t, err)
		vm.Providers[ProviderName] = provider
		flags := pflag.NewFlagSet("default-project", pflag.ContinueOnError)
		flags.SetOutput(io.Discard)
		provider.ConfigureProviderFlags(flags, vm.SingleProject)
		require.NotEmpty(t, flags.Lookup("gce-default-project").Deprecated)
		require.NoError(t, flags.Parse([]string{
			"--gce-default-project=legacy-flag-project",
		}))

		require.Equal(t, "legacy-flag-project", InfraProject())
		require.Equal(t, "legacy-flag-project", MetadataProject())
		require.Equal(t, "cockroach-test-artifacts-legacy-flag-project", vm.ArtifactsBucket())
	})

	t.Run("DNS and service account overrides remain independent", func(t *testing.T) {
		initTestGCEProjectDefaults(t)
		provider, err := NewProvider(WithInfraProject("infra-project"))
		require.NoError(t, err)
		vm.Providers[ProviderName] = provider
		createOpts := DefaultProviderOpts()
		flags := pflag.NewFlagSet("infrastructure-overrides", pflag.ContinueOnError)
		flags.SetOutput(io.Discard)
		createOpts.ConfigureCreateFlags(flags)
		provider.ConfigureProviderFlags(flags, vm.SingleProject)
		require.NoError(t, flags.Parse([]string{
			"--gce-dns-project=flag-dns-project",
			"--gce-dns-domain=flag.example",
			"--gce-managed-dns-domain=managed.flag.example",
			"--gce-default-service-account=flag-service-account",
			"--gce-infra-project=flag-infra-project",
		}))

		require.Equal(t, "flag-dns-project", provider.dnsProviderOpts.DNSProject)
		require.Equal(t, "flag.example", provider.dnsProviderOpts.PublicDomain)
		require.Equal(t, "managed.flag.example", provider.dnsProviderOpts.ManagedDomain)
		require.Equal(t, "flag-service-account", provider.defaultServiceAccountFor(createOpts))
		dns, ok := provider.dnsProvider.(*dnsProvider)
		require.True(t, ok)
		require.Equal(t, "flag-dns-project", dns.dnsProject)
		require.Equal(t, "flag.example", dns.publicDomain)
		require.Equal(t, "managed.flag.example", dns.managedDomain)
	})

	t.Run("staging infra flag selects staging DNS domains", func(t *testing.T) {
		initTestGCEProjectDefaults(t)
		provider, err := NewProvider()
		require.NoError(t, err)
		vm.Providers[ProviderName] = provider
		flags := pflag.NewFlagSet("staging-infrastructure", pflag.ContinueOnError)
		flags.SetOutput(io.Discard)
		provider.ConfigureProviderFlags(flags, vm.SingleProject)
		require.NoError(t, flags.Parse([]string{
			"--gce-infra-project=" + StagingProjectID,
		}))

		require.Equal(t, StagingProjectID, InfraProject())
		require.Equal(t, StagingProjectID, MetadataProject())
		require.Equal(t, StagingProjectID, provider.dnsProviderOpts.DNSProject)
		require.Equal(t, "roachprod.staging.crdb.dev", provider.dnsProviderOpts.PublicDomain)
		require.Equal(t, "roachprod-managed.staging.crdb.dev", provider.dnsProviderOpts.ManagedDomain)
	})
}

func initTestGCEProjectDefaults(t *testing.T) {
	t.Helper()
	unsetEnv(t, "ROACHPROD_GCE_PROJECT")
	unsetEnv(t, "ROACHPROD_GCE_INFRA_PROJECT")
	unsetEnv(t, "ROACHPROD_GCE_METADATA_PROJECT")
	unsetEnv(t, "ROACHPROD_GCE_ARTIFACTS_BUCKET")
	unsetEnv(t, "ROACHPROD_GCE_DNS_PROJECT")
	unsetEnv(t, "ROACHPROD_GCE_DEFAULT_SERVICE_ACCOUNT")
	unsetEnv(t, "ROACHPROD_GCE_DNS_ZONE")
	unsetEnv(t, "ROACHPROD_GCE_DNS_DOMAIN")
	unsetEnv(t, "ROACHPROD_DNS")
	unsetEnv(t, "ROACHPROD_GCE_DNS_MANAGED_ZONE")
	unsetEnv(t, "ROACHPROD_GCE_DNS_MANAGED_DOMAIN")
	t.Setenv("ROACHPROD_GCE_DEFAULT_PROJECT", DefaultProjectID)
	require.NoError(t, initGCEProjectDefaults())
	initDNSDefault()
}

func unsetEnv(t *testing.T, key string) {
	t.Helper()
	value, set := os.LookupEnv(key)
	require.NoError(t, os.Unsetenv(key))
	t.Cleanup(func() {
		if set {
			require.NoError(t, os.Setenv(key, value))
		} else {
			require.NoError(t, os.Unsetenv(key))
		}
	})
}
