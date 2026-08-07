// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"io"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"
	"time"

	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestAllowedLocalSSDCount(t *testing.T) {
	for _, c := range []struct {
		machineType string
		expected    []int
		unsupported bool
	}{
		// N1 has the same ssd counts for all cpu counts.
		{"n1-standard-4", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-highcpu-64", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-highmem-96", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},

		{"n2-standard-4", []int{1, 2, 4, 8, 16, 24}, false},
		{"n2-standard-8", []int{1, 2, 4, 8, 16, 24}, false},
		{"n2-standard-16", []int{2, 4, 8, 16, 24}, false},
		// N.B. n2-standard-30 doesn't exist, but we still get the ssd counts based on cpu count.
		{"n2-standard-30", []int{4, 8, 16, 24}, false},
		{"n2-standard-32", []int{4, 8, 16, 24}, false},
		{"n2-standard-48", []int{8, 16, 24}, false},
		{"n2-standard-64", []int{8, 16, 24}, false},
		{"n2-standard-80", []int{8, 16, 24}, false},
		{"n2-standard-96", []int{16, 24}, false},
		{"n2-standard-128", []int{16, 24}, false},

		{"c2-standard-4", []int{1, 2, 4, 8}, false},
		{"c2-standard-8", []int{1, 2, 4, 8}, false},
		{"c2-standard-16", []int{2, 4, 8}, false},
		{"c2-standard-30", []int{4, 8}, false},
		{"c2-standard-60", []int{8}, false},
		// N.B. n2-standard-64 doesn't exist, but we still get the ssd counts based on cpu count.
		{"c2-standard-64", []int{8}, false},

		{"c4a-standard-4", nil, true},
		{"c4a-standard-4-lssd", []int{1}, false},
		{"c4a-standard-16-lssd", []int{4}, false},
		{"c4a-standard-1-lssd", nil, true},
		{"c4a-highmem-32-lssd", []int{6}, false},
		{"c4a-highcpu-32-lssd", nil, true},
	} {
		t.Run(c.machineType, func(t *testing.T) {
			actual, err := AllowedLocalSSDCount(c.machineType)
			if c.unsupported {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.EqualValues(t, c.expected, actual)
			}
		})
	}
}

func TestDefaultServiceAccount(t *testing.T) {
	assert.Equal(t, "roachprod-vm@test-project.iam.gserviceaccount.com", vmServiceAccount("test-project"))
	assert.Equal(t, DefaultProviderOpts().defaultServiceAccount, DefaultServiceAccount())
	assert.False(t, DefaultProviderOpts().UseIAP)
}

// TestDefaultArtifactsBucket keeps vm's provider-independent fallback in sync
// with the bucket GCE derives from its default infrastructure project. The vm
// package cannot derive this itself because importing gce would create a cycle.
func TestDefaultArtifactsBucket(t *testing.T) {
	require.Equal(t, vm.DefaultArtifactsBucket, artifactsBucketForProject(DefaultProjectID))
}

func TestDNSDefaults(t *testing.T) {
	oldInfraProject := defaultInfraProject
	oldZone := dnsDefaultZone
	oldDomain := dnsDefaultDomain
	oldDomainExplicit := dnsDefaultDomainExplicit
	oldManagedZone := dnsDefaultManagedZone
	oldManagedDomain := dnsDefaultManagedDomain
	oldManagedDomainExplicit := dnsDefaultManagedDomainExplicit
	t.Cleanup(func() {
		defaultInfraProject = oldInfraProject
		dnsDefaultZone = oldZone
		dnsDefaultDomain = oldDomain
		dnsDefaultDomainExplicit = oldDomainExplicit
		dnsDefaultManagedZone = oldManagedZone
		dnsDefaultManagedDomain = oldManagedDomain
		dnsDefaultManagedDomainExplicit = oldManagedDomainExplicit
	})

	unsetEnv(t, "ROACHPROD_GCE_DNS_ZONE")
	unsetEnv(t, "ROACHPROD_GCE_DNS_DOMAIN")
	unsetEnv(t, "ROACHPROD_DNS")
	unsetEnv(t, "ROACHPROD_GCE_DNS_MANAGED_ZONE")
	unsetEnv(t, "ROACHPROD_GCE_DNS_MANAGED_DOMAIN")
	for _, tc := range []struct {
		name          string
		infraProject  string
		publicDomain  string
		managedDomain string
	}{
		{
			name:          "production",
			infraProject:  DefaultProjectID,
			publicDomain:  "roachprod.crdb.dev",
			managedDomain: "roachprod-managed.crdb.dev",
		},
		{
			name:          "staging",
			infraProject:  StagingProjectID,
			publicDomain:  "roachprod.staging.crdb.dev",
			managedDomain: "roachprod-managed.staging.crdb.dev",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defaultInfraProject = tc.infraProject
			initDNSDefault()

			require.Equal(t, "roachprod", dnsDefaultZone)
			require.Equal(t, tc.publicDomain, dnsDefaultDomain)
			require.False(t, dnsDefaultDomainExplicit)
			require.Equal(t, "roachprod-managed", dnsDefaultManagedZone)
			require.Equal(t, tc.managedDomain, dnsDefaultManagedDomain)
			require.False(t, dnsDefaultManagedDomainExplicit)
		})
	}
}

func TestBuildInstancePropertiesLocalSSDDisks(t *testing.T) {
	l, err := (&logger.Config{Stdout: io.Discard, Stderr: io.Discard}).NewLogger("")
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{Projects: []string{"test-project"}, infraProject: "default-project"}

	testCases := []struct {
		name                 string
		machineType          string
		ssdCount             int
		expectedScratchDisks int
		expectedSSDCount     int
	}{
		{
			name:                 "requestable single-count local SSD machine attaches scratch disks",
			machineType:          "c2-standard-60",
			ssdCount:             8,
			expectedScratchDisks: 8,
			expectedSSDCount:     8,
		},
		{
			name:                 "requestable local SSD machine bumps below-min count",
			machineType:          "c2-standard-60",
			ssdCount:             1,
			expectedScratchDisks: 8,
			expectedSSDCount:     8,
		},
		{
			name:                 "requestable local SSD machine passes through above-min invalid count",
			machineType:          "c2-standard-30",
			ssdCount:             5,
			expectedScratchDisks: 5,
			expectedSSDCount:     5,
		},
		{
			name:                 "auto-attached local SSD machine does not request scratch disks",
			machineType:          "c4a-standard-4-lssd",
			ssdCount:             1,
			expectedScratchDisks: 0,
			expectedSSDCount:     1,
		},
		{
			name:                 "auto-attached local SSD machine normalizes mismatched count",
			machineType:          "c4a-standard-4-lssd",
			ssdCount:             2,
			expectedScratchDisks: 0,
			expectedSSDCount:     1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := vm.DefaultCreateOpts()
			opts.SSDOpts.UseLocalSSD = true
			providerOpts := DefaultProviderOpts()
			providerOpts.MachineType = tc.machineType
			providerOpts.SSDCount = tc.ssdCount

			props, err := p.buildInstanceProperties(
				l, opts, providerOpts, "startup-script", "us-east1-b", nil,
			)
			assert.NoError(t, err)
			if err != nil {
				return
			}

			scratchDisks := 0
			for _, disk := range props.GetDisks() {
				if disk.GetType() == computepb.AttachedDisk_SCRATCH.String() {
					scratchDisks++
				}
			}
			assert.Equal(t, tc.expectedScratchDisks, scratchDisks)
			assert.Equal(t, tc.expectedSSDCount, providerOpts.SSDCount)
		})
	}
}

func TestBuildInstancePropertiesAddressModeAndSubnet(t *testing.T) {
	l, err := (&logger.Config{Stdout: io.Discard, Stderr: io.Discard}).NewLogger("")
	require.NoError(t, err)
	p := &Provider{Projects: []string{"test-project"}, infraProject: "default-project"}
	providerOpts := DefaultProviderOpts()
	providerOpts.Subnets = map[string]string{"us-east1": "private-subnet"}

	privateOpts := vm.DefaultCreateOpts()
	privateOpts.AddressMode = vm.AddressModePrivate
	props, err := p.buildInstanceProperties(
		l, privateOpts, providerOpts, "startup-script", "us-east1-b", nil,
	)
	require.NoError(t, err)
	require.Len(t, props.GetNetworkInterfaces(), 1)
	require.Empty(t, props.GetNetworkInterfaces()[0].GetAccessConfigs())
	require.Empty(t, props.GetTags().GetItems())
	require.Equal(t,
		"projects/test-project/regions/us-east1/subnetworks/private-subnet",
		props.GetNetworkInterfaces()[0].GetSubnetwork(),
	)

	providerOpts.UseIAP = true
	props, err = p.buildInstanceProperties(
		l, privateOpts, providerOpts, "startup-script", "us-east1-b", nil,
	)
	require.NoError(t, err)
	require.Equal(t, []string{iapSSHTag}, props.GetTags().GetItems())

	publicOpts := vm.DefaultCreateOpts()
	props, err = p.buildInstanceProperties(
		l, publicOpts, providerOpts, "startup-script", "us-east1-b", nil,
	)
	require.NoError(t, err)
	require.Len(t, props.GetNetworkInterfaces()[0].GetAccessConfigs(), 1)
	require.Empty(t, props.GetTags().GetItems())
}

func TestComputeAddressArgs(t *testing.T) {
	providerOpts := DefaultProviderOpts()
	publicOpts := vm.DefaultCreateOpts()
	require.Empty(t, computeAddressArgs(publicOpts, providerOpts))

	privateOpts := vm.DefaultCreateOpts()
	privateOpts.AddressMode = vm.AddressModePrivate
	require.Equal(t, []string{"--no-address"}, computeAddressArgs(privateOpts, providerOpts))

	providerOpts.UseIAP = true
	require.Equal(t,
		[]string{"--no-address", "--tags", iapSSHTag},
		computeAddressArgs(privateOpts, providerOpts),
	)
	// Public mode keeps the original network arguments even when the IAP flag
	// is supplied; IAP is only meaningful for private instances.
	require.Empty(t, computeAddressArgs(publicOpts, providerOpts))
}

func TestPublicAddressModePreservesSubnetDefaults(t *testing.T) {
	l, err := (&logger.Config{Stdout: io.Discard, Stderr: io.Discard}).NewLogger("")
	require.NoError(t, err)
	p := &Provider{Projects: []string{"test-project"}, infraProject: "default-project"}
	providerOpts := DefaultProviderOpts()
	publicOpts := vm.DefaultCreateOpts()
	require.Equal(t, vm.AddressModePublic, publicOpts.AddressMode)

	props, err := p.buildInstanceProperties(
		l, publicOpts, providerOpts, "startup-script", "us-east1-b", nil,
	)
	require.NoError(t, err)
	require.Len(t, props.GetNetworkInterfaces(), 1)
	networkInterface := props.GetNetworkInterfaces()[0]
	require.Equal(t,
		"projects/test-project/regions/us-east1/subnetworks/test-project-vpc-us-east1",
		networkInterface.GetSubnetwork(),
	)
	require.Empty(t, networkInterface.GetNetwork())
	require.Len(t, networkInterface.GetAccessConfigs(), 1)
	require.Equal(t, "External NAT", networkInterface.GetAccessConfigs()[0].GetName())
	require.Equal(t,
		computepb.AccessConfig_ONE_TO_ONE_NAT.String(),
		networkInterface.GetAccessConfigs()[0].GetType(),
	)
	require.Empty(t, props.GetTags().GetItems())
	require.Empty(t, computeAddressArgs(publicOpts, providerOpts))

	zeroValueProps, err := p.buildInstanceProperties(
		l, vm.CreateOpts{}, providerOpts, "startup-script", "us-east1-b", nil,
	)
	require.NoError(t, err)
	require.Len(t, zeroValueProps.GetNetworkInterfaces()[0].GetAccessConfigs(), 1)
}

func TestResolveAddressMode(t *testing.T) {
	defaultProjectProvider := &Provider{
		Projects:     []string{"default-project"},
		infraProject: "default-project",
	}
	nonDefaultProjectProvider := &Provider{
		Projects:     []string{"other-project"},
		infraProject: "default-project",
	}

	mode, err := defaultProjectProvider.resolveAddressMode(vm.AddressModeAuto)
	require.NoError(t, err)
	require.Equal(t, vm.AddressModePrivate, mode)
	mode, err = nonDefaultProjectProvider.resolveAddressMode(vm.AddressModeAuto)
	require.NoError(t, err)
	require.Equal(t, vm.AddressModePublic, mode)
	mode, err = defaultProjectProvider.resolveAddressMode(vm.AddressModePublic)
	require.NoError(t, err)
	require.Equal(t, vm.AddressModePublic, mode)
	mode, err = defaultProjectProvider.resolveAddressMode("")
	require.NoError(t, err)
	require.Equal(t, vm.AddressModePublic, mode)
}

func TestParseRegionSubnetMap(t *testing.T) {
	m, err := parseRegionSubnetMap("")
	require.NoError(t, err)
	require.Nil(t, m)

	m, err = parseRegionSubnetMap("us-east1=a,us-west1=projects/host/regions/us-west1/subnetworks/b")
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"us-east1": "a",
		"us-west1": "projects/host/regions/us-west1/subnetworks/b",
	}, m)

	_, err = parseRegionSubnetMap("bogus")
	require.Error(t, err)
	_, err = parseRegionSubnetMap("us-east1=")
	require.Error(t, err)
}

func TestDefaultNetworkResources(t *testing.T) {
	require.Equal(t,
		"projects/test-project/global/networks/test-project-vpc",
		DefaultNetworkSelfLink("test-project"),
	)
	require.Equal(t,
		"projects/test-project/regions/us-east1/subnetworks/test-project-vpc-us-east1",
		DefaultSubnetSelfLink("test-project", "us-east1"),
	)
}

func TestResolveSubnet(t *testing.T) {
	const project = "test-project"
	for _, tc := range []struct {
		name        string
		opts        ProviderOpts
		zone        string
		expected    string
		expectedErr string
	}{
		{
			name:     "project convention when no config",
			opts:     ProviderOpts{},
			zone:     "us-east1-b",
			expected: "projects/test-project/regions/us-east1/subnetworks/test-project-vpc-us-east1",
		},
		{
			name:     "per-region map selects the zone's region",
			opts:     ProviderOpts{Subnets: map[string]string{"us-east1": "east-subnet", "us-west1": "west-subnet"}},
			zone:     "us-west1-a",
			expected: "projects/test-project/regions/us-west1/subnetworks/west-subnet",
		},
		{
			name:        "map must cover every selected region",
			opts:        ProviderOpts{Subnets: map[string]string{"us-east1": "east-subnet"}},
			zone:        "us-west1-a",
			expectedErr: `no GCE subnet configured for region "us-west1"`,
		},
		{
			name:     "shared VPC host-project self-link is not rewritten",
			opts:     ProviderOpts{Subnets: map[string]string{"us-east1": "projects/host-project/regions/us-east1/subnetworks/shared"}},
			zone:     "us-east1-b",
			expected: "projects/host-project/regions/us-east1/subnetworks/shared",
		},
		{
			name:     "map overrides the default for its region",
			opts:     ProviderOpts{Subnets: map[string]string{"us-east1": "east-subnet"}},
			zone:     "us-east1-b",
			expected: "projects/test-project/regions/us-east1/subnetworks/east-subnet",
		},
		{
			name:        "empty subnet value errors",
			opts:        ProviderOpts{Subnets: map[string]string{"us-east1": ""}},
			zone:        "us-east1-b",
			expectedErr: `empty GCE subnet configured for region "us-east1"`,
		},
		{
			name:        "self-link region mismatch errors",
			opts:        ProviderOpts{Subnets: map[string]string{"us-east1": "projects/host/regions/us-west1/subnetworks/wrong"}},
			zone:        "us-east1-b",
			expectedErr: `is in region "us-west1" but zone maps to region "us-east1"`,
		},
		{
			name:        "invalid zone errors",
			opts:        ProviderOpts{},
			zone:        "ab",
			expectedErr: "invalid zone",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.opts.resolveSubnet(project, tc.zone)
			if tc.expectedErr != "" {
				require.ErrorContains(t, err, tc.expectedErr)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}
}

func TestValidateSubnetConfigRequiresEverySelectedRegion(t *testing.T) {
	opts := &ProviderOpts{
		Subnets: map[string]string{"us-east1": "custom-east"},
	}
	require.NoError(t, opts.validateSubnetConfig(
		"test-project", []string{"us-east1-b", "us-east1-c"},
	))
	require.ErrorContains(t, opts.validateSubnetConfig(
		"test-project", []string{"us-east1-b", "us-west1-a"},
	), `no GCE subnet configured for region "us-west1"`)

	opts.Subnets["us-west1"] = "custom-west"
	require.NoError(t, opts.validateSubnetConfig(
		"test-project", []string{"us-east1-b", "us-west1-a"},
	))
}

func TestSubnetCreateFlags(t *testing.T) {
	opts := DefaultProviderOpts()
	flags := pflag.NewFlagSet("gce-subnets", pflag.ContinueOnError)
	opts.ConfigureCreateFlags(flags)

	require.Nil(t, flags.Lookup("gce-subnet"))
	require.Nil(t, flags.Lookup("gce-network"))
	require.NotNil(t, flags.Lookup("gce-subnets"))
	require.NoError(t, flags.Parse([]string{
		"--gce-subnets=us-east1=custom-east,us-west1=custom-west",
	}))
	require.Equal(t, map[string]string{
		"us-east1": "custom-east",
		"us-west1": "custom-west",
	}, opts.Subnets)
}

func TestCLISubnetArgs(t *testing.T) {
	const project = "test-project"

	args, err := (&ProviderOpts{}).cliSubnetArgs(project, "us-east1-b")
	require.NoError(t, err)
	require.Equal(t, []string{
		"--subnet", "projects/test-project/regions/us-east1/subnetworks/test-project-vpc-us-east1",
	}, args)

	args, err = (&ProviderOpts{
		Subnets: map[string]string{"us-east1": "s"},
	}).cliSubnetArgs(project, "us-east1-b")
	require.NoError(t, err)
	require.Equal(t, []string{
		"--subnet", "projects/test-project/regions/us-east1/subnetworks/s",
	}, args)

	_, err = (&ProviderOpts{
		Subnets: map[string]string{"us-east1": "s"},
	}).cliSubnetArgs(project, "us-west1-a")
	require.Error(t, err)
}

func TestBuildInstancePropertiesSubnets(t *testing.T) {
	l, err := (&logger.Config{Stdout: io.Discard, Stderr: io.Discard}).NewLogger("")
	require.NoError(t, err)
	p := &Provider{Projects: []string{"test-project"}, infraProject: "default-project"}
	opts := vm.DefaultCreateOpts()

	// A per-region subnet populates only Subnetwork; GCE infers Network.
	po := DefaultProviderOpts()
	po.Subnets = map[string]string{"us-east1": "east-subnet"}
	props, err := p.buildInstanceProperties(l, opts, po, "startup-script", "us-east1-b", nil)
	require.NoError(t, err)
	require.Equal(t, "projects/test-project/regions/us-east1/subnetworks/east-subnet",
		props.GetNetworkInterfaces()[0].GetSubnetwork())
	require.Empty(t, props.GetNetworkInterfaces()[0].GetNetwork())

	// With no overrides, Subnetwork follows the project convention.
	props, err = p.buildInstanceProperties(l, opts, DefaultProviderOpts(), "startup-script", "us-east1-b", nil)
	require.NoError(t, err)
	require.Equal(t, "projects/test-project/regions/us-east1/subnetworks/test-project-vpc-us-east1",
		props.GetNetworkInterfaces()[0].GetSubnetwork())
	require.Empty(t, props.GetNetworkInterfaces()[0].GetNetwork())

	// CRDB and workload node groups with identical config resolve identically.
	newGroup := func() *ProviderOpts {
		o := DefaultProviderOpts()
		o.Subnets = map[string]string{"us-east1": "shared-subnet"}
		return o
	}
	crdbProps, err := p.buildInstanceProperties(l, opts, newGroup(), "startup-script", "us-east1-b", nil)
	require.NoError(t, err)
	workloadProps, err := p.buildInstanceProperties(l, opts, newGroup(), "startup-script", "us-east1-b", nil)
	require.NoError(t, err)
	require.Equal(t,
		crdbProps.GetNetworkInterfaces()[0].GetSubnetwork(),
		workloadProps.GetNetworkInterfaces()[0].GetSubnetwork())
}

func TestVMNetworkParsing(t *testing.T) {
	jsonInstance := jsonVM{
		Name:              "private-json-vm",
		Labels:            map[string]string{vm.TagLifetime: time.Hour.String()},
		CreationTimestamp: time.Now(),
		SelfLink:          "https://www.googleapis.com/compute/v1/projects/test-project/zones/us-east1-b/instances/private-json-vm",
	}
	jsonInstance.NetworkInterfaces = []struct {
		Network       string
		NetworkIP     string
		AccessConfigs []struct {
			Name  string
			NatIP string
		}
	}{
		{
			Network:   "projects/test-project/global/networks/private-vpc",
			NetworkIP: "10.0.0.2",
		},
	}
	jsonInstance.Scheduling.OnHostMaintenance = "MIGRATE"
	jsonInstance.Tags.Items = []string{iapSSHTag}
	parsedJSON := jsonInstance.toVM("test-project", "roachprod.example")
	require.Empty(t, parsedJSON.Errors)
	require.Equal(t, "10.0.0.2", parsedJSON.PrivateIP)
	require.Empty(t, parsedJSON.PublicIP)
	require.Equal(t, "private-vpc", parsedJSON.VPC)
	require.Equal(t, []string{iapSSHTag}, parsedJSON.NetworkTags)
	require.True(t, UsesIAP(*parsedJSON))

	sdkVM := (&sdkInstance{&computepb.Instance{
		Name:              proto.String("private-sdk-vm"),
		Labels:            map[string]string{vm.TagLifetime: time.Hour.String()},
		CreationTimestamp: proto.String(time.Now().Format(time.RFC3339)),
		SelfLink: proto.String(
			"https://www.googleapis.com/compute/v1/projects/test-project/zones/us-east1-b/instances/private-sdk-vm",
		),
		NetworkInterfaces: []*computepb.NetworkInterface{
			{
				Network:   proto.String("projects/test-project/global/networks/private-vpc"),
				NetworkIP: proto.String("10.0.0.3"),
			},
		},
		Scheduling: &computepb.Scheduling{OnHostMaintenance: proto.String("MIGRATE")},
		Tags:       &computepb.Tags{Items: []string{iapSSHTag}},
		Zone:       proto.String("projects/test-project/zones/us-east1-b"),
	}}).toVM("test-project", "roachprod.example")
	require.Empty(t, sdkVM.Errors)
	require.Equal(t, "10.0.0.3", sdkVM.PrivateIP)
	require.Empty(t, sdkVM.PublicIP)
	require.Equal(t, "private-vpc", sdkVM.VPC)
	require.Equal(t, []string{iapSSHTag}, sdkVM.NetworkTags)
	require.True(t, UsesIAP(*sdkVM))

	publicJSONInstance := jsonInstance
	publicJSONInstance.Name = "public-json-vm"
	publicJSONInstance.Tags.Items = nil
	publicJSONInstance.NetworkInterfaces[0].AccessConfigs = []struct {
		Name  string
		NatIP string
	}{
		{Name: "External NAT", NatIP: "192.0.2.1"},
	}
	parsedPublicJSON := publicJSONInstance.toVM("test-project", "roachprod.example")
	require.Empty(t, parsedPublicJSON.Errors)
	require.Equal(t, "10.0.0.2", parsedPublicJSON.PrivateIP)
	require.Equal(t, "192.0.2.1", parsedPublicJSON.PublicIP)
	require.Equal(t, "private-vpc", parsedPublicJSON.VPC)
	require.Empty(t, parsedPublicJSON.NetworkTags)

	parsedPublicSDK := (&sdkInstance{&computepb.Instance{
		Name:              proto.String("public-sdk-vm"),
		Labels:            map[string]string{vm.TagLifetime: time.Hour.String()},
		CreationTimestamp: proto.String(time.Now().Format(time.RFC3339)),
		SelfLink: proto.String(
			"https://www.googleapis.com/compute/v1/projects/test-project/zones/us-east1-b/instances/public-sdk-vm",
		),
		NetworkInterfaces: []*computepb.NetworkInterface{
			{
				Network:   proto.String("projects/test-project/global/networks/private-vpc"),
				NetworkIP: proto.String("10.0.0.4"),
				AccessConfigs: []*computepb.AccessConfig{
					{Name: proto.String("External NAT"), NatIP: proto.String("192.0.2.2")},
				},
			},
		},
		Scheduling: &computepb.Scheduling{OnHostMaintenance: proto.String("MIGRATE")},
		Zone:       proto.String("projects/test-project/zones/us-east1-b"),
	}}).toVM("test-project", "roachprod.example")
	require.Empty(t, parsedPublicSDK.Errors)
	require.Equal(t, "10.0.0.4", parsedPublicSDK.PrivateIP)
	require.Equal(t, "192.0.2.2", parsedPublicSDK.PublicIP)
	require.Equal(t, "private-vpc", parsedPublicSDK.VPC)
	require.Empty(t, parsedPublicSDK.NetworkTags)
}

func TestValidateProvisionedAddressMode(t *testing.T) {
	require.NoError(t, validateProvisionedAddressMode(vm.List{
		{Name: "private", PrivateIP: "10.0.0.2"},
	}, vm.AddressModePrivate))
	require.NoError(t, validateProvisionedAddressMode(vm.List{
		{Name: "public", PrivateIP: "10.0.0.2", PublicIP: "192.0.2.1"},
	}, vm.AddressModePublic))
	require.Error(t, validateProvisionedAddressMode(vm.List{
		{Name: "unexpected-public", PrivateIP: "10.0.0.2", PublicIP: "192.0.2.1"},
	}, vm.AddressModePrivate))
	require.Error(t, validateProvisionedAddressMode(vm.List{
		{Name: "missing-public", PrivateIP: "10.0.0.2"},
	}, vm.AddressModePublic))
}

func TestParseGCECapacityError(t *testing.T) {
	const details = `ERROR: (gcloud.compute.instances.create) Could not fetch resource:
---
code: ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS
errorDetails:
- localizedMessage:
    locale: en-US
    message: A t2a-standard-8 VM instance is currently unavailable in the us-central1-a
      zone. Consider trying your request in the us-central1-f zone(s), which currently
      has capacity to accommodate your request.
- errorInfo:
    domain: compute.googleapis.com
    metadatas:
      vmType: t2a-standard-8
      zone: us-central1-a
      zonesAvailable: us-central1-f
    reason: resource_availability
message: The zone 'projects/cockroach-ephemeral/zones/us-central1-a' does not have
  enough resources available to fulfill the request.`

	capacityErr := parseGCECapacityError(details)
	assert.NotNil(t, capacityErr)
	assert.Equal(t, vm.CreateCapacityClassZone, capacityErr.CapacityClass)
	assert.Equal(t, ProviderName, capacityErr.Provider)
	assert.Equal(t, "t2a-standard-8", capacityErr.MachineType)
	assert.Equal(t, []string{"us-central1-a"}, capacityErr.FailedZones)
	assert.Equal(t, []string{"us-central1-f"}, capacityErr.SuggestedZones)
}

func TestAnnotateGCECapacityErrorAddsZone(t *testing.T) {
	const details = `ERROR: (gcloud.compute.instance-groups.managed.wait-until) The zone does not have enough resources available to fulfill the request.`

	err := maybeGCECapacityError(errors.New("wait-until failed"), []byte(details))
	err = annotateGCECapacityError(err, "us-central1-a")

	var capacityErr *vm.CreateCapacityError
	assert.True(t, errors.As(err, &capacityErr))
	assert.Equal(t, vm.CreateCapacityClassZone, capacityErr.CapacityClass)
	assert.Equal(t, []string{"us-central1-a"}, capacityErr.FailedZones)
}

func TestDefaultC4AZonesExcludesUnsupportedZones(t *testing.T) {
	for _, geo := range []bool{false, true} {
		for i := 0; i < 20; i++ {
			zones := DefaultC4AZones(geo)
			if !IsSupportedC4AZone(zones) {
				t.Errorf("DefaultC4AZones(geo=%v) returned zones unsupported by C4A: %v", geo, zones)
			}
		}
	}
}

func TestC4AZoneValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		zones     []string
		supported bool
	}{
		{name: "empty", supported: true},
		{name: "supported default", zones: []string{"us-east1-b", "us-west1-c", "europe-central2-a"}, supported: true},
		{name: "unsupported west1 b", zones: []string{"us-east1-b", "us-west1-b"}, supported: false},
		{name: "unsupported asia northeast1 a", zones: []string{"asia-northeast1-a"}, supported: false},
		{name: "unsupported europe central2 b", zones: []string{"europe-central2-b"}, supported: false},
		{name: "unsupported europe central2 c", zones: []string{"europe-central2-c"}, supported: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.supported, IsSupportedC4AZone(tc.zones))
		})
	}
}

func TestComputeZonesRejectsUnsupportedC4AExplicitZones(t *testing.T) {
	_, err := computeZones(vm.CreateOpts{GeoDistributed: true}, &ProviderOpts{
		MachineType: "c4a-standard-4-lssd",
		Zones:       []string{"us-east1-b", "us-west1-b"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "C4A instances are not supported")
}

func TestDefaultRetryZoneCandidates(t *testing.T) {
	assert.Equal(
		t,
		[]string{"us-east1-b", "us-east1-c", "us-east1-d"},
		DefaultRetryZoneCandidates("n2-standard-4"),
	)
	assert.Equal(
		t,
		[]string{"us-east1-b", "us-east1-c", "us-east1-d"},
		DefaultRetryZoneCandidates("c4a-standard-4-lssd"),
	)
	assert.Equal(
		t,
		[]string{"us-central1-a", "us-central1-b", "us-central1-f"},
		DefaultRetryZoneCandidates("t2a-standard-4"),
	)
}

func Test_buildFilterPreemptionCliArgs(t *testing.T) {
	type args struct {
		vms         vm.List
		projectName string
		since       time.Time
	}
	tests := []struct {
		name        string
		args        args
		wantCliArgs string
		wantErr     error
	}{
		{
			name: "One VM",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
				},
				projectName: "test-project",
				since:       timeutil.Now().Add(-time.Hour * 3),
			},
			wantCliArgs: "logging read --project=test-project --format=json --freshness=4h resource.type=gce_instance AND " +
				"(protoPayload.methodName=compute.instances.preempted) AND " +
				"(protoPayload.resourceName=projects/test-project/zones/us-west1-a/instances/test-vm)",
			wantErr: nil,
		},
		{name: "Two VMs + different project name + since 7 hrs",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
					{
						Name: "test-vm1",
						Zone: "us-west1-a",
					},
				},
				projectName: "test-project-z",
				since:       timeutil.Now().Add(-time.Hour * 7),
			},
			wantCliArgs: "logging read --project=test-project-z --format=json --freshness=8h resource.type=gce_instance AND " +
				"(protoPayload.methodName=compute.instances.preempted) AND " +
				"(protoPayload.resourceName=projects/test-project-z/zones/us-west1-a/instances/test-vm OR " +
				"protoPayload.resourceName=projects/test-project-z/zones/us-west1-a/instances/test-vm1)",
			wantErr: nil,
		},
		{name: "Two VMs from different zones + since 4 hrs",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
					{
						Name: "test-vm1",
						Zone: "us-east1-a",
					},
				},
				projectName: "test-project",
				since:       timeutil.Now().Add(-time.Hour * 4),
			},
			wantCliArgs: "logging read --project=test-project --format=json --freshness=5h resource.type=gce_instance AND " +
				"(protoPayload.methodName=compute.instances.preempted) AND " +
				"(protoPayload.resourceName=projects/test-project/zones/us-west1-a/instances/test-vm OR " +
				"protoPayload.resourceName=projects/test-project/zones/us-east1-a/instances/test-vm1)",
			wantErr: nil,
		},
		{name: "Nil VMs",
			args: args{
				vms:         nil,
				projectName: "test-project",
				since:       timeutil.Now().Add(-time.Hour * 4),
			},
			wantCliArgs: "",
			wantErr:     errors.New("vms cannot be nil"),
		},
		{name: "Empty Project",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
				},
				projectName: "",
				since:       timeutil.Now().Add(-time.Hour * 4),
			},
			wantCliArgs: "",
			wantErr:     errors.New("project name cannot be empty"),
		},
		{name: "Since in future",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
				},
				projectName: "test",
				since:       timeutil.Now().Add(time.Hour * 1),
			},
			wantCliArgs: "",
			wantErr:     errors.New("since cannot be in the future"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cliArgs, err := buildFilterPreemptionCliArgs(tt.args.vms, tt.args.projectName, tt.args.since)
			if tt.wantErr == nil {
				joinedString := strings.Join(cliArgs, " ")
				assert.Equalf(t, tt.wantCliArgs, joinedString, "buildFilterPreemptionCliArgs(%v, %v, %v)", tt.args.vms, tt.args.projectName, tt.args.since)
				assert.Equalf(t, tt.wantErr, err, "buildFilterPreemptionCliArgs(%v, %v, %v)", tt.args.vms, tt.args.projectName, tt.args.since)
			} else {
				assert.Equalf(t, []string(nil), cliArgs, "buildFilterPreemptionCliArgs(%v, %v, %v)", tt.args.vms, tt.args.projectName, tt.args.since)
				assert.Equalf(t, tt.wantErr.Error(), err.Error(), "buildFilterPreemptionCliArgs(%v, %v, %v)", tt.args.vms, tt.args.projectName, tt.args.since)
			}
		})
	}
}

func randInstanceGroupSizes(r *rand.Rand) []jsonManagedInstanceGroup {
	// We do not test empty sets, hence the +1.
	count := r.Intn(10) + 1
	groups := make([]jsonManagedInstanceGroup, count)
	for i := 0; i < count; i++ {
		groups[i].Size = r.Intn(32)
	}
	return groups
}

func TestComputeGrowDistribution(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	c := quick.Config{MaxCount: 128,
		Rand: rng,
		Values: func(values []reflect.Value, r *rand.Rand) {
			values[0] = reflect.ValueOf(randInstanceGroupSizes(r))
		}}

	testDistribution := func(groups []jsonManagedInstanceGroup) bool {
		// Generate a random number of new nodes to add to the groups.
		newNodeCount := rng.Intn(24) + 1

		// Compute the total number of nodes before the distribution and
		// the maximum distance between the number of nodes in the groups.
		totalNodesBefore := 0
		curMax, curMin := 0.0, math.MaxFloat64
		for _, g := range groups {
			totalNodesBefore += g.Size
			curMax = math.Max(curMax, float64(g.Size))
			curMin = math.Min(curMin, float64(g.Size))
		}
		maxDistanceBefore := curMax - curMin

		// Sort the groups, compute the new distribution and apply it to the
		// group sizes.
		sort.Slice(groups, func(i, j int) bool {
			return groups[i].Size < groups[j].Size
		})
		newTargetSize := computeGrowDistribution(groups, newNodeCount)
		for idx := range newTargetSize {
			groups[idx].Size += newTargetSize[idx]
		}

		// Compute the total number of nodes after the distribution and the maximum
		// distance between the number of nodes in the groups.
		totalNodesAfter := 0
		curMax, curMin = 0.0, math.MaxFloat64
		for _, g := range groups {
			totalNodesAfter += g.Size
			curMax = math.Max(curMax, float64(g.Size))
			curMin = math.Min(curMin, float64(g.Size))
		}
		maxDistanceAfter := curMax - curMin

		// The total number of nodes should be the sum of the new node count and the
		// total number of nodes before the distribution.
		if totalNodesAfter != totalNodesBefore+newNodeCount {
			return false
		}
		// The maximum distance between the number of nodes in the groups should not
		// increase by more than 1, otherwise the new distribution was not fair.
		if maxDistanceAfter > maxDistanceBefore+1.0 {
			return false
		}
		return true
	}
	if err := quick.Check(testDistribution, &c); err != nil {
		t.Error(err)
	}
}
