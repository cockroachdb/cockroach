// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"testing"
	"time"

	"cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestSdkInstanceToVM(t *testing.T) {
	// Note: This test relies on the default config values:
	// config.UseSharedUser = true and config.SharedUser = "ubuntu"

	testCases := []struct {
		name       string
		instance   *computepb.Instance
		project    string
		dnsDomain  string
		validate   func(*testing.T, *vm.VM)
		expectErrs int // Number of expected errors in vm.Errors
	}{
		{
			name: "fully configured instance",
			instance: &computepb.Instance{
				Name:              proto.String("test-vm-1"),
				MachineType:       proto.String("projects/test-project/zones/us-east1-b/machineTypes/n2-standard-4"),
				CpuPlatform:       proto.String("Intel Cascade Lake"),
				Zone:              proto.String("projects/test-project/zones/us-east1-b"),
				SelfLink:          proto.String("https://www.googleapis.com/compute/v1/projects/test-project/zones/us-east1-b/instances/test-vm-1"),
				CreationTimestamp: proto.String("2024-01-15T10:00:00.000-08:00"),
				Labels: map[string]string{
					vm.TagLifetime: "12h",
					"cluster":      "test-cluster",
				},
				NetworkInterfaces: []*computepb.NetworkInterface{
					{
						NetworkIP: proto.String("10.0.0.5"),
						Network:   proto.String("projects/test-project/global/networks/default"),
						AccessConfigs: []*computepb.AccessConfig{
							{
								Name:  proto.String("External NAT"),
								NatIP: proto.String("34.123.45.67"),
							},
						},
					},
				},
				Scheduling: &computepb.Scheduling{
					Preemptible:       proto.Bool(false),
					OnHostMaintenance: proto.String("MIGRATE"),
				},
				Disks: []*computepb.AttachedDisk{
					{
						Boot:       proto.Bool(true),
						Source:     proto.String("projects/test-project/zones/us-east1-b/disks/test-vm-1-boot"),
						DiskSizeGb: proto.Int64(50),
					},
				},
			},
			project:   "test-project",
			dnsDomain: "example.com",
			validate: func(t *testing.T, result *vm.VM) {
				assert.Equal(t, "test-vm-1", result.Name)
				assert.Equal(t, "test-project", result.Project)
				assert.Equal(t, "us-east1-b", result.Zone)
				assert.Equal(t, "n2-standard-4", result.MachineType)
				assert.Equal(t, "10.0.0.5", result.PrivateIP)
				assert.Equal(t, "34.123.45.67", result.PublicIP)
				assert.Equal(t, "test-vm-1.example.com", result.PublicDNS)
				assert.Equal(t, "example.com", result.PublicDNSZone)
				assert.Equal(t, "test-vm-1.us-east1-b.test-project", result.DNS)
				assert.Equal(t, "ubuntu", result.RemoteUser)
				assert.Equal(t, ProviderName, result.Provider)
				assert.Equal(t, ProviderName, result.DNSProvider)
				assert.Equal(t, "test-vm-1", result.ProviderID)
				assert.Equal(t, "test-project", result.ProviderAccountID)
				assert.Equal(t, "default", result.VPC)
				assert.Equal(t, 12*time.Hour, result.Lifetime)
				assert.False(t, result.Preemptible)
				assert.Equal(t, vm.ArchAMD64, result.CPUArch)
				assert.Equal(t, "cascade lake", result.CPUFamily)
				assert.Equal(t, "test-cluster", result.Labels["cluster"])
				assert.NotZero(t, result.CreatedAt)
			},
			expectErrs: 0,
		},
		{
			name: "preemptible instance",
			instance: &computepb.Instance{
				Name:              proto.String("preempt-vm"),
				MachineType:       proto.String("projects/test/zones/us-central1-a/machineTypes/n2-standard-2"),
				CpuPlatform:       proto.String("Intel Skylake"),
				Zone:              proto.String("projects/test/zones/us-central1-a"),
				SelfLink:          proto.String("https://www.googleapis.com/compute/v1/projects/test/zones/us-central1-a/instances/preempt-vm"),
				CreationTimestamp: proto.String("2024-01-15T10:00:00.000-08:00"),
				Labels: map[string]string{
					vm.TagLifetime: "6h",
				},
				NetworkInterfaces: []*computepb.NetworkInterface{
					{
						NetworkIP: proto.String("10.0.0.10"),
						Network:   proto.String("projects/test/global/networks/default"),
						AccessConfigs: []*computepb.AccessConfig{
							{
								Name:  proto.String("External NAT"),
								NatIP: proto.String("34.100.200.50"),
							},
						},
					},
				},
				Scheduling: &computepb.Scheduling{
					Preemptible:       proto.Bool(true),
					OnHostMaintenance: proto.String("TERMINATE"),
				},
				Disks: []*computepb.AttachedDisk{
					{
						Boot:       proto.Bool(true),
						Source:     proto.String("projects/test/zones/us-central1-a/disks/preempt-vm-boot"),
						DiskSizeGb: proto.Int64(30),
					},
				},
			},
			project:   "test",
			dnsDomain: "gcp.example.com",
			validate: func(t *testing.T, result *vm.VM) {
				assert.Equal(t, "preempt-vm", result.Name)
				assert.True(t, result.Preemptible)
				assert.Equal(t, 6*time.Hour, result.Lifetime)
			},
			expectErrs: 0,
		},
		{
			name: "instance with persistent disks",
			instance: &computepb.Instance{
				Name:              proto.String("vm-with-disks"),
				MachineType:       proto.String("projects/test/zones/us-west1-b/machineTypes/n2-standard-8"),
				CpuPlatform:       proto.String("Intel Cascade Lake"),
				Zone:              proto.String("projects/test/zones/us-west1-b"),
				SelfLink:          proto.String("https://www.googleapis.com/compute/v1/projects/test/zones/us-west1-b/instances/vm-with-disks"),
				CreationTimestamp: proto.String("2024-01-15T10:00:00.000-08:00"),
				Labels: map[string]string{
					vm.TagLifetime: "24h",
				},
				NetworkInterfaces: []*computepb.NetworkInterface{
					{
						NetworkIP: proto.String("10.1.0.5"),
						Network:   proto.String("projects/test/global/networks/default"),
						AccessConfigs: []*computepb.AccessConfig{
							{
								Name:  proto.String("External NAT"),
								NatIP: proto.String("35.100.50.100"),
							},
						},
					},
				},
				Scheduling: &computepb.Scheduling{
					OnHostMaintenance: proto.String("MIGRATE"),
				},
				Disks: []*computepb.AttachedDisk{
					{
						Boot:       proto.Bool(true),
						Source:     proto.String("projects/test/zones/us-west1-b/disks/vm-boot"),
						DiskSizeGb: proto.Int64(50),
					},
					{
						Boot:       proto.Bool(false),
						Source:     proto.String("projects/test/zones/us-west1-b/disks/data-disk-1"),
						DiskSizeGb: proto.Int64(500),
					},
					{
						Boot:       proto.Bool(false),
						Source:     proto.String("projects/test/zones/us-west1-b/disks/data-disk-2"),
						DiskSizeGb: proto.Int64(1000),
					},
				},
			},
			project:   "test",
			dnsDomain: "gcp.example.com",
			validate: func(t *testing.T, result *vm.VM) {
				assert.Equal(t, "vm-with-disks", result.Name)
				assert.NotNil(t, result.BootVolume)
				assert.Equal(t, "vm-boot", result.BootVolume.Name)
				assert.Equal(t, 50, result.BootVolume.Size)
				assert.Len(t, result.NonBootAttachedVolumes, 2)
				assert.Equal(t, "data-disk-1", result.NonBootAttachedVolumes[0].Name)
				assert.Equal(t, 500, result.NonBootAttachedVolumes[0].Size)
				assert.Equal(t, "data-disk-2", result.NonBootAttachedVolumes[1].Name)
				assert.Equal(t, 1000, result.NonBootAttachedVolumes[1].Size)
			},
			expectErrs: 0,
		},
		{
			name: "instance with local SSDs",
			instance: &computepb.Instance{
				Name:              proto.String("vm-with-ssd"),
				MachineType:       proto.String("projects/test/zones/us-east1-b/machineTypes/n2-standard-4"),
				CpuPlatform:       proto.String("Intel Cascade Lake"),
				Zone:              proto.String("projects/test/zones/us-east1-b"),
				SelfLink:          proto.String("https://www.googleapis.com/compute/v1/projects/test/zones/us-east1-b/instances/vm-with-ssd"),
				CreationTimestamp: proto.String("2024-01-15T10:00:00.000-08:00"),
				Labels: map[string]string{
					vm.TagLifetime: "12h",
				},
				NetworkInterfaces: []*computepb.NetworkInterface{
					{
						NetworkIP: proto.String("10.0.0.20"),
						Network:   proto.String("projects/test/global/networks/default"),
						AccessConfigs: []*computepb.AccessConfig{
							{
								Name:  proto.String("External NAT"),
								NatIP: proto.String("34.150.100.200"),
							},
						},
					},
				},
				Scheduling: &computepb.Scheduling{
					OnHostMaintenance: proto.String("TERMINATE"),
				},
				Disks: []*computepb.AttachedDisk{
					{
						Boot:       proto.Bool(true),
						Source:     proto.String("projects/test/zones/us-east1-b/disks/vm-boot"),
						DiskSizeGb: proto.Int64(50),
					},
					{
						Type:       proto.String("SCRATCH"),
						Source:     proto.String(""), // Local SSDs have no source
						DiskSizeGb: proto.Int64(375),
					},
					{
						Type:       proto.String("SCRATCH"),
						Source:     proto.String(""),
						DiskSizeGb: proto.Int64(375),
					},
				},
			},
			project:   "test",
			dnsDomain: "gcp.example.com",
			validate: func(t *testing.T, result *vm.VM) {
				assert.Equal(t, "vm-with-ssd", result.Name)
				assert.Len(t, result.LocalDisks, 2)
				assert.Equal(t, 375, result.LocalDisks[0].Size)
				assert.Equal(t, "local-ssd", result.LocalDisks[0].ProviderVolumeType)
				assert.Equal(t, 375, result.LocalDisks[1].Size)
			},
			expectErrs: 0,
		},
		{
			name: "instance with missing lifetime label",
			instance: &computepb.Instance{
				Name:              proto.String("no-lifetime-vm"),
				MachineType:       proto.String("projects/test/zones/us-east1-b/machineTypes/n2-standard-2"),
				CpuPlatform:       proto.String("Intel Skylake"),
				Zone:              proto.String("projects/test/zones/us-east1-b"),
				SelfLink:          proto.String("https://www.googleapis.com/compute/v1/projects/test/zones/us-east1-b/instances/no-lifetime-vm"),
				CreationTimestamp: proto.String("2024-01-15T10:00:00.000-08:00"),
				Labels:            map[string]string{},
				NetworkInterfaces: []*computepb.NetworkInterface{
					{
						NetworkIP: proto.String("10.0.0.5"),
						Network:   proto.String("projects/test/global/networks/default"),
						AccessConfigs: []*computepb.AccessConfig{
							{
								Name:  proto.String("External NAT"),
								NatIP: proto.String("34.123.45.67"),
							},
						},
					},
				},
				Scheduling: &computepb.Scheduling{
					OnHostMaintenance: proto.String("MIGRATE"),
				},
				Disks: []*computepb.AttachedDisk{
					{
						Boot:       proto.Bool(true),
						Source:     proto.String("projects/test/zones/us-east1-b/disks/boot"),
						DiskSizeGb: proto.Int64(50),
					},
				},
			},
			project:   "test",
			dnsDomain: "gcp.example.com",
			validate: func(t *testing.T, result *vm.VM) {
				assert.Equal(t, "no-lifetime-vm", result.Name)
				assert.Equal(t, time.Duration(0), result.Lifetime)
			},
			expectErrs: 1, // Should have ErrNoExpiration error
		},
		{
			name: "instance with invalid lifetime label",
			instance: &computepb.Instance{
				Name:              proto.String("bad-lifetime-vm"),
				MachineType:       proto.String("projects/test/zones/us-east1-b/machineTypes/n2-standard-2"),
				CpuPlatform:       proto.String("Intel Skylake"),
				Zone:              proto.String("projects/test/zones/us-east1-b"),
				SelfLink:          proto.String("https://www.googleapis.com/compute/v1/projects/test/zones/us-east1-b/instances/bad-lifetime-vm"),
				CreationTimestamp: proto.String("2024-01-15T10:00:00.000-08:00"),
				Labels: map[string]string{
					vm.TagLifetime: "invalid-duration",
				},
				NetworkInterfaces: []*computepb.NetworkInterface{
					{
						NetworkIP: proto.String("10.0.0.5"),
						Network:   proto.String("projects/test/global/networks/default"),
						AccessConfigs: []*computepb.AccessConfig{
							{
								Name:  proto.String("External NAT"),
								NatIP: proto.String("34.123.45.67"),
							},
						},
					},
				},
				Scheduling: &computepb.Scheduling{
					OnHostMaintenance: proto.String("MIGRATE"),
				},
				Disks: []*computepb.AttachedDisk{
					{
						Boot:       proto.Bool(true),
						Source:     proto.String("projects/test/zones/us-east1-b/disks/boot"),
						DiskSizeGb: proto.Int64(50),
					},
				},
			},
			project:   "test",
			dnsDomain: "gcp.example.com",
			validate: func(t *testing.T, result *vm.VM) {
				assert.Equal(t, "bad-lifetime-vm", result.Name)
			},
			expectErrs: 1, // Should have ErrNoExpiration error due to parse failure
		},
		{
			name: "instance with missing network interfaces",
			instance: &computepb.Instance{
				Name:              proto.String("no-network-vm"),
				MachineType:       proto.String("projects/test/zones/us-east1-b/machineTypes/n2-standard-2"),
				CpuPlatform:       proto.String("Intel Skylake"),
				Zone:              proto.String("projects/test/zones/us-east1-b"),
				SelfLink:          proto.String("https://www.googleapis.com/compute/v1/projects/test/zones/us-east1-b/instances/no-network-vm"),
				CreationTimestamp: proto.String("2024-01-15T10:00:00.000-08:00"),
				Labels: map[string]string{
					vm.TagLifetime: "12h",
				},
				NetworkInterfaces: []*computepb.NetworkInterface{},
				Scheduling: &computepb.Scheduling{
					OnHostMaintenance: proto.String("MIGRATE"),
				},
				Disks: []*computepb.AttachedDisk{
					{
						Boot:       proto.Bool(true),
						Source:     proto.String("projects/test/zones/us-east1-b/disks/boot"),
						DiskSizeGb: proto.Int64(50),
					},
				},
			},
			project:   "test",
			dnsDomain: "gcp.example.com",
			validate: func(t *testing.T, result *vm.VM) {
				assert.Equal(t, "no-network-vm", result.Name)
				assert.Empty(t, result.PrivateIP)
				assert.Empty(t, result.PublicIP)
			},
			expectErrs: 1, // Should have ErrBadNetwork error
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sdk := &sdkInstance{tc.instance}
			result := sdk.toVM(tc.project, tc.dnsDomain)
			require.NotNil(t, result)
			tc.validate(t, result)
			assert.Len(t, result.Errors, tc.expectErrs, "Expected %d errors, got %d: %v", tc.expectErrs, len(result.Errors), result.Errors)
		})
	}
}

func TestSdkAttachedDiskToVolume(t *testing.T) {
	testCases := []struct {
		name           string
		attachedDisk   *computepb.AttachedDisk
		expectedVolume *vm.Volume
		expectedType   VolumeType
		expectError    bool
	}{
		{
			name: "boot disk",
			attachedDisk: &computepb.AttachedDisk{
				Boot:       proto.Bool(true),
				Source:     proto.String("projects/test-project/zones/us-east1-b/disks/vm-boot-disk"),
				DiskSizeGb: proto.Int64(50),
			},
			expectedVolume: &vm.Volume{
				Name:               "vm-boot-disk",
				Zone:               "us-east1-b",
				Size:               50,
				ProviderResourceID: "vm-boot-disk",
				ProviderVolumeType: "pd-ssd",
			},
			expectedType: VolumeTypeBoot,
			expectError:  false,
		},
		{
			name: "persistent data disk",
			attachedDisk: &computepb.AttachedDisk{
				Boot:       proto.Bool(false),
				Source:     proto.String("projects/prod-project/zones/us-central1-a/disks/data-disk-1"),
				DiskSizeGb: proto.Int64(500),
			},
			expectedVolume: &vm.Volume{
				Name:               "data-disk-1",
				Zone:               "us-central1-a",
				Size:               500,
				ProviderResourceID: "data-disk-1",
				ProviderVolumeType: "pd-ssd",
			},
			expectedType: VolumeTypePersistent,
			expectError:  false,
		},
		{
			name: "local SSD (scratch disk)",
			attachedDisk: &computepb.AttachedDisk{
				Type:       proto.String("SCRATCH"),
				Source:     proto.String(""),
				DiskSizeGb: proto.Int64(375),
			},
			expectedVolume: &vm.Volume{
				Size:               375,
				ProviderVolumeType: "local-ssd",
			},
			expectedType: VolumeTypeLocalSSD,
			expectError:  false,
		},
		{
			name: "disk with zone in different region",
			attachedDisk: &computepb.AttachedDisk{
				Boot:       proto.Bool(false),
				Source:     proto.String("projects/test/zones/europe-west1-b/disks/euro-disk"),
				DiskSizeGb: proto.Int64(1000),
			},
			expectedVolume: &vm.Volume{
				Name:               "euro-disk",
				Zone:               "europe-west1-b",
				Size:               1000,
				ProviderResourceID: "euro-disk",
				ProviderVolumeType: "pd-ssd",
			},
			expectedType: VolumeTypePersistent,
			expectError:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sdk := &sdkAttachedDisk{tc.attachedDisk}
			volume, volType, err := sdk.toVolume()

			if tc.expectError {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotNil(t, volume)
				assert.Equal(t, tc.expectedType, volType)
				assert.Equal(t, tc.expectedVolume.Name, volume.Name)
				assert.Equal(t, tc.expectedVolume.Zone, volume.Zone)
				assert.Equal(t, tc.expectedVolume.Size, volume.Size)
				assert.Equal(t, tc.expectedVolume.ProviderResourceID, volume.ProviderResourceID)
				assert.Equal(t, tc.expectedVolume.ProviderVolumeType, volume.ProviderVolumeType)
			}
		})
	}
}

// ============================================================
// HELPER FUNCTIONS TESTS
// ============================================================

func TestParseLifetimeFromLabels(t *testing.T) {
	testCases := []struct {
		name          string
		labels        map[string]string
		expectedDur   time.Duration
		expectedError bool
	}{
		{
			name: "valid lifetime",
			labels: map[string]string{
				vm.TagLifetime: "24h",
			},
			expectedDur: 24 * time.Hour,
		},
		{
			name: "valid lifetime with minutes",
			labels: map[string]string{
				vm.TagLifetime: "30m",
			},
			expectedDur: 30 * time.Minute,
		},
		{
			name: "valid lifetime complex",
			labels: map[string]string{
				vm.TagLifetime: "2h30m",
			},
			expectedDur: 2*time.Hour + 30*time.Minute,
		},
		{
			name:          "missing lifetime label",
			labels:        map[string]string{},
			expectedError: true,
		},
		{
			name: "invalid lifetime format",
			labels: map[string]string{
				vm.TagLifetime: "invalid",
			},
			expectedError: true,
		},
		{
			name: "empty lifetime value",
			labels: map[string]string{
				vm.TagLifetime: "",
			},
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := parseLifetimeFromLabels(tc.labels)

			if tc.expectedError {
				require.Error(t, err)
				assert.Equal(t, vm.ErrNoExpiration, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedDur, result)
			}
		})
	}
}

func TestParseNetworkInfo(t *testing.T) {
	testCases := []struct {
		name              string
		networkInterfaces []*computepb.NetworkInterface
		expectedPublicIP  string
		expectedPrivateIP string
		expectedVPC       string
		expectedError     bool
	}{
		{
			name: "valid network config",
			networkInterfaces: []*computepb.NetworkInterface{
				{
					NetworkIP: proto.String("10.0.0.5"),
					Network:   proto.String("projects/test-project/global/networks/default"),
					AccessConfigs: []*computepb.AccessConfig{
						{
							Name:  proto.String("External NAT"),
							NatIP: proto.String("34.123.45.67"),
						},
					},
				},
			},
			expectedPublicIP:  "34.123.45.67",
			expectedPrivateIP: "10.0.0.5",
			expectedVPC:       "default",
		},
		{
			name: "custom VPC name",
			networkInterfaces: []*computepb.NetworkInterface{
				{
					NetworkIP: proto.String("192.168.1.10"),
					Network:   proto.String("projects/test-project/global/networks/my-custom-vpc"),
					AccessConfigs: []*computepb.AccessConfig{
						{
							Name:  proto.String("External NAT"),
							NatIP: proto.String("35.200.100.50"),
						},
					},
				},
			},
			expectedPublicIP:  "35.200.100.50",
			expectedPrivateIP: "192.168.1.10",
			expectedVPC:       "my-custom-vpc",
		},
		{
			name:              "no network interfaces",
			networkInterfaces: []*computepb.NetworkInterface{},
			expectedError:     true,
		},
		{
			name: "no access configs",
			networkInterfaces: []*computepb.NetworkInterface{
				{
					NetworkIP:     proto.String("10.0.0.5"),
					Network:       proto.String("projects/test-project/global/networks/default"),
					AccessConfigs: []*computepb.AccessConfig{},
				},
			},
			expectedError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			publicIP, privateIP, vpc, err := parseNetworkInfo(tc.networkInterfaces)

			if tc.expectedError {
				require.Error(t, err)
				assert.Equal(t, vm.ErrBadNetwork, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedPublicIP, publicIP)
				assert.Equal(t, tc.expectedPrivateIP, privateIP)
				assert.Equal(t, tc.expectedVPC, vpc)
			}
		})
	}
}

func TestParseProjectFromSelfLink(t *testing.T) {
	testCases := []struct {
		name     string
		selfLink string
		expected string
	}{
		{
			name:     "standard GCP self-link",
			selfLink: "https://www.googleapis.com/compute/v1/projects/cockroach-workers/zones/us-central1-a/instances/test-vm",
			expected: "cockroach-workers",
		},
		{
			name:     "different project",
			selfLink: "https://www.googleapis.com/compute/v1/projects/my-test-project/zones/europe-west2-a/instances/vm-1",
			expected: "my-test-project",
		},
		{
			name:     "project with hyphens and numbers",
			selfLink: "https://www.googleapis.com/compute/v1/projects/test-proj-123/zones/asia-southeast1-b/instances/node",
			expected: "test-proj-123",
		},
		{
			name:     "no project in self-link",
			selfLink: "https://www.googleapis.com/compute/v1/zones/us-east1-b/instances/test-vm",
			expected: "",
		},
		{
			name:     "empty self-link",
			selfLink: "",
			expected: "",
		},
		{
			name:     "project at end of self-link",
			selfLink: "https://www.googleapis.com/compute/v1/projects/final-project",
			expected: "final-project",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := parseProjectFromSelfLink(tc.selfLink)
			assert.Equal(t, tc.expected, result)
		})
	}
}
