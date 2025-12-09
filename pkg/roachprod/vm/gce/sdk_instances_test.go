// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================
// URL GENERATION HELPERS TESTS
// ============================================================

func TestRegionFromZone(t *testing.T) {
	testCases := []struct {
		name     string
		zone     string
		expected string
	}{
		{
			name:     "standard zone format",
			zone:     "us-east1-b",
			expected: "us-east1",
		},
		{
			name:     "another standard zone",
			zone:     "europe-west2-a",
			expected: "europe-west2",
		},
		{
			name:     "asia zone",
			zone:     "asia-southeast1-c",
			expected: "asia-southeast1",
		},
		{
			name:     "no dashes (invalid format but should handle)",
			zone:     "invalid",
			expected: "invalid",
		},
		{
			name:     "empty string",
			zone:     "",
			expected: "",
		},
		{
			name:     "single dash",
			zone:     "us-east1",
			expected: "us",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := regionFromZone(tc.zone)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMachineTypeURL(t *testing.T) {
	testCases := []struct {
		name        string
		zone        string
		machineType string
		expected    string
	}{
		{
			name:        "n2-standard instance",
			zone:        "us-east1-b",
			machineType: "n2-standard-4",
			expected:    "zones/us-east1-b/machineTypes/n2-standard-4",
		},
		{
			name:        "n2d-standard instance",
			zone:        "us-west1-a",
			machineType: "n2d-standard-8",
			expected:    "zones/us-west1-a/machineTypes/n2d-standard-8",
		},
		{
			name:        "t2a ARM instance",
			zone:        "europe-west2-b",
			machineType: "t2a-standard-16",
			expected:    "zones/europe-west2-b/machineTypes/t2a-standard-16",
		},
		{
			name:        "custom machine type",
			zone:        "us-central1-c",
			machineType: "custom-8-32768",
			expected:    "zones/us-central1-c/machineTypes/custom-8-32768",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := machineTypeURL(tc.zone, tc.machineType)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestDiskTypeURL(t *testing.T) {
	testCases := []struct {
		name     string
		zone     string
		diskType string
		expected string
	}{
		{
			name:     "pd-ssd disk",
			zone:     "us-east1-b",
			diskType: "pd-ssd",
			expected: "zones/us-east1-b/diskTypes/pd-ssd",
		},
		{
			name:     "pd-standard disk",
			zone:     "europe-west1-c",
			diskType: "pd-standard",
			expected: "zones/europe-west1-c/diskTypes/pd-standard",
		},
		{
			name:     "pd-balanced disk",
			zone:     "asia-east1-a",
			diskType: "pd-balanced",
			expected: "zones/asia-east1-a/diskTypes/pd-balanced",
		},
		{
			name:     "local-ssd",
			zone:     "us-west1-b",
			diskType: "local-ssd",
			expected: "zones/us-west1-b/diskTypes/local-ssd",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := diskTypeURL(tc.zone, tc.diskType)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestImageURL(t *testing.T) {
	testCases := []struct {
		name     string
		project  string
		image    string
		expected string
	}{
		{
			name:     "ubuntu image",
			project:  "ubuntu-os-cloud",
			image:    "ubuntu-2204-jammy-v20240319",
			expected: "projects/ubuntu-os-cloud/global/images/ubuntu-2204-jammy-v20240319",
		},
		{
			name:     "debian image",
			project:  "debian-cloud",
			image:    "debian-11-bullseye-v20240312",
			expected: "projects/debian-cloud/global/images/debian-11-bullseye-v20240312",
		},
		{
			name:     "FIPS image",
			project:  FIPSImageProject,
			image:    FIPSImage,
			expected: "projects/" + FIPSImageProject + "/global/images/" + FIPSImage,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := imageURL(tc.project, tc.image)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// ============================================================
// BUILDER FUNCTIONS TESTS
// ============================================================

func TestSelectImage(t *testing.T) {
	testCases := []struct {
		name            string
		opts            vm.CreateOpts
		providerOpts    *ProviderOpts
		expectedImage   string
		expectedProject string
		expectedError   string
	}{
		{
			name: "default ubuntu image",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				Image:       "ubuntu-2204-jammy-v20240319",
				MachineType: "n2-standard-4",
			},
			expectedImage:   "ubuntu-2204-jammy-v20240319",
			expectedProject: defaultImageProject,
		},
		{
			name: "FIPS architecture",
			opts: vm.CreateOpts{
				Arch: string(vm.ArchFIPS),
			},
			providerOpts: &ProviderOpts{
				Image:       "ubuntu-2204-jammy-v20240319",
				MachineType: "n2-standard-4",
			},
			expectedImage:   FIPSImage,
			expectedProject: FIPSImageProject,
		},
		{
			name: "ARM64 architecture with t2a machine",
			opts: vm.CreateOpts{
				Arch: string(vm.ArchARM64),
			},
			providerOpts: &ProviderOpts{
				Image:       "ubuntu-2204-jammy-v20240319",
				MachineType: "t2a-standard-4",
			},
			expectedImage:   ARM64Image,
			expectedProject: defaultImageProject,
		},
		{
			name: "ARM64 requested but non-ARM machine",
			opts: vm.CreateOpts{
				Arch: string(vm.ArchARM64),
			},
			providerOpts: &ProviderOpts{
				Image:       "ubuntu-2204-jammy-v20240319",
				MachineType: "n2-standard-4",
			},
			expectedError: "requested arch is arm64, but machine type is n2-standard-4",
		},
		{
			name: "non-ARM arch requested with ARM machine",
			opts: vm.CreateOpts{
				Arch: string(vm.ArchAMD64),
			},
			providerOpts: &ProviderOpts{
				Image:       "ubuntu-2204-jammy-v20240319",
				MachineType: "t2a-standard-4",
			},
			expectedError: "machine type t2a-standard-4 is arm64, but requested arch is amd64",
		},
		{
			name: "local SSD with ARM machine",
			opts: func() vm.CreateOpts {
				opts := vm.CreateOpts{}
				opts.SSDOpts.UseLocalSSD = true
				return opts
			}(),
			providerOpts: &ProviderOpts{
				Image:       "ubuntu-2204-jammy-v20240319",
				MachineType: "t2a-standard-4",
			},
			expectedError: "local SSDs are not supported with t2a-standard-4 instance types",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			image, project, err := selectImage(tc.opts, tc.providerOpts)

			if tc.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expectedImage, image)
				assert.Equal(t, tc.expectedProject, project)
			}
		})
	}
}

func TestBuildMinCPUPlatform(t *testing.T) {
	testCases := []struct {
		name         string
		providerOpts *ProviderOpts
		expected     *string
	}{
		{
			name: "Intel CPU platform",
			providerOpts: &ProviderOpts{
				MachineType:    "n2-standard-4",
				MinCPUPlatform: "Intel Cascade Lake",
			},
			expected: strPtr("Intel Cascade Lake"),
		},
		{
			name: "n2d machine with Intel platform - should convert to AMD",
			providerOpts: &ProviderOpts{
				MachineType:    "n2d-standard-8",
				MinCPUPlatform: "Intel Cascade Lake",
			},
			expected: strPtr("AMD Milan"),
		},
		{
			name: "n2d machine with AMD platform",
			providerOpts: &ProviderOpts{
				MachineType:    "n2d-standard-8",
				MinCPUPlatform: "AMD Milan",
			},
			expected: strPtr("AMD Milan"),
		},
		{
			name: "ARM machine - should return nil",
			providerOpts: &ProviderOpts{
				MachineType:    "t2a-standard-4",
				MinCPUPlatform: "Intel Cascade Lake",
			},
			expected: nil,
		},
		{
			name: "no CPU platform specified",
			providerOpts: &ProviderOpts{
				MachineType:    "n2-standard-4",
				MinCPUPlatform: "",
			},
			expected: nil,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildMinCPUPlatform(newTestLogger(t), tc.providerOpts)
			if tc.expected == nil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				assert.Equal(t, *tc.expected, *result)
			}
		})
	}
}

func TestBuildAdvancedFeatures(t *testing.T) {
	testCases := []struct {
		name              string
		providerOpts      *ProviderOpts
		expectNil         bool
		expectedTurboMode string
		expectedThreadsPC int32
	}{
		{
			name: "both turbo mode and threads per core",
			providerOpts: &ProviderOpts{
				TurboMode:      "ENABLED",
				ThreadsPerCore: 2,
			},
			expectNil:         false,
			expectedTurboMode: "ENABLED",
			expectedThreadsPC: 2,
		},
		{
			name: "only turbo mode",
			providerOpts: &ProviderOpts{
				TurboMode:      "DISABLED",
				ThreadsPerCore: 0,
			},
			expectNil:         false,
			expectedTurboMode: "DISABLED",
		},
		{
			name: "only threads per core",
			providerOpts: &ProviderOpts{
				TurboMode:      "",
				ThreadsPerCore: 1,
			},
			expectNil:         false,
			expectedThreadsPC: 1,
		},
		{
			name: "neither configured - should return nil",
			providerOpts: &ProviderOpts{
				TurboMode:      "",
				ThreadsPerCore: 0,
			},
			expectNil: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildAdvancedFeatures(tc.providerOpts)

			if tc.expectNil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				if tc.expectedTurboMode != "" {
					require.NotNil(t, result.TurboMode)
					assert.Equal(t, tc.expectedTurboMode, *result.TurboMode)
				}
				if tc.expectedThreadsPC > 0 {
					require.NotNil(t, result.ThreadsPerCore)
					assert.Equal(t, tc.expectedThreadsPC, *result.ThreadsPerCore)
				}
			}
		})
	}
}

func TestBuildServiceAccounts(t *testing.T) {
	testCases := []struct {
		name                   string
		project                string
		providerOpts           *ProviderOpts
		defaultProject         string
		expectNil              bool
		expectedServiceAccount string
	}{
		{
			name:    "explicit service account",
			project: "test-project",
			providerOpts: &ProviderOpts{
				ServiceAccount: "test@test-project.iam.gserviceaccount.com",
			},
			defaultProject:         "default-project",
			expectNil:              false,
			expectedServiceAccount: "test@test-project.iam.gserviceaccount.com",
		},
		{
			name:    "use default service account",
			project: "default-project",
			providerOpts: &ProviderOpts{
				ServiceAccount:        "",
				defaultServiceAccount: "default@default-project.iam.gserviceaccount.com",
			},
			defaultProject:         "default-project",
			expectNil:              false,
			expectedServiceAccount: "default@default-project.iam.gserviceaccount.com",
		},
		{
			name:    "no service account",
			project: "test-project",
			providerOpts: &ProviderOpts{
				ServiceAccount:        "",
				defaultServiceAccount: "",
			},
			defaultProject: "default-project",
			expectNil:      true,
		},
		{
			name:    "non-default project without service account",
			project: "other-project",
			providerOpts: &ProviderOpts{
				ServiceAccount:        "",
				defaultServiceAccount: "default@default-project.iam.gserviceaccount.com",
			},
			defaultProject: "default-project",
			expectNil:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildServiceAccounts(tc.project, tc.providerOpts, tc.defaultProject)

			if tc.expectNil {
				assert.Nil(t, result)
			} else {
				require.NotNil(t, result)
				require.Len(t, result, 1)
				assert.Equal(t, tc.expectedServiceAccount, *result[0].Email)
				assert.Equal(t, []string{"https://www.googleapis.com/auth/cloud-platform"}, result[0].Scopes)
			}
		})
	}
}

func TestBuildNetworkInterfaces(t *testing.T) {
	testCases := []struct {
		name            string
		project         string
		zone            string
		expectedNetwork string
		expectedSubnet  string
	}{
		{
			name:            "us-east1-b zone",
			project:         "test-project",
			zone:            "us-east1-b",
			expectedNetwork: "projects/test-project/global/networks/default",
			expectedSubnet:  "projects/test-project/regions/us-east1/subnetworks/default",
		},
		{
			name:            "europe-west2-a zone",
			project:         "prod-project",
			zone:            "europe-west2-a",
			expectedNetwork: "projects/prod-project/global/networks/default",
			expectedSubnet:  "projects/prod-project/regions/europe-west2/subnetworks/default",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildNetworkInterfaces(tc.project, tc.zone)

			require.Len(t, result, 1)
			assert.Equal(t, tc.expectedNetwork, *result[0].Network)
			assert.Equal(t, tc.expectedSubnet, *result[0].Subnetwork)
			require.Len(t, result[0].AccessConfigs, 1)
			assert.Equal(t, "External NAT", *result[0].AccessConfigs[0].Name)
		})
	}
}

func TestBuildScheduling(t *testing.T) {
	testCases := []struct {
		name                      string
		opts                      vm.CreateOpts
		providerOpts              *ProviderOpts
		expectedError             string
		expectedPreemptible       bool
		expectedProvisioningModel string
		expectedOnHostMaintenance string
		expectedAutomaticRestart  bool
	}{
		{
			name: "regular instance with migrate",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				preemptible:          false,
				UseSpot:              false,
				TerminateOnMigration: false,
			},
			expectedOnHostMaintenance: "MIGRATE",
			expectedAutomaticRestart:  true,
		},
		{
			name: "regular instance with terminate",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				preemptible:          false,
				UseSpot:              false,
				TerminateOnMigration: true,
			},
			expectedOnHostMaintenance: "TERMINATE",
			expectedAutomaticRestart:  true,
		},
		{
			name: "preemptible instance",
			opts: vm.CreateOpts{
				Lifetime: 12 * time.Hour,
			},
			providerOpts: &ProviderOpts{
				preemptible:          true,
				TerminateOnMigration: true,
			},
			expectedPreemptible:       true,
			expectedOnHostMaintenance: "TERMINATE",
			expectedAutomaticRestart:  false,
		},
		{
			name: "preemptible instance with lifetime > 24h",
			opts: vm.CreateOpts{
				Lifetime: 25 * time.Hour,
			},
			providerOpts: &ProviderOpts{
				preemptible:          true,
				TerminateOnMigration: true,
			},
			expectedError: "lifetime cannot be longer than 24 hours",
		},
		{
			name: "preemptible without terminate on migration",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				preemptible:          true,
				TerminateOnMigration: false,
			},
			expectedError: "preemptible instances require 'TERMINATE' maintenance policy",
		},
		{
			name: "spot instance",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				UseSpot: true,
			},
			expectedProvisioningModel: "SPOT",
			expectedOnHostMaintenance: "TERMINATE",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := buildScheduling(tc.opts, tc.providerOpts)

			if tc.expectedError != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.expectedError)
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				if tc.expectedPreemptible {
					require.NotNil(t, result.Preemptible)
					assert.True(t, *result.Preemptible)
				}

				if tc.expectedProvisioningModel != "" {
					require.NotNil(t, result.ProvisioningModel)
					assert.Equal(t, tc.expectedProvisioningModel, *result.ProvisioningModel)
				}

				if tc.expectedOnHostMaintenance != "" {
					require.NotNil(t, result.OnHostMaintenance)
					assert.Equal(t, tc.expectedOnHostMaintenance, *result.OnHostMaintenance)
				}

				if result.AutomaticRestart != nil {
					assert.Equal(t, tc.expectedAutomaticRestart, *result.AutomaticRestart)
				}
			}
		})
	}
}

func TestBuildBootDisk(t *testing.T) {
	testCases := []struct {
		name         string
		zone         string
		opts         vm.CreateOpts
		providerOpts *ProviderOpts
		labels       map[string]string
		imageProject string
		image        string
	}{
		{
			name: "basic boot disk",
			zone: "us-east1-b",
			opts: vm.CreateOpts{
				OsVolumeSize: 100,
			},
			providerOpts: &ProviderOpts{
				BootDiskType: "pd-ssd",
			},
			labels:       map[string]string{"env": "test"},
			imageProject: "ubuntu-os-cloud",
			image:        "ubuntu-2204-jammy-v20240319",
		},
		{
			name: "boot disk with pd-standard",
			zone: "europe-west2-a",
			opts: vm.CreateOpts{
				OsVolumeSize: 50,
			},
			providerOpts: &ProviderOpts{
				BootDiskType: "pd-standard",
			},
			labels:       map[string]string{},
			imageProject: "debian-cloud",
			image:        "debian-11-bullseye-v20240312",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildBootDisk(tc.zone, tc.opts, tc.providerOpts, tc.labels, tc.imageProject, tc.image)

			require.NotNil(t, result)
			assert.True(t, *result.Boot)
			assert.True(t, *result.AutoDelete)
			assert.Equal(t, "persistent-disk-0", *result.DeviceName)

			require.NotNil(t, result.InitializeParams)
			assert.Equal(t, int64(tc.opts.OsVolumeSize), *result.InitializeParams.DiskSizeGb)
			assert.Contains(t, *result.InitializeParams.DiskType, tc.providerOpts.BootDiskType)
			assert.Contains(t, *result.InitializeParams.SourceImage, tc.image)
			assert.Equal(t, tc.labels, result.InitializeParams.Labels)
		})
	}
}

func TestBuildAdditionalDisks(t *testing.T) {
	testCases := []struct {
		name          string
		zone          string
		opts          vm.CreateOpts
		providerOpts  *ProviderOpts
		labels        map[string]string
		expectedCount int
		expectedType  string
		expectError   bool
	}{
		{
			name: "boot disk only",
			zone: "us-east1-b",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				BootDiskOnly: true,
			},
			labels:        map[string]string{},
			expectedCount: 0,
		},
		{
			name: "local SSDs",
			zone: "us-east1-b",
			opts: func() vm.CreateOpts {
				opts := vm.CreateOpts{}
				opts.SSDOpts.UseLocalSSD = true
				return opts
			}(),
			providerOpts: &ProviderOpts{
				MachineType: "n2-standard-4",
				SSDCount:    2,
			},
			labels:        map[string]string{},
			expectedCount: 2,
			expectedType:  "SCRATCH",
		},
		{
			name: "persistent disks",
			zone: "us-west1-a",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				PDVolumeCount: 3,
				PDVolumeSize:  500,
				PDVolumeType:  "pd-ssd",
			},
			labels:        map[string]string{"env": "prod"},
			expectedCount: 3,
			expectedType:  "persistent",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := buildAdditionalDisks(newTestLogger(t), tc.zone, tc.opts, tc.providerOpts, tc.labels)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, result, tc.expectedCount)

				if tc.expectedCount > 0 {
					if tc.expectedType == "SCRATCH" {
						for _, disk := range result {
							assert.Equal(t, "SCRATCH", *disk.Type)
							assert.True(t, *disk.AutoDelete)
							assert.Equal(t, "NVME", *disk.Interface)
						}
					} else if tc.expectedType == "persistent" {
						for i, disk := range result {
							assert.True(t, *disk.AutoDelete)
							assert.False(t, *disk.Boot)
							assert.Equal(t, fmt.Sprintf("persistent-disk-%d", i+1), *disk.DeviceName)
							assert.Equal(t, int64(tc.providerOpts.PDVolumeSize), *disk.InitializeParams.DiskSizeGb)
							assert.Contains(t, *disk.InitializeParams.DiskType, tc.providerOpts.PDVolumeType)
						}
					}
				}
			}
		})
	}
}

func TestBuildDisks(t *testing.T) {
	testCases := []struct {
		name          string
		zone          string
		opts          vm.CreateOpts
		providerOpts  *ProviderOpts
		labels        map[string]string
		imageProject  string
		image         string
		expectedCount int
		expectError   bool
	}{
		{
			name: "boot disk only",
			zone: "us-east1-b",
			opts: vm.CreateOpts{
				OsVolumeSize: 100,
			},
			providerOpts: &ProviderOpts{
				BootDiskType: "pd-ssd",
				BootDiskOnly: true,
			},
			labels:        map[string]string{"env": "test"},
			imageProject:  "ubuntu-os-cloud",
			image:         "ubuntu-2204-jammy-v20240319",
			expectedCount: 1, // Just boot disk
		},
		{
			name: "boot disk with local SSDs",
			zone: "us-west1-a",
			opts: func() vm.CreateOpts {
				opts := vm.CreateOpts{
					OsVolumeSize: 100,
				}
				opts.SSDOpts.UseLocalSSD = true
				return opts
			}(),
			providerOpts: &ProviderOpts{
				BootDiskType: "pd-ssd",
				MachineType:  "n2-standard-4",
				SSDCount:     2,
			},
			labels:        map[string]string{},
			imageProject:  "ubuntu-os-cloud",
			image:         "ubuntu-2204-jammy-v20240319",
			expectedCount: 3, // 1 boot + 2 local SSDs
		},
		{
			name: "boot disk with persistent disks",
			zone: "europe-west2-a",
			opts: vm.CreateOpts{
				OsVolumeSize: 50,
			},
			providerOpts: &ProviderOpts{
				BootDiskType:  "pd-standard",
				PDVolumeCount: 3,
				PDVolumeSize:  500,
				PDVolumeType:  "pd-ssd",
			},
			labels:        map[string]string{"env": "prod"},
			imageProject:  "debian-cloud",
			image:         "debian-11-bullseye-v20240312",
			expectedCount: 4, // 1 boot + 3 persistent
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := buildDisks(newTestLogger(t), tc.zone, tc.opts, tc.providerOpts, tc.labels, tc.imageProject, tc.image)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Len(t, result, tc.expectedCount)

				// Verify first disk is boot disk
				if len(result) > 0 {
					assert.True(t, *result[0].Boot)
					assert.Equal(t, "persistent-disk-0", *result[0].DeviceName)
				}
			}
		})
	}
}

func TestBuildProjectZoneMap(t *testing.T) {
	testCases := []struct {
		name        string
		vms         vm.List
		expected    map[string]map[string][]string
		expectError bool
	}{
		{
			name: "single project, single zone",
			vms: vm.List{
				{Name: "vm1", Project: "proj1", Zone: "us-east1-b", Provider: ProviderName},
				{Name: "vm2", Project: "proj1", Zone: "us-east1-b", Provider: ProviderName},
			},
			expected: map[string]map[string][]string{
				"proj1": {
					"us-east1-b": []string{"vm1", "vm2"},
				},
			},
		},
		{
			name: "single project, multiple zones",
			vms: vm.List{
				{Name: "vm1", Project: "proj1", Zone: "us-east1-b", Provider: ProviderName},
				{Name: "vm2", Project: "proj1", Zone: "us-west1-a", Provider: ProviderName},
				{Name: "vm3", Project: "proj1", Zone: "us-east1-b", Provider: ProviderName},
			},
			expected: map[string]map[string][]string{
				"proj1": {
					"us-east1-b": []string{"vm1", "vm3"},
					"us-west1-a": []string{"vm2"},
				},
			},
		},
		{
			name: "multiple projects, multiple zones",
			vms: vm.List{
				{Name: "vm1", Project: "proj1", Zone: "us-east1-b", Provider: ProviderName},
				{Name: "vm2", Project: "proj2", Zone: "europe-west2-a", Provider: ProviderName},
				{Name: "vm3", Project: "proj1", Zone: "us-west1-a", Provider: ProviderName},
			},
			expected: map[string]map[string][]string{
				"proj1": {
					"us-east1-b": []string{"vm1"},
					"us-west1-a": []string{"vm3"},
				},
				"proj2": {
					"europe-west2-a": []string{"vm2"},
				},
			},
		},
		{
			name: "wrong provider",
			vms: vm.List{
				{Name: "vm1", Project: "proj1", Zone: "us-east1-b", Provider: "aws"},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := buildProjectZoneMap(tc.vms)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "received VM instance from")
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.expected, result)
			}
		})
	}
}

func TestComputeZones(t *testing.T) {
	testCases := []struct {
		name          string
		opts          vm.CreateOpts
		providerOpts  *ProviderOpts
		expected      []string
		checkNotEmpty bool // Just verify zones are returned, don't check specific values
		expectError   bool
	}{
		{
			name: "use specified zones",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				Zones: []string{"us-east1-b", "us-west1-a"},
			},
			expected: []string{"us-east1-b", "us-west1-a"},
		},
		{
			name: "use default zones when not specified",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				Zones: []string{},
			},
			checkNotEmpty: true, // DefaultZones() returns dynamic values
		},
		{
			name: "ARM machine with default zones",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				Zones:       []string{},
				MachineType: "t2a-standard-4",
			},
			expected: []string{"us-central1-a"},
		},
		{
			name: "ARM machine with unsupported zone",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				Zones:       []string{"us-east1-b"}, // Not a T2A zone
				MachineType: "t2a-standard-4",
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := computeZones(tc.opts, tc.providerOpts)

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tc.checkNotEmpty {
					assert.NotEmpty(t, result, "expected non-empty zones list")
				} else {
					assert.Equal(t, tc.expected, result)
				}
			}
		})
	}
}

func TestComputeLabelsMap(t *testing.T) {
	testCases := []struct {
		name           string
		opts           vm.CreateOpts
		providerOpts   *ProviderOpts
		expectedLabels map[string]string // Keys that must exist
		expectError    bool
	}{
		{
			name: "basic labels",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				Managed: false,
				UseSpot: false,
			},
			expectedLabels: map[string]string{
				vm.TagCluster:   "", // Will be set by vm.GetDefaultLabelMap
				vm.TagRoachprod: "true",
				vm.TagCreated:   "", // Timestamp, just check it exists
			},
		},
		{
			name: "managed instance",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				Managed: true,
				UseSpot: false,
			},
			expectedLabels: map[string]string{
				ManagedLabel: "true",
			},
		},
		{
			name: "spot instance",
			opts: vm.CreateOpts{},
			providerOpts: &ProviderOpts{
				Managed: false,
				UseSpot: true,
			},
			expectedLabels: map[string]string{
				vm.TagSpotInstance: "true",
			},
		},
		{
			name: "custom labels",
			opts: vm.CreateOpts{
				CustomLabels: map[string]string{
					"env":  "test",
					"team": "platform",
				},
			},
			providerOpts: &ProviderOpts{},
			expectedLabels: map[string]string{
				"env":  "test",
				"team": "platform",
			},
		},
		{
			name: "duplicate custom label",
			opts: vm.CreateOpts{
				CustomLabels: map[string]string{
					"roachprod": "override", // Conflicts with vm.TagRoachprod
				},
			},
			providerOpts: &ProviderOpts{},
			expectError:  true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result, err := computeLabelsMap(tc.opts, tc.providerOpts)

			if tc.expectError {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "duplicate label")
			} else {
				require.NoError(t, err)
				require.NotNil(t, result)

				// Check that expected labels exist
				for key, expectedValue := range tc.expectedLabels {
					actualValue, exists := result[key]
					require.True(t, exists, "expected label %s to exist", key)
					if expectedValue != "" {
						assert.Equal(t, expectedValue, actualValue, "label %s value mismatch", key)
					}
				}

				// Check that created timestamp exists and is properly formatted
				created, exists := result[vm.TagCreated]
				require.True(t, exists, "created timestamp should exist")
				assert.Contains(t, created, "_", "created timestamp should have underscores instead of colons")
			}
		})
	}
}

// Helper function for tests
func strPtr(s string) *string {
	return &s
}
