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

func TestValidateArchitecture(t *testing.T) {
	testCases := []struct {
		name        string
		arch        string
		expectError bool
	}{
		{
			name:        "valid ARM64",
			arch:        "ARM64",
			expectError: false,
		},
		{
			name:        "valid X86_64",
			arch:        "X86_64",
			expectError: false,
		},
		{
			name:        "invalid architecture",
			arch:        "MIPS",
			expectError: true,
		},
		{
			name:        "lowercase arm64 is invalid",
			arch:        "arm64",
			expectError: true,
		},
		{
			name:        "empty string is invalid",
			arch:        "",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateArchitecture(tc.arch)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateDiskType(t *testing.T) {
	testCases := []struct {
		name        string
		diskType    string
		expectError bool
	}{
		{
			name:        "valid local-ssd",
			diskType:    "local-ssd",
			expectError: false,
		},
		{
			name:        "valid pd-balanced",
			diskType:    "pd-balanced",
			expectError: false,
		},
		{
			name:        "valid pd-extreme",
			diskType:    "pd-extreme",
			expectError: false,
		},
		{
			name:        "valid pd-ssd",
			diskType:    "pd-ssd",
			expectError: false,
		},
		{
			name:        "valid pd-standard",
			diskType:    "pd-standard",
			expectError: false,
		},
		{
			name:        "invalid disk type",
			diskType:    "nvme-ssd",
			expectError: true,
		},
		{
			name:        "empty string is invalid",
			diskType:    "",
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateDiskType(tc.diskType)
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateVolumeCreateOpts(t *testing.T) {
	testCases := []struct {
		name          string
		opts          vm.VolumeCreateOpts
		expectError   bool
		errorContains string
	}{
		{
			name: "valid options",
			opts: vm.VolumeCreateOpts{
				Size:         100,
				Architecture: "X86_64",
				Type:         "pd-ssd",
			},
			expectError: false,
		},
		{
			name: "IOPS not supported",
			opts: vm.VolumeCreateOpts{
				Size: 100,
				IOPS: 3000,
			},
			expectError:   true,
			errorContains: "IOPS is not supported",
		},
		{
			name: "zero size",
			opts: vm.VolumeCreateOpts{
				Size: 0,
			},
			expectError:   true,
			errorContains: "size 0",
		},
		{
			name: "encryption not supported",
			opts: vm.VolumeCreateOpts{
				Size:      100,
				Encrypted: true,
			},
			expectError:   true,
			errorContains: "encryption is not implemented",
		},
		{
			name: "invalid architecture",
			opts: vm.VolumeCreateOpts{
				Size:         100,
				Architecture: "MIPS",
			},
			expectError:   true,
			errorContains: "architecture",
		},
		{
			name: "invalid disk type",
			opts: vm.VolumeCreateOpts{
				Size: 100,
				Type: "nvme-ultra",
			},
			expectError:   true,
			errorContains: "type",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateVolumeCreateOpts(tc.opts)
			if tc.expectError {
				require.Error(t, err)
				if tc.errorContains != "" {
					assert.Contains(t, err.Error(), tc.errorContains)
				}
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBuildDiskSourceURL(t *testing.T) {
	testCases := []struct {
		name     string
		project  string
		zone     string
		diskName string
		expected string
	}{
		{
			name:     "standard disk URL",
			project:  "test-project",
			zone:     "us-east1-b",
			diskName: "my-disk",
			expected: "projects/test-project/zones/us-east1-b/disks/my-disk",
		},
		{
			name:     "europe zone",
			project:  "prod-project",
			zone:     "europe-west2-a",
			diskName: "prod-volume-1",
			expected: "projects/prod-project/zones/europe-west2-a/disks/prod-volume-1",
		},
		{
			name:     "disk with numbers",
			project:  "my-project-123",
			zone:     "asia-southeast1-c",
			diskName: "data-disk-001",
			expected: "projects/my-project-123/zones/asia-southeast1-c/disks/data-disk-001",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildDiskSourceURL(tc.project, tc.zone, tc.diskName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildDiskTypeURL(t *testing.T) {
	testCases := []struct {
		name     string
		project  string
		zone     string
		diskType string
		expected string
	}{
		{
			name:     "pd-ssd disk type",
			project:  "test-project",
			zone:     "us-east1-b",
			diskType: "pd-ssd",
			expected: "projects/test-project/zones/us-east1-b/diskTypes/pd-ssd",
		},
		{
			name:     "pd-standard disk type",
			project:  "prod-project",
			zone:     "europe-west2-a",
			diskType: "pd-standard",
			expected: "projects/prod-project/zones/europe-west2-a/diskTypes/pd-standard",
		},
		{
			name:     "local-ssd type",
			project:  "dev-project",
			zone:     "us-central1-a",
			diskType: "local-ssd",
			expected: "projects/dev-project/zones/us-central1-a/diskTypes/local-ssd",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildDiskTypeURL(tc.project, tc.zone, tc.diskType)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildDevicePath(t *testing.T) {
	testCases := []struct {
		name     string
		diskName string
		expected string
	}{
		{
			name:     "simple disk name",
			diskName: "my-disk",
			expected: "/dev/disk/by-id/google-my-disk",
		},
		{
			name:     "disk with numbers",
			diskName: "data-disk-001",
			expected: "/dev/disk/by-id/google-data-disk-001",
		},
		{
			name:     "disk with hyphens",
			diskName: "test-cluster-disk-1",
			expected: "/dev/disk/by-id/google-test-cluster-disk-1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildDevicePath(tc.diskName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildSnapshotFilters(t *testing.T) {
	testCases := []struct {
		name     string
		opts     vm.VolumeSnapshotListOpts
		expected []string
	}{
		{
			name: "name prefix only",
			opts: vm.VolumeSnapshotListOpts{
				NamePrefix: "test-snapshot",
			},
			expected: []string{"name:test-snapshot*"},
		},
		{
			name: "created before only",
			opts: vm.VolumeSnapshotListOpts{
				CreatedBefore: time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC),
			},
			expected: []string{"creationTimestamp<'2024-01-15'"},
		},
		{
			name: "labels only",
			opts: vm.VolumeSnapshotListOpts{
				Labels: map[string]string{
					"env":  "prod",
					"team": "platform",
				},
			},
			expected: []string{"labels.env=prod", "labels.team=platform"},
		},
		{
			name: "all filters combined",
			opts: vm.VolumeSnapshotListOpts{
				NamePrefix:    "backup",
				CreatedBefore: time.Date(2023, 12, 31, 0, 0, 0, 0, time.UTC),
				Labels: map[string]string{
					"cluster": "test-cluster",
				},
			},
			expected: []string{
				"name:backup*",
				"creationTimestamp<'2023-12-31'",
				"labels.cluster=test-cluster",
			},
		},
		{
			name:     "no filters",
			opts:     vm.VolumeSnapshotListOpts{},
			expected: []string{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildSnapshotFilters(tc.opts)

			// Sort both slices for comparison since map iteration order is not guaranteed
			assert.ElementsMatch(t, tc.expected, result)
		})
	}
}

func TestBuildDiskResource(t *testing.T) {
	testCases := []struct {
		name     string
		opts     vm.VolumeCreateOpts
		project  string
		validate func(*testing.T, *computepb.Disk)
	}{
		{
			name: "minimal disk",
			opts: vm.VolumeCreateOpts{
				Name: "test-disk",
				Size: 100,
				Zone: "us-east1-b",
			},
			project: "test-project",
			validate: func(t *testing.T, disk *computepb.Disk) {
				assert.Equal(t, "test-disk", disk.GetName())
				assert.Equal(t, int64(100), disk.GetSizeGb())
				assert.Nil(t, disk.Type)
				assert.Nil(t, disk.Architecture)
				assert.Nil(t, disk.SourceSnapshot)
				assert.Nil(t, disk.Labels)
			},
		},
		{
			name: "disk with type",
			opts: vm.VolumeCreateOpts{
				Name: "ssd-disk",
				Size: 200,
				Zone: "us-central1-a",
				Type: "pd-ssd",
			},
			project: "prod-project",
			validate: func(t *testing.T, disk *computepb.Disk) {
				assert.Equal(t, "ssd-disk", disk.GetName())
				assert.Equal(t, int64(200), disk.GetSizeGb())
				assert.Equal(t, "projects/prod-project/zones/us-central1-a/diskTypes/pd-ssd", disk.GetType())
			},
		},
		{
			name: "disk with architecture",
			opts: vm.VolumeCreateOpts{
				Name:         "arm-disk",
				Size:         50,
				Zone:         "europe-west1-b",
				Architecture: "ARM64",
			},
			project: "test-project",
			validate: func(t *testing.T, disk *computepb.Disk) {
				assert.Equal(t, "arm-disk", disk.GetName())
				assert.Equal(t, "ARM64", disk.GetArchitecture())
			},
		},
		{
			name: "disk from snapshot",
			opts: vm.VolumeCreateOpts{
				Name:             "restored-disk",
				Size:             500,
				Zone:             "asia-southeast1-c",
				SourceSnapshotID: "projects/test-project/global/snapshots/snapshot-123",
			},
			project: "test-project",
			validate: func(t *testing.T, disk *computepb.Disk) {
				assert.Equal(t, "restored-disk", disk.GetName())
				assert.Equal(t, "projects/test-project/global/snapshots/snapshot-123", disk.GetSourceSnapshot())
			},
		},
		{
			name: "disk with labels",
			opts: vm.VolumeCreateOpts{
				Name: "labeled-disk",
				Size: 100,
				Zone: "us-west1-a",
				Labels: map[string]string{
					"env":     "prod",
					"team":    "platform",
					"Cluster": "test-cluster", // Should be serialized
				},
			},
			project: "test-project",
			validate: func(t *testing.T, disk *computepb.Disk) {
				assert.Equal(t, "labeled-disk", disk.GetName())
				require.NotNil(t, disk.Labels)
				assert.Equal(t, "prod", disk.Labels["env"])
				assert.Equal(t, "platform", disk.Labels["team"])
				// Check that label serialization is applied
				assert.Contains(t, disk.Labels, "cluster") // Serialized from "Cluster"
			},
		},
		{
			name: "disk with all options",
			opts: vm.VolumeCreateOpts{
				Name:             "full-disk",
				Size:             1000,
				Zone:             "us-east1-b",
				Type:             "pd-extreme",
				Architecture:     "X86_64",
				SourceSnapshotID: "projects/test/snapshots/snap-1",
				Labels: map[string]string{
					"purpose": "testing",
				},
			},
			project: "test-project",
			validate: func(t *testing.T, disk *computepb.Disk) {
				assert.Equal(t, "full-disk", disk.GetName())
				assert.Equal(t, int64(1000), disk.GetSizeGb())
				assert.Equal(t, "projects/test-project/zones/us-east1-b/diskTypes/pd-extreme", disk.GetType())
				assert.Equal(t, "X86_64", disk.GetArchitecture())
				assert.Equal(t, "projects/test/snapshots/snap-1", disk.GetSourceSnapshot())
				assert.Equal(t, "testing", disk.Labels["purpose"])
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			disk := buildDiskResource(tc.opts, tc.project)
			require.NotNil(t, disk)
			tc.validate(t, disk)
		})
	}
}

func TestDiskToVolume(t *testing.T) {
	testCases := []struct {
		name     string
		disk     *computepb.Disk
		expected vm.Volume
	}{
		{
			name: "minimal disk",
			disk: &computepb.Disk{
				Name:   proto.String("test-disk"),
				SizeGb: proto.Int64(100),
				Type:   proto.String("projects/test-project/zones/us-east1-b/diskTypes/pd-ssd"),
				Zone:   proto.String("projects/test-project/zones/us-east1-b"),
			},
			expected: vm.Volume{
				ProviderResourceID: "test-disk",
				ProviderVolumeType: "pd-ssd",
				Zone:               "us-east1-b",
				Encrypted:          false,
				Name:               "test-disk",
				Labels:             nil,
				Size:               100,
			},
		},
		{
			name: "disk with labels",
			disk: &computepb.Disk{
				Name:   proto.String("labeled-disk"),
				SizeGb: proto.Int64(500),
				Type:   proto.String("projects/prod/zones/us-central1-a/diskTypes/pd-balanced"),
				Zone:   proto.String("projects/prod/zones/us-central1-a"),
				Labels: map[string]string{
					"env":     "prod",
					"cluster": "test-cluster",
					"owner":   "platform-team",
				},
			},
			expected: vm.Volume{
				ProviderResourceID: "labeled-disk",
				ProviderVolumeType: "pd-balanced",
				Zone:               "us-central1-a",
				Encrypted:          false,
				Name:               "labeled-disk",
				Labels: map[string]string{
					"env":     "prod",
					"cluster": "test-cluster",
					"owner":   "platform-team",
				},
				Size: 500,
			},
		},
		{
			name: "extreme disk",
			disk: &computepb.Disk{
				Name:   proto.String("extreme-disk"),
				SizeGb: proto.Int64(1000),
				Type:   proto.String("projects/test/zones/europe-west1-b/diskTypes/pd-extreme"),
				Zone:   proto.String("projects/test/zones/europe-west1-b"),
			},
			expected: vm.Volume{
				ProviderResourceID: "extreme-disk",
				ProviderVolumeType: "pd-extreme",
				Zone:               "europe-west1-b",
				Encrypted:          false,
				Name:               "extreme-disk",
				Labels:             nil,
				Size:               1000,
			},
		},
		{
			name: "standard disk",
			disk: &computepb.Disk{
				Name:   proto.String("standard-disk"),
				SizeGb: proto.Int64(200),
				Type:   proto.String("projects/test/zones/asia-southeast1-c/diskTypes/pd-standard"),
				Zone:   proto.String("projects/test/zones/asia-southeast1-c"),
			},
			expected: vm.Volume{
				ProviderResourceID: "standard-disk",
				ProviderVolumeType: "pd-standard",
				Zone:               "asia-southeast1-c",
				Encrypted:          false,
				Name:               "standard-disk",
				Labels:             nil,
				Size:               200,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := diskToVolume(tc.disk)
			assert.Equal(t, tc.expected, result)
		})
	}
}
