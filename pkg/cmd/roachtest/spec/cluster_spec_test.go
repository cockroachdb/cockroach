// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/stretchr/testify/require"
)

func TestClustersCompatible(t *testing.T) {
	t.Run("spec does not match", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 4}
		s2 := ClusterSpec{NodeCount: 5}
		require.False(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different lifetime", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5, Lifetime: 100}
		s2 := ClusterSpec{NodeCount: 5, Lifetime: 200}
		require.True(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different GCE spec with cloud as GCE", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5}
		s2 := ClusterSpec{NodeCount: 5}
		s1.VolumeType = "mock_volume1"
		s2.VolumeType = "mock_volume2"
		require.False(t, ClustersCompatible(s1, s2, GCE))
	})
	t.Run("spec has different GCE spec with cloud as AWS", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5}
		s2 := ClusterSpec{NodeCount: 5}
		s1.VolumeType = "mock_volume1"
		s2.VolumeType = "mock_volume2"
		require.False(t, ClustersCompatible(s1, s2, AWS))
	})
	t.Run("spec has different spec with cloud as AWS", func(t *testing.T) {
		s1 := ClusterSpec{NodeCount: 5}
		s2 := ClusterSpec{NodeCount: 5}
		s1.GCE.MinCPUPlatform = "mock_platform1"
		s2.GCE.MinCPUPlatform = "mock_platform2"
		require.True(t, ClustersCompatible(s1, s2, AWS))
	})
}

func TestMayUseLocalSSD(t *testing.T) {
	tests := []struct {
		name                  string
		spec                  ClusterSpec
		defaultPreferLocalSSD bool
		expected              bool
	}{
		{
			name:                  "default spec with PreferLocalSSD=true",
			defaultPreferLocalSSD: true,
			expected:              true,
		},
		{
			name:                  "default spec with PreferLocalSSD=false",
			defaultPreferLocalSSD: false,
			expected:              false,
		},
		{
			name:                  "explicit non-local-ssd volume type",
			spec:                  ClusterSpec{VolumeType: "gp3"},
			defaultPreferLocalSSD: true,
			expected:              false,
		},
		{
			name:                  "explicit local-ssd volume type",
			spec:                  ClusterSpec{VolumeType: "local-ssd"},
			defaultPreferLocalSSD: false,
			expected:              true,
		},
		{
			name:                  "LocalSSD disabled",
			spec:                  ClusterSpec{LocalSSD: LocalSSDDisable},
			defaultPreferLocalSSD: true,
			expected:              false,
		},
		{
			name:                  "LocalSSD preferred",
			spec:                  ClusterSpec{LocalSSD: LocalSSDPreferOn},
			defaultPreferLocalSSD: false,
			expected:              true,
		},
		{
			name:                  "RandomizeVolumeType enabled",
			spec:                  ClusterSpec{RandomizeVolumeType: true},
			defaultPreferLocalSSD: false,
			expected:              true,
		},
		{
			name:                  "VolumeSize set overrides PreferLocalSSD",
			spec:                  ClusterSpec{VolumeSize: 100},
			defaultPreferLocalSSD: true,
			expected:              false,
		},
		{
			name:                  "VolumeSize set overrides LocalSSDPreferOn",
			spec:                  ClusterSpec{VolumeSize: 100, LocalSSD: LocalSSDPreferOn},
			defaultPreferLocalSSD: true,
			expected:              false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tc.spec.mayUseLocalSSD(tc.defaultPreferLocalSSD)
			require.Equal(t, tc.expected, result)
		})
	}
}

func TestClustersRetainClearedInfo(t *testing.T) {
	// Adding a test in case we switch the ClustersCompatible signature to take
	// pointers to ClusterSpec in the future.
	t.Run("original structs are not modified", func(t *testing.T) {
		s1 := ClusterSpec{
			NodeCount:              5,
			ExposedMetamorphicInfo: map[string]string{"VolumeType": "io2"},
		}
		s2 := ClusterSpec{
			NodeCount:              5,
			ExposedMetamorphicInfo: map[string]string{"VolumeType": "gp3"},
		}

		ClustersCompatible(s1, s2, GCE)

		// Original data should still be there
		require.Equal(t, "io2", s1.ExposedMetamorphicInfo["VolumeType"])
		require.Equal(t, "gp3", s2.ExposedMetamorphicInfo["VolumeType"])
	})
}

func TestGCEVolumeTypeForMachineType(t *testing.T) {
	tests := []struct {
		name                  string
		opts                  []Option
		arch                  vm.CPUArch
		benchmark             bool
		defaultPreferLocalSSD bool
		expectedMachineType   string
		expectedVolumeType    string
		expectVolumeType      bool
		expectedUseLocalSSD   bool
		expectedArch          vm.CPUArch
	}{
		{
			name:                "AMD64 gets pd-ssd",
			arch:                vm.ArchAMD64,
			expectedMachineType: "n2-standard-4",
			expectedVolumeType:  "pd-ssd",
			expectVolumeType:    true,
			expectedArch:        vm.ArchAMD64,
		},
		{
			name:                "ARM64 non-benchmark (C4A) gets hyperdisk-balanced",
			arch:                vm.ArchARM64,
			expectedMachineType: "c4a-standard-4",
			expectedVolumeType:  "hyperdisk-balanced",
			expectVolumeType:    true,
			expectedArch:        vm.ArchARM64,
		},
		{
			name:                  "ARM64 non-benchmark (C4A) gets -lssd machine when local SSD is preferred",
			arch:                  vm.ArchARM64,
			defaultPreferLocalSSD: true,
			expectedMachineType:   "c4a-standard-4-lssd",
			expectedUseLocalSSD:   true,
			expectedArch:          vm.ArchARM64,
		},
		{
			name:                  "ARM64 non-benchmark default local SSD preference falls back to hyperdisk when local SSD is unsupported",
			opts:                  []Option{CPU(1)},
			arch:                  vm.ArchARM64,
			defaultPreferLocalSSD: true,
			expectedMachineType:   "c4a-standard-1",
			expectedVolumeType:    "hyperdisk-balanced",
			expectVolumeType:      true,
			expectedArch:          vm.ArchARM64,
		},
		{
			name:                "ARM64 non-benchmark explicit local SSD falls back to AMD64 when local SSD is unsupported",
			opts:                []Option{CPU(32), VolumeType("local-ssd")},
			arch:                vm.ArchARM64,
			expectedMachineType: "n2-custom-32-65536",
			expectedUseLocalSSD: true,
			expectedArch:        vm.ArchAMD64,
		},
		{
			name:                  "ARM64 non-benchmark explicit local SSD preference falls back to hyperdisk when local SSD is unsupported",
			opts:                  []Option{CPU(32), PreferLocalSSD()},
			arch:                  vm.ArchARM64,
			defaultPreferLocalSSD: false,
			expectedMachineType:   "c4a-highcpu-32",
			expectedVolumeType:    "hyperdisk-balanced",
			expectVolumeType:      true,
			expectedArch:          vm.ArchARM64,
		},
		{
			name:                "ARM64 benchmark explicit local SSD preference falls back to AMD64 when local SSD is unsupported",
			opts:                []Option{CPU(32), PreferLocalSSD()},
			arch:                vm.ArchARM64,
			benchmark:           true,
			expectedMachineType: "n2-custom-32-65536",
			expectedUseLocalSSD: true,
			expectedArch:        vm.ArchAMD64,
		},
		{
			name:                  "ARM64 randomized storage on C4A highcpu does not select unavailable local SSD",
			opts:                  []Option{CPU(32), RandomizeVolumeType()},
			arch:                  vm.ArchARM64,
			defaultPreferLocalSSD: true,
			expectedMachineType:   "c4a-highcpu-32",
			expectedVolumeType:    "hyperdisk-balanced",
			expectVolumeType:      true,
			expectedArch:          vm.ArchARM64,
		},
		{
			name:                  "ARM64 benchmark default local SSD uses C4A local SSD",
			arch:                  vm.ArchARM64,
			benchmark:             true,
			defaultPreferLocalSSD: true,
			expectedMachineType:   "c4a-standard-4-lssd",
			expectedUseLocalSSD:   true,
			expectedArch:          vm.ArchARM64,
		},
		{
			name:                  "ARM64 benchmark default local SSD preference falls back to AMD64 when local SSD is unsupported",
			opts:                  []Option{CPU(32)},
			arch:                  vm.ArchARM64,
			benchmark:             true,
			defaultPreferLocalSSD: true,
			expectedMachineType:   "n2-custom-32-65536",
			expectedUseLocalSSD:   true,
			expectedArch:          vm.ArchAMD64,
		},
		{
			name:                "ARM64 benchmark with local SSD disabled preserves pd-ssd on T2A",
			opts:                []Option{DisableLocalSSD()},
			arch:                vm.ArchARM64,
			benchmark:           true,
			expectedMachineType: "t2a-standard-4",
			expectedVolumeType:  "pd-ssd",
			expectVolumeType:    true,
			expectedArch:        vm.ArchARM64,
		},
		{
			name:                "ARM64 benchmark with volume size preserves pd-ssd on T2A",
			opts:                []Option{VolumeSize(500)},
			arch:                vm.ArchARM64,
			benchmark:           true,
			expectedMachineType: "t2a-standard-4",
			expectedVolumeType:  "pd-ssd",
			expectVolumeType:    true,
			expectedArch:        vm.ArchARM64,
		},
		{
			name:                "ARM64 explicit pd-ssd uses T2A",
			opts:                []Option{VolumeType("pd-ssd")},
			arch:                vm.ArchARM64,
			expectedMachineType: "t2a-standard-4",
			expectedVolumeType:  "pd-ssd",
			expectVolumeType:    true,
			expectedArch:        vm.ArchARM64,
		},
		{
			name:                "ARM64 explicit pd-ssd falls back to AMD64 when T2A cannot match Auto memory",
			opts:                []Option{CPU(32), VolumeType("pd-ssd")},
			arch:                vm.ArchARM64,
			expectedMachineType: "n2-custom-32-65536",
			expectedVolumeType:  "pd-ssd",
			expectVolumeType:    true,
			expectedArch:        vm.ArchAMD64,
		},
		{
			name:                  "ARM64 benchmark local SSD falls back to AMD64 when C4A cannot match disk count",
			opts:                  []Option{CPU(16), Disks(16)},
			arch:                  vm.ArchARM64,
			benchmark:             true,
			defaultPreferLocalSSD: true,
			expectedMachineType:   "n2-standard-16",
			expectedUseLocalSSD:   true,
			expectedArch:          vm.ArchAMD64,
		},
		{
			name:                  "ARM64 C4A falls back to AMD64 when explicit zones are unsupported",
			opts:                  []Option{Geo(), GCEZones("us-east1-b,us-west1-b")},
			arch:                  vm.ArchARM64,
			defaultPreferLocalSSD: true,
			expectedMachineType:   "n2-standard-4",
			expectedUseLocalSSD:   true,
			expectedArch:          vm.ArchAMD64,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			s := MakeClusterSpec(4, tc.opts...)
			params := RoachprodClusterConfig{
				Cloud:         GCE,
				PreferredArch: tc.arch,
				Benchmark:     tc.benchmark,
			}
			params.Defaults.PreferLocalSSD = tc.defaultPreferLocalSSD
			createOpts, providerOpts, _, selectedMachineType, selectedArch, err := s.RoachprodOpts(params)
			require.NoError(t, err)
			gceOpts, ok := providerOpts.(*gce.ProviderOpts)
			require.True(t, ok)
			require.Equal(t, tc.expectedMachineType, selectedMachineType)
			require.Equal(t, tc.expectedMachineType, gceOpts.MachineType)
			if tc.expectVolumeType {
				require.Equal(t, tc.expectedVolumeType, gceOpts.PDVolumeType)
			}
			require.Equal(t, tc.expectedUseLocalSSD, createOpts.SSDOpts.UseLocalSSD)
			if tc.expectedArch != "" {
				require.Equal(t, tc.expectedArch, selectedArch)
			}
		})
	}
}

func TestGCEWorkloadUsesMachineCompatibleVolumeType(t *testing.T) {
	tests := []struct {
		name      string
		opts      []Option
		benchmark bool
	}{
		{
			name:      "benchmark remote storage",
			opts:      []Option{DisableLocalSSD()},
			benchmark: true,
		},
		{
			name: "explicit pd-ssd",
			opts: []Option{VolumeType("pd-ssd")},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := append([]Option{WorkloadNode(), WorkloadRequiresDisk()}, tc.opts...)
			s := MakeClusterSpec(5, opts...)
			params := RoachprodClusterConfig{
				Cloud:         GCE,
				PreferredArch: vm.ArchARM64,
				Benchmark:     tc.benchmark,
			}

			_, providerOpts, workloadProviderOpts, _, _, err := s.RoachprodOpts(params)
			require.NoError(t, err)

			gceOpts, ok := providerOpts.(*gce.ProviderOpts)
			require.True(t, ok)
			require.Equal(t, "t2a-standard-4", gceOpts.MachineType)
			require.Equal(t, "pd-ssd", gceOpts.PDVolumeType)

			workloadGCEOpts, ok := workloadProviderOpts.(*gce.ProviderOpts)
			require.True(t, ok)
			require.Equal(t, "c4a-standard-4", workloadGCEOpts.MachineType)
			require.Equal(t, "hyperdisk-balanced", workloadGCEOpts.PDVolumeType)
			require.False(t, workloadGCEOpts.BootDiskOnly)
		})
	}
}

func TestGCELocalSSDWorkloadBootDiskOnlyAvoidsLSSDMachineType(t *testing.T) {
	s := MakeClusterSpec(5, WorkloadNode())
	params := RoachprodClusterConfig{
		Cloud:         GCE,
		PreferredArch: vm.ArchARM64,
	}
	params.Defaults.PreferLocalSSD = true

	createOpts, providerOpts, workloadProviderOpts, _, _, err := s.RoachprodOpts(params)
	require.NoError(t, err)
	require.True(t, createOpts.SSDOpts.UseLocalSSD)

	gceOpts, ok := providerOpts.(*gce.ProviderOpts)
	require.True(t, ok)
	require.Equal(t, "c4a-standard-4-lssd", gceOpts.MachineType)
	require.False(t, gceOpts.BootDiskOnly)

	workloadGCEOpts, ok := workloadProviderOpts.(*gce.ProviderOpts)
	require.True(t, ok)
	require.Equal(t, "c4a-standard-4", workloadGCEOpts.MachineType)
	require.True(t, workloadGCEOpts.BootDiskOnly)
}

func TestGCET2ADefaultZones(t *testing.T) {
	s := MakeClusterSpec(4, DisableLocalSSD())
	params := RoachprodClusterConfig{
		Cloud:         GCE,
		PreferredArch: vm.ArchARM64,
		Benchmark:     true,
	}
	_, providerOpts, workloadProviderOpts, selectedMachineType, selectedArch, err := s.RoachprodOpts(params)
	require.NoError(t, err)

	gceOpts := providerOpts.(*gce.ProviderOpts)
	require.Equal(t, gceOpts.MachineType, selectedMachineType)
	zones := s.RoachprodCreateZones(params, selectedArch, selectedMachineType)
	providerOpts, workloadProviderOpts = s.SetRoachprodOptsZones(
		providerOpts, workloadProviderOpts, params.Cloud, zones,
	)
	gceOpts = providerOpts.(*gce.ProviderOpts)
	workloadGCEOpts := workloadProviderOpts.(*gce.ProviderOpts)

	require.Equal(t, "t2a-standard-4", gceOpts.MachineType)
	require.True(t, gce.IsSupportedT2AZone(gceOpts.Zones))
	require.Equal(t, gceOpts.Zones, workloadGCEOpts.Zones)
}

func TestGCEC4ADefaultZonesExcludeWest1B(t *testing.T) {
	s := MakeClusterSpec(4, Geo())
	params := RoachprodClusterConfig{
		Cloud:         GCE,
		PreferredArch: vm.ArchARM64,
	}
	params.Defaults.PreferLocalSSD = true
	_, providerOpts, workloadProviderOpts, selectedMachineType, selectedArch, err := s.RoachprodOpts(params)
	require.NoError(t, err)
	require.Equal(t, vm.ArchARM64, selectedArch)

	gceOpts := providerOpts.(*gce.ProviderOpts)
	require.Equal(t, gceOpts.MachineType, selectedMachineType)
	zones := s.RoachprodCreateZones(params, selectedArch, selectedMachineType)
	providerOpts, workloadProviderOpts = s.SetRoachprodOptsZones(
		providerOpts, workloadProviderOpts, params.Cloud, zones,
	)
	gceOpts = providerOpts.(*gce.ProviderOpts)
	workloadGCEOpts := workloadProviderOpts.(*gce.ProviderOpts)

	require.True(t, strings.HasPrefix(gceOpts.MachineType, "c4a-"))
	for _, z := range gceOpts.Zones {
		require.NotEqual(t, "us-west1-b", z, "C4A is not available in us-west1-b")
	}
	require.Equal(t, gceOpts.Zones, workloadGCEOpts.Zones)
}
