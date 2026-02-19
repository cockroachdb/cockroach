// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod/test/framework"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce"
	"github.com/stretchr/testify/require"
)

// ========================================
// Architecture tests
// ========================================

// TestCreateAMD64 tests creating a basic AMD64 GCE cluster with default settings
func TestCreateAMD64(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	t.Logf("Creating AMD64 cluster with %d nodes (seed=%d)", numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
}

// TestCreateARM64 tests creating an ARM64 cluster with a T2A machine type,
// a T2A-compatible zone, and persistent disk storage.
func TestCreateARM64(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	// Pick a random T2A-compatible zone
	zone := gce.SupportedT2AZones[rpt.Rand().Intn(len(gce.SupportedT2AZones))]
	t.Logf("Using T2A zone: %s, nodes: %d (seed=%d)", zone, numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--arch", "arm64",
		"--gce-machine-type", "t2a-standard-4",
		"--gce-zones", zone,
		"--local-ssd=false",
		"--gce-pd-volume-type", "pd-balanced",
		"--gce-pd-volume-size", "100",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterMachineType("t2a-standard-4")
	rpt.AssertClusterArchitecture("arm64")
	rpt.AssertClusterZone(zone)
}

// TestCreateFIPS tests creating a FIPS-mode cluster.
// FIPS implies amd64 with OpenSSL — the VM reports amd64 architecture.
func TestCreateFIPS(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	t.Logf("Creating FIPS cluster with %d nodes (seed=%d)", numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--arch", "fips",
		"--gce-machine-type", "n2-standard-4",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterMachineType("n2-standard-4")
	rpt.AssertClusterArchitecture("amd64")
}

// ========================================
// Zone distribution tests
// ========================================

// TestCreateWithSpecificZone tests creating a cluster in a specific GCE zone
// and verifies all VMs are created in the correct zone
func TestCreateWithSpecificZone(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	// Get the list of potential zones from roachprod GCE provider implementation
	// This list is small, is there a benefit to expanding it?
	validZones := gce.DefaultZones(string(vm.ArchAMD64), true)

	// Randomly pick a zone from the valid zones
	zone := validZones[rpt.Rand().Intn(len(validZones))]
	numNodes := 2 + rpt.Rand().Intn(2) // 2-3 nodes
	t.Logf("Testing with zone: %s, nodes: %d (seed=%d)", zone, numNodes, rpt.Seed())

	// Create cluster with specific zone
	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-zones", zone,
	)

	// Verify cluster is in the correct zone
	rpt.AssertClusterZone(zone)

	// Also verify we got the right number of nodes
	rpt.AssertClusterNodeCount(numNodes)

	// And verify it's on GCE
	rpt.AssertClusterCloud("gce")
}

// TestCreateWithZoneCounts tests creating a cluster using the AZ:N zone format
// where N specifies how many nodes should be in each zone
func TestCreateWithZoneCounts(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	// Get valid zones and randomly select 2-3 zones
	validZones := gce.DefaultZones(string(vm.ArchAMD64), true)
	numZones := 2 + rpt.Rand().Intn(2) // 2 or 3 zones

	// Shuffle and pick first numZones
	selectedZones := make([]string, len(validZones))
	copy(selectedZones, validZones)
	rpt.Rand().Shuffle(len(selectedZones), func(i, j int) {
		selectedZones[i], selectedZones[j] = selectedZones[j], selectedZones[i]
	})
	selectedZones = selectedZones[:numZones]

	// Assign random node counts to each zone (1-3 nodes per zone)
	zoneCounts := make(map[string]int)
	totalNodes := 0
	zoneArgs := make([]string, numZones)
	for i, zone := range selectedZones {
		count := 1 + rpt.Rand().Intn(3) // 1-3 nodes
		zoneCounts[zone] = count
		totalNodes += count
		zoneArgs[i] = fmt.Sprintf("%s:%d", zone, count)
	}

	zonesArg := strings.Join(zoneArgs, ",")

	t.Logf("Creating cluster with zone counts: %s (total nodes: %d)", zonesArg, totalNodes)

	// Create cluster with zone counts
	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", totalNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-zones", zonesArg,
	)

	// Verify cluster has the correct total number of nodes
	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(totalNodes)
	rpt.AssertClusterCloud("gce")

	// Verify node distribution across zones
	info := rpt.GetClusterInfo()
	actualZoneCounts := make(map[string]int)
	for _, vm := range info.VMs {
		actualZoneCounts[vm.Zone]++
	}

	t.Logf("Expected zone distribution: %v", zoneCounts)
	t.Logf("Actual zone distribution: %v", actualZoneCounts)

	// Verify each zone has the expected number of nodes
	for zone, expectedCount := range zoneCounts {
		actualCount := actualZoneCounts[zone]
		require.Equal(t, expectedCount, actualCount,
			"Zone %s should have %d nodes, but has %d", zone, expectedCount, actualCount)
	}

	// Verify no unexpected zones
	require.Equal(t, len(zoneCounts), len(actualZoneCounts),
		"Should have nodes in exactly %d zones, but found %d", len(zoneCounts), len(actualZoneCounts))
}

// TestCreateGeo tests creating a geo-distributed cluster.
// --geo selects from hardcoded multi-region defaults:
//   - us-east1-{b,c,d} (shuffled)
//   - us-west1-{a,b,c}
//   - europe-west2-{a,b,c}
//
// Unlike --gce-zones which takes explicit zone names, --geo is a convenience
// flag for multi-region spread. --gce-zones overrides --geo if both are set.
// Verifies nodes are spread across at least 2 zones.
func TestCreateGeo(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", "3",
		"--clouds", "gce",
		"--lifetime", "1h",
		"--geo",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(3)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterMultiZone(2)
}

// ========================================
// Image tests
// ========================================

// TestCreateCustomImage tests creating a cluster with a specific GCE image
// instead of the default roachprod image.
// Uses the same image drtprod uses. (different than roachprod's default)
func TestCreateCustomImage(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-image", "ubuntu-2204-jammy-v20250112", // Supported until April 2027
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
}

// ========================================
// Storage tests
// ========================================
// Note: a single local SSD with ext4 is the default storage configuration
// when no storage flags are specified. The tests below cover non-default
// storage configurations.

// TestCreatePersistentDisk tests creating a cluster with persistent disk storage,
// randomly selecting from the simple PD types that don't require extra provisioning
// (pd-ssd, pd-balanced, pd-standard). Types like pd-extreme and hyperdisk-* need
// provisioned IOPS/throughput and are covered by TestCreateHyperdisk.
func TestCreatePersistentDisk(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	pdTypes := []string{"pd-ssd", "pd-balanced", "pd-standard"}
	pdType := pdTypes[rpt.Rand().Intn(len(pdTypes))]
	t.Logf("Testing with PD type: %s, nodes: %d (seed=%d)", pdType, numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--local-ssd=false",
		"--gce-pd-volume-type", pdType,
		"--gce-pd-volume-size", "200",
		"--gce-pd-volume-count", "2",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterNonBootVolumeCount(2)
	rpt.AssertClusterNonBootVolumeType(pdType)
	rpt.AssertClusterNonBootVolumeSize(200)
}

// TestCreateLocalSSDMultiStore tests creating a cluster with multiple local
// SSDs, multiple stores (one per disk), and spot instances.
func TestCreateLocalSSDMultiStore(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	t.Logf("Creating local SSD multi-store cluster with %d nodes (seed=%d)", numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--local-ssd",
		"--gce-local-ssd-count", "2",
		"--gce-enable-multiple-stores",
		"--gce-machine-type", "n2-standard-4",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterMachineType("n2-standard-4")
	rpt.AssertClusterLocalDiskCount(2)
}

// TestCreateBootDiskOnly tests creating a cluster with boot-disk-only mode,
// a specific boot disk type, cron enabled, and TERMINATE maintenance policy.
func TestCreateBootDiskOnly(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	t.Logf("Creating boot-disk-only cluster with %d nodes (seed=%d)", numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-boot-disk-only",
		"--gce-boot-disk-type", "pd-balanced",
		"--gce-enable-cron",
		"--gce-terminateOnMigration",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterNonBootVolumeCount(0)
}

// TestCreateHyperdisk tests creating a cluster with multiple hyperdisk-balanced
// volumes with provisioned IOPS and throughput. These flags are only wired through
// the CLI path, so --gce-use-bulk-insert=false is required.
func TestCreateHyperdisk(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	t.Logf("Creating hyperdisk cluster with %d nodes (seed=%d)", numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-use-bulk-insert=false",
		"--local-ssd=false",
		"--gce-pd-volume-count", "2",
		"--gce-pd-volume-type", "hyperdisk-balanced",
		"--gce-pd-volume-size", "100",
		"--gce-pd-volume-provisioned-iops", "3000",
		"--gce-pd-volume-provisioned-throughput", "140",
		"--gce-machine-type", "n2-standard-4",
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterNonBootVolumeCount(2)
	rpt.AssertClusterNonBootVolumeType("hyperdisk-balanced")
	rpt.AssertClusterNonBootVolumeSize(100)

	// Note: provisioned IOPS and throughput are GCE control plane properties
	// not exposed by roachprod list. The create command succeeding is itself
	// validation — GCE rejects invalid IOPS/throughput for the disk type.
}

// TestCreateFilesystemRandomized tests creating a cluster with a randomly selected filesystem.
// Roachprod supports ext4, zfs, xfs, f2fs, and btrfs.
func TestCreateFilesystemRandomized(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	numNodes := 1 + rpt.Rand().Intn(3)
	filesystems := []vm.Filesystem{vm.Ext4, vm.Zfs, vm.Xfs, vm.F2fs, vm.Btrfs}
	fs := filesystems[rpt.Rand().Intn(len(filesystems))]
	t.Logf("Testing with filesystem: %s, nodes: %d (seed=%d)", fs, numNodes, rpt.Seed())

	rpt.RunExpectSuccess("create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--filesystem", string(fs),
	)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterFilesystem(string(fs))
}

// ========================================
// Instance and CPU configuration tests
// ========================================

// TestCreateCPUConfig tests CPU hardware configuration flags: threads-per-core,
// min-cpu-platform, and turbo mode. Uses a C4 machine type which supports turbo mode.
// These flags require --gce-use-bulk-insert=false as they are not yet wired through
// the SDK BulkInsert path.
//
// --threads-per-core controls SMT (Simultaneous Multithreading / Hyper-Threading)
// and is randomized: 0 (omit flag, GCE default), 1 (disable SMT — one thread per
// physical core), or 2 (explicitly enable SMT — two threads per core).
func TestCreateCPUConfig(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(10*time.Minute))

	// 0 = omit the flag (GCE default), 1 = disable SMT, 2 = enable SMT
	threadsPerCore := rpt.Rand().Intn(3)
	t.Logf("Testing with threads-per-core=%d (0=omitted) (seed=%d)", threadsPerCore, rpt.Seed())

	numNodes := 1 + rpt.Rand().Intn(3)
	args := []string{
		"create", rpt.ClusterName(),
		"-n", fmt.Sprintf("%d", numNodes),
		"--clouds", "gce",
		"--lifetime", "1h",
		"--gce-use-bulk-insert=false",
		"--gce-machine-type", "c4-standard-4",
		"--gce-min-cpu-platform", "any",
		"--gce-turbo-mode", "ALL_CORE_MAX",
	}
	if threadsPerCore > 0 {
		args = append(args, "--gce-threads-per-core", fmt.Sprintf("%d", threadsPerCore))
	}

	rpt.RunExpectSuccess(args...)

	rpt.AssertClusterExists()
	rpt.AssertClusterNodeCount(numNodes)
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterMachineType("c4-standard-4")
}

// ========================================
// Randomized tests
// ========================================

// TestCreateRandomized tests creating clusters with randomized GCE options
func TestCreateRandomized(t *testing.T) {
	t.Parallel() // SAFE FOR TESTING (see main_test.go)
	rpt := framework.NewRoachprodTest(t, framework.WithTimeout(15*time.Minute))

	// Generate random GCE create options using the test's seeded RNG
	opts := framework.RandomGCECreateOptions(rpt.Rand())
	t.Logf("Creating cluster with random options (seed=%d): %s", rpt.Seed(), opts.String())

	// Create cluster with randomized options
	args := opts.ToCreateArgs(rpt.ClusterName())
	rpt.RunExpectSuccess(args...)

	// Verify cluster exists with correct configuration
	rpt.AssertClusterExists()
	rpt.AssertClusterCloud("gce")
	rpt.AssertClusterNodeCount(opts.NumNodes)

	// Type assert to GCE options for verification
	gceOpts := opts.ProviderOpts.(*gce.ProviderOpts)
	rpt.AssertClusterMachineType(gceOpts.MachineType)
	if len(gceOpts.Zones) > 0 {
		// If specific zones were requested, verify first node is in one of them
		info := rpt.GetClusterInfo()
		require.NotEmpty(t, info.VMs, "Cluster should have VMs")
		require.Contains(t, gceOpts.Zones, info.VMs[0].Zone,
			"VM zone %s should be in requested zones %v", info.VMs[0].Zone, gceOpts.Zones)
	}

	// Verify we can get cluster status
	status := rpt.Status()
	require.True(t, status.Success(), "Status should succeed")
}
