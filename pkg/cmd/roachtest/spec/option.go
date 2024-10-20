// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package spec

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

// Option for MakeClusterSpec.
type Option func(spec *ClusterSpec)

// Arch requests a specific CPU architecture.
//
// Note that it is not guaranteed that this architecture will be used (e.g. if
// the requested machine size isn't available in this architecture).
//
// TODO(radu): add a flag to indicate whether it's a preference or a requirement.
func Arch(arch vm.CPUArch) Option {
	return func(spec *ClusterSpec) {
		spec.Arch = arch
	}
}

// CPU sets the number of CPUs for each node.
func CPU(n int) Option {
	return func(spec *ClusterSpec) {
		spec.CPUs = n
	}
}

// WorkloadNodeCount indicates the count of last nodes in cluster to be treated
// as workload node. Defaults to a VM with 4 CPUs if not specified by
// WorkloadNodeCPUs.
func WorkloadNodeCount(n int) Option {
	return func(spec *ClusterSpec) {
		spec.WorkloadNodeCount = n
		spec.WorkloadNode = true
	}
}

// TODO(GouravKumar): remove use of WorkloadNode, use WorkloadNodeCount instead
func WorkloadNode() Option {
	return func(spec *ClusterSpec) {
		spec.WorkloadNode = true
		spec.WorkloadNodeCount = 1
	}
}

func WorkloadNodeCPU(n int) Option {
	return func(spec *ClusterSpec) {
		spec.WorkloadNodeCPUs = n
	}
}

// Mem requests nodes with low/standard/high ratio of memory per CPU.
func Mem(level MemPerCPU) Option {
	return func(spec *ClusterSpec) {
		spec.Mem = level
	}
}

// VolumeSize is the size in GB of the disk volume.
func VolumeSize(n int) Option {
	return func(spec *ClusterSpec) {
		spec.VolumeSize = n
	}
}

// SSD is a node option which requests nodes with the specified number of SSDs.
func SSD(n int) Option {
	return func(spec *ClusterSpec) {
		spec.SSDs = n
	}
}

// RAID0 enables RAID 0 striping across all disks on the node.
func RAID0(enabled bool) Option {
	return func(spec *ClusterSpec) {
		spec.RAID0 = enabled
	}
}

// Geo requests Geo-distributed nodes.
func Geo() Option {
	return func(spec *ClusterSpec) {
		spec.Geo = true
	}
}

func nodeLifetime(lifetime time.Duration) Option {
	return func(spec *ClusterSpec) {
		spec.Lifetime = lifetime
	}
}

// GatherCores enables core gathering after test runs.
func GatherCores() Option {
	return func(spec *ClusterSpec) {
		spec.GatherCores = true
	}
}

// clusterReusePolicy indicates what clusters a particular test can run on and
// who (if anybody) can reuse the cluster after the test has finished running
// (either passing or failing). See the individual policies for details.
//
// Only tests whose cluster spec matches can ever run on the same
// cluster, regardless of this policy.
//
// Clean clusters (freshly-created clusters or cluster on which a test with the
// Any policy ran) are accepted by all policies.
//
// Note that not all combinations of "what cluster can I accept" and "how am I
// soiling this cluster" can be expressed. For example, there's no way to
// express that I'll accept a cluster that was tagged a certain way but after me
// nobody else can reuse the cluster at all.
type clusterReusePolicy interface {
	clusterReusePolicy()
}

// ReusePolicyAny means that only clean clusters are accepted and the cluster
// can be used by any other test (i.e. the cluster remains "clean").
type ReusePolicyAny struct{}

// ReusePolicyNone means that only clean clusters are accepted and the cluster
// cannot be reused afterwards.
type ReusePolicyNone struct{}

// ReusePolicyTagged means that clusters left over by similarly-tagged tests are
// accepted in addition to clean cluster and, regardless of how the cluster
// started up, it will be tagged with the given tag at the end (so only
// similarly-tagged tests can use it afterwards).
//
// The idea is that a tag identifies a particular way in which a test is soiled,
// since it's common for groups of tests to mess clusters up in similar ways and
// to also be able to reset the cluster when the test starts. It's like a virus
// - if you carry it, you infect a clean host and can otherwise intermingle with
// other hosts that are already infected. Note that using this policy assumes
// that the way in which every test soils the cluster is idempotent.
type ReusePolicyTagged struct{ Tag string }

func (ReusePolicyAny) clusterReusePolicy()    {}
func (ReusePolicyNone) clusterReusePolicy()   {}
func (ReusePolicyTagged) clusterReusePolicy() {}

// ReuseAny is an Option that specifies a cluster with ReusePolicyAny.
func ReuseAny() Option {
	return func(spec *ClusterSpec) {
		spec.ReusePolicy = ReusePolicyAny{}
	}
}

// ReuseNone is an Option that specifies a cluster with ReusePolicyNone.
func ReuseNone() Option {
	return func(spec *ClusterSpec) {
		spec.ReusePolicy = ReusePolicyNone{}
	}
}

// ReuseTagged is an Option that specifies a cluster with ReusePolicyTagged.
func ReuseTagged(tag string) Option {
	return func(spec *ClusterSpec) {
		spec.ReusePolicy = ReusePolicyTagged{Tag: tag}
	}
}

// PreferLocalSSD specifies that we use instance-local SSDs whenever possible
// (depending on other constraints on machine type).
//
// By default, a test cluster may or may not use a local SSD depending on
// --local-ssd flag and machine type.
func PreferLocalSSD() Option {
	return func(spec *ClusterSpec) {
		spec.LocalSSD = LocalSSDPreferOn
	}
}

// DisableLocalSSD specifies that we never use instance-local SSDs.
//
// By default, a test cluster may or may not use a local SSD depending on
// --local-ssd flag and machine type.
func DisableLocalSSD() Option {
	return func(spec *ClusterSpec) {
		spec.LocalSSD = LocalSSDDisable
	}
}

// TerminateOnMigration ensures VM is terminated in case GCE triggers a live migration.
func TerminateOnMigration() Option {
	return func(spec *ClusterSpec) {
		spec.TerminateOnMigration = true
	}
}

// UseSpotVMs creates a spot vm or equivalent of a cloud provider.
// Using this option creates SpotVMs instead of on demand VMS. SpotVMS are
// cheaper but can be terminated at any time by the cloud provider.
// This option is only supported by GCE for now.
// See https://cloud.google.com/compute/docs/instances/spot,
// https://azure.microsoft.com/en-in/products/virtual-machines/spot
// and https://aws.amazon.com/ec2/spot/ for more details.
func UseSpotVMs() Option {
	return func(spec *ClusterSpec) {
		spec.UseSpotVMs = true
	}
}

// SetFileSystem is an Option which can be used to set
// the underlying file system to be used.
func SetFileSystem(fs fileSystemType) Option {
	return func(spec *ClusterSpec) {
		spec.FileSystem = fs
	}
}

// RandomlyUseZfs is an Option which randomly picks
// the file system to be used, and sets it to zfs,
// about 20% of the time.
// Zfs is only picked if the cloud is gce.
func RandomlyUseZfs() Option {
	return func(spec *ClusterSpec) {
		spec.RandomlyUseZfs = true
	}
}

// GCEMachineType sets the machine (instance) type when the cluster is on GCE.
func GCEMachineType(machineType string) Option {
	return func(spec *ClusterSpec) {
		spec.GCE.MachineType = machineType
	}
}

// GCEMinCPUPlatform sets the minimum CPU platform when the cluster is on GCE.
func GCEMinCPUPlatform(platform string) Option {
	return func(spec *ClusterSpec) {
		spec.GCE.MinCPUPlatform = platform
	}
}

// GCEVolumeType sets the volume type when the cluster is on GCE.
func GCEVolumeType(volumeType string) Option {
	return func(spec *ClusterSpec) {
		spec.GCE.VolumeType = volumeType
	}
}

// GCEZones is a node option which requests Geo-distributed nodes; only applies
// when the test runs on GCE.
//
// Note that this overrides the --zones flag and is useful for tests that
// require running on specific zones.
func GCEZones(zones string) Option {
	return func(spec *ClusterSpec) {
		spec.GCE.Zones = zones
	}
}

// AWSMachineType sets the machine (instance) type when the cluster is on AWS.
func AWSMachineType(machineType string) Option {
	return func(spec *ClusterSpec) {
		spec.AWS.MachineType = machineType
	}
}

// AWSVolumeThroughput sets the minimum provisioned EBS volume throughput when
// the cluster is on AWS.
func AWSVolumeThroughput(throughput int) Option {
	return func(spec *ClusterSpec) {
		spec.AWS.VolumeThroughput = throughput
	}
}

// AWSZones is a node option which requests Geo-distributed nodes; only applies
// when the test runs on AWS.
//
// Note that this overrides the --zones flag and is useful for tests that
// require running on specific zones.
func AWSZones(zones string) Option {
	return func(spec *ClusterSpec) {
		spec.AWS.Zones = zones
	}
}

// AzureZones is a node option which requests Geo-distributed nodes; only applies
// when the test runs on Azure.
//
// Note that this overrides the --zones flag and is useful for tests that
// require running on specific zones.
//
// TODO(darrylwong): Something is not quite right when creating
// zones that have overlapping address spaces, i.e. eastus and westus.
// See: https://github.com/cockroachdb/cockroach/issues/124612
func AzureZones(zones string) Option {
	return func(spec *ClusterSpec) {
		spec.Azure.Zones = zones
	}
}
