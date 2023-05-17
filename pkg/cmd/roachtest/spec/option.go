// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spec

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
)

// Option is the interface satisfied by options to MakeClusterSpec.
type Option interface {
	apply(spec *ClusterSpec)
}

type cloudOption string

func (o cloudOption) apply(spec *ClusterSpec) {
	spec.Cloud = string(o)
}

// Cloud controls what cloud is used to create the cluster.
func Cloud(s string) Option {
	return cloudOption(s)
}

type archOption string

func (o archOption) apply(spec *ClusterSpec) {
	spec.Arch = vm.CPUArch(o)
}

// Request specific CPU architecture.
func Arch(arch vm.CPUArch) Option {
	return archOption(arch)
}

type nodeCPUOption int

func (o nodeCPUOption) apply(spec *ClusterSpec) {
	spec.CPUs = int(o)
}

// CPU is a node option which requests nodes with the specified number of CPUs.
func CPU(n int) Option {
	return nodeCPUOption(n)
}

type nodeMemOption MemPerCPU

func (o nodeMemOption) apply(spec *ClusterSpec) {
	spec.Mem = MemPerCPU(o)
}

// Mem requests nodes with low/standard/high ratio of memory per CPU.
func Mem(level MemPerCPU) Option {
	return nodeMemOption(level)
}

type volumeSizeOption int

func (o volumeSizeOption) apply(spec *ClusterSpec) {
	spec.VolumeSize = int(o)
}

// VolumeSize is the size in GB of the disk volume.
func VolumeSize(n int) Option {
	return volumeSizeOption(n)
}

type nodeSSDOption int

func (o nodeSSDOption) apply(spec *ClusterSpec) {
	spec.SSDs = int(o)
}

// SSD is a node option which requests nodes with the specified number of SSDs.
func SSD(n int) Option {
	return nodeSSDOption(n)
}

type raid0Option bool

func (o raid0Option) apply(spec *ClusterSpec) {
	spec.RAID0 = bool(o)
}

// RAID0 enables RAID 0 striping across all disks on the node.
func RAID0(enabled bool) Option {
	return raid0Option(enabled)
}

type nodeGeoOption struct{}

func (o nodeGeoOption) apply(spec *ClusterSpec) {
	spec.Geo = true
}

// Geo is a node option which requests Geo-distributed nodes.
func Geo() Option {
	return nodeGeoOption{}
}

type nodeZonesOption string

func (o nodeZonesOption) apply(spec *ClusterSpec) {
	spec.Zones = string(o)
}

// Zones is a node option which requests Geo-distributed nodes. Note that this
// overrides the --zones flag and is useful for tests that require running on
// specific Zones.
func Zones(s string) Option {
	return nodeZonesOption(s)
}

type nodeLifetimeOption time.Duration

func (o nodeLifetimeOption) apply(spec *ClusterSpec) {
	spec.Lifetime = time.Duration(o)
}

type gatherCoresOption struct{}

func (o gatherCoresOption) apply(spec *ClusterSpec) {
	spec.GatherCores = true
}

// GatherCores enables core gathering after test runs.
func GatherCores() Option {
	return gatherCoresOption{}
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

type clusterReusePolicyOption struct {
	p clusterReusePolicy
}

// ReuseAny is an Option that specifies a cluster with ReusePolicyAny.
func ReuseAny() Option {
	return clusterReusePolicyOption{p: ReusePolicyAny{}}
}

// ReuseNone is an Option that specifies a cluster with ReusePolicyNone.
func ReuseNone() Option {
	return clusterReusePolicyOption{p: ReusePolicyNone{}}
}

// ReuseTagged is an Option that specifies a cluster with ReusePolicyTagged.
func ReuseTagged(tag string) Option {
	return clusterReusePolicyOption{p: ReusePolicyTagged{Tag: tag}}
}

func (p clusterReusePolicyOption) apply(spec *ClusterSpec) {
	spec.ReusePolicy = p.p
}

type preferLocalSSDOption bool

func (o preferLocalSSDOption) apply(spec *ClusterSpec) {
	spec.PreferLocalSSD = bool(o)
}

// PreferLocalSSD specifies whether to prefer using local SSD, when possible.
func PreferLocalSSD(prefer bool) Option {
	return preferLocalSSDOption(prefer)
}

type terminateOnMigrationOption struct{}

func (o terminateOnMigrationOption) apply(spec *ClusterSpec) {
	spec.TerminateOnMigration = true
}

// TerminateOnMigration ensures VM is terminated in case GCE triggers a live migration.
func TerminateOnMigration() Option {
	return &terminateOnMigrationOption{}
}

type setFileSystem struct {
	fs fileSystemType
}

func (s *setFileSystem) apply(spec *ClusterSpec) {
	spec.FileSystem = s.fs
}

// SetFileSystem is an Option which can be used to set
// the underlying file system to be used.
func SetFileSystem(fs fileSystemType) Option {
	return &setFileSystem{fs}
}

type randomlyUseZfs struct{}

func (r *randomlyUseZfs) apply(spec *ClusterSpec) {
	spec.RandomlyUseZfs = true
}

// RandomlyUseZfs is an Option which randomly picks
// the file system to be used, and sets it to zfs,
// about 20% of the time.
// Zfs is only picked if the cloud is gce.
func RandomlyUseZfs() Option {
	return &randomlyUseZfs{}
}
