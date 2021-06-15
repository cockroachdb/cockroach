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

import "time"

// Option is the interface satisfied by options to MakeClusterSpec.
type Option interface {
	apply(spec *ClusterSpec)
}

type nodeCPUOption int

func (o nodeCPUOption) apply(spec *ClusterSpec) {
	spec.CPUs = int(o)
}

// CPU is a node option which requests nodes with the specified number of CPUs.
func CPU(n int) Option {
	return nodeCPUOption(n)
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

type preferSSDOption struct{}

func (*preferSSDOption) apply(spec *ClusterSpec) {
	spec.PreferLocalSSD = true
}

// PreferSSD prefers using local SSD, when possible.
func PreferSSD() Option {
	return &preferSSDOption{}
}
