// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// clusterSpec represents a test's description of what its cluster needs to
// look like. It becomes part of a clusterConfig when the cluster is created.
type clusterSpec struct {
	Cloud        string
	InstanceType string // auto-chosen if left empty
	NodeCount    int
	// CPUs is the number of CPUs per node.
	CPUs           int
	SSDs           int
	VolumeSize     int
	PreferLocalSSD bool
	Zones          string
	Geo            bool
	Lifetime       time.Duration
	ReusePolicy    clusterReusePolicy
}

func makeClusterSpec(
	cloud string, instanceType string, nodeCount int, opts ...createOption,
) clusterSpec {
	spec := clusterSpec{Cloud: cloud, InstanceType: instanceType, NodeCount: nodeCount}
	defaultOpts := []createOption{cpu(4), nodeLifetimeOption(12 * time.Hour), reuseAny()}
	for _, o := range append(defaultOpts, opts...) {
		o.apply(&spec)
	}
	return spec
}

func clustersCompatible(s1, s2 clusterSpec) bool {
	s1.Lifetime = 0
	s2.Lifetime = 0
	return s1 == s2
}

func (s clusterSpec) String() string {
	str := fmt.Sprintf("n%dcpu%d", s.NodeCount, s.CPUs)
	if s.Geo {
		str += "-geo"
	}
	return str
}

func firstZone(zones string) string {
	return strings.SplitN(zones, ",", 2)[0]
}

func (s *clusterSpec) args(extra ...string) []string {
	var args []string

	switch s.Cloud {
	case spec.AWS:
		args = append(args, "--clouds=aws")
	case spec.GCE:
		args = append(args, "--clouds=gce")
	case spec.Azure:
		args = append(args, "--clouds=azure")
	}

	if s.Cloud != spec.Local && s.CPUs != 0 {
		// Use the machine type specified as a CLI flag.
		machineType := s.InstanceType
		if len(machineType) == 0 {
			// If no machine type was specified, choose one
			// based on the cloud and CPU count.
			switch s.Cloud {
			case spec.AWS:
				machineType = awsMachineType(s.CPUs)
			case spec.GCE:
				machineType = gceMachineType(s.CPUs)
			case spec.Azure:
				machineType = azureMachineType(s.CPUs)
			}
		}

		var isAWSSSD bool // true if we're on AWS and local SSD is requested
		if s.Cloud == spec.AWS {
			typeAndSize := strings.Split(machineType, ".")
			if len(typeAndSize) == 2 {
				awsType := typeAndSize[0]
				// All SSD machine types that we use end in 'd or begins with i3 (e.g. i3, i3en).
				isAWSSSD = strings.HasPrefix(awsType, "i3") || strings.HasSuffix(awsType, "d")
			}
		}

		if s.Cloud == spec.AWS {
			if s.PreferLocalSSD && isAWSSSD {
				args = append(args, "--local-ssd=true")
			} else {
				args = append(args, "--local-ssd=false")
			}
		}

		var arg string
		switch s.Cloud {
		case spec.AWS:
			if isAWSSSD {
				arg = "--aws-machine-type-ssd"
			} else {
				arg = "--aws-machine-type"
			}
		case spec.GCE:
			arg = "--gce-machine-type"
		case spec.Azure:
			arg = "--azure-machine-type"
		default:
			panic(fmt.Sprintf("unsupported cloud: %s\n", s.Cloud))
		}

		args = append(args, arg+"="+machineType)
	}

	if s.Cloud != spec.Local && s.VolumeSize != 0 {
		fmt.Fprintln(os.Stdout, "test specification requires non-local SSDs, ignoring roachtest --local-ssd flag")
		// Set network disk options.
		args = append(args, "--local-ssd=false")

		var arg string
		switch s.Cloud {
		case spec.GCE:
			arg = fmt.Sprintf("--gce-pd-volume-size=%d", s.VolumeSize)
		default:
			fmt.Fprintf(os.Stderr, "specifying volume size is not yet supported on %s", s.Cloud)
			os.Exit(1)
		}
		args = append(args, arg)
	}

	if s.Cloud != spec.Local && s.SSDs != 0 {
		var arg string
		switch s.Cloud {
		case spec.GCE:
			arg = fmt.Sprintf("--gce-local-ssd-count=%d", s.SSDs)
		default:
			fmt.Fprintf(os.Stderr, "specifying ssd count is not yet supported on %s", s.Cloud)
			os.Exit(1)
		}
		args = append(args, arg)
	}

	if s.Cloud != spec.Local {
		zones := s.Zones
		if zones == "" {
			zones = zonesF
		}
		if zones != "" {
			if !s.Geo {
				zones = firstZone(zones)
			}
			var arg string
			switch s.Cloud {
			case spec.AWS:
				arg = "--aws-zones=" + zones
			case spec.GCE:
				arg = "--gce-zones=" + zones
			case spec.Azure:
				arg = "--azure-locations=" + zones
			default:
				fmt.Fprintf(os.Stderr, "specifying zones is not yet supported on %s", s.Cloud)
				os.Exit(1)
			}
			args = append(args, arg)
		}
	}

	if s.Geo {
		args = append(args, "--geo")
	}
	if s.Lifetime != 0 {
		args = append(args, "--lifetime="+s.Lifetime.String())
	}
	if len(extra) > 0 {
		args = append(args, extra...)
	}
	return args
}

func (s *clusterSpec) expiration() time.Time {
	l := s.Lifetime
	if l == 0 {
		l = 12 * time.Hour
	}
	return timeutil.Now().Add(l)
}

type createOption interface {
	apply(spec *clusterSpec)
}

type nodeCPUOption int

func (o nodeCPUOption) apply(spec *clusterSpec) {
	spec.CPUs = int(o)
}

// cpu is a node option which requests nodes with the specified number of CPUs.
func cpu(n int) nodeCPUOption {
	return nodeCPUOption(n)
}

type volumeSizeOption int

func (o volumeSizeOption) apply(spec *clusterSpec) {
	spec.VolumeSize = int(o)
}

// volumeSize is the size in GB of the disk volume.
func volumeSize(n int) volumeSizeOption {
	return volumeSizeOption(n)
}

type nodeSSDOption int

func (o nodeSSDOption) apply(spec *clusterSpec) {
	spec.SSDs = int(o)
}

// ssd is a node option which requests nodes with the specified number of SSDs.
func ssd(n int) nodeSSDOption {
	return nodeSSDOption(n)
}

type nodeGeoOption struct{}

func (o nodeGeoOption) apply(spec *clusterSpec) {
	spec.Geo = true
}

// geo is a node option which requests geo-distributed nodes.
func geo() nodeGeoOption {
	return nodeGeoOption{}
}

type nodeZonesOption string

func (o nodeZonesOption) apply(spec *clusterSpec) {
	spec.Zones = string(o)
}

// zones is a node option which requests geo-distributed nodes. Note that this
// overrides the --zones flag and is useful for tests that require running on
// specific zones.
func zones(s string) nodeZonesOption {
	return nodeZonesOption(s)
}

type nodeLifetimeOption time.Duration

func (o nodeLifetimeOption) apply(spec *clusterSpec) {
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

// reusePolicyAny means that only clean clusters are accepted and the cluster
// can be used by any other test (i.e. the cluster remains "clean").
type reusePolicyAny struct{}

// reusePolicyNone means that only clean clusters are accepted and the cluster
// cannot be reused afterwards.
type reusePolicyNone struct{}

// reusePolicyTagged means that clusters left over by similarly-tagged tests are
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
type reusePolicyTagged struct{ tag string }

func (reusePolicyAny) clusterReusePolicy()    {}
func (reusePolicyNone) clusterReusePolicy()   {}
func (reusePolicyTagged) clusterReusePolicy() {}

type clusterReusePolicyOption struct {
	p clusterReusePolicy
}

func reuseAny() clusterReusePolicyOption {
	return clusterReusePolicyOption{p: reusePolicyAny{}}
}
func reuseNone() clusterReusePolicyOption {
	return clusterReusePolicyOption{p: reusePolicyNone{}}
}
func reuseTagged(tag string) clusterReusePolicyOption {
	return clusterReusePolicyOption{p: reusePolicyTagged{tag: tag}}
}

func (p clusterReusePolicyOption) apply(spec *clusterSpec) {
	spec.ReusePolicy = p.p
}
