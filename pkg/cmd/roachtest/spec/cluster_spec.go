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
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ClusterSpec represents a test's description of what its cluster needs to
// look like. It becomes part of a clusterConfig when the cluster is created.
type ClusterSpec struct {
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

// MakeClusterSpec makes a ClusterSpec.
func MakeClusterSpec(cloud string, instanceType string, nodeCount int, opts ...Option) ClusterSpec {
	spec := ClusterSpec{Cloud: cloud, InstanceType: instanceType, NodeCount: nodeCount}
	defaultOpts := []Option{CPU(4), nodeLifetimeOption(12 * time.Hour), ReuseAny()}
	for _, o := range append(defaultOpts, opts...) {
		o.apply(&spec)
	}
	return spec
}

// ClustersCompatible returns true if the clusters are compatible, i.e. the test
// asking for s2 can reuse s1.
func ClustersCompatible(s1, s2 ClusterSpec) bool {
	s1.Lifetime = 0
	s2.Lifetime = 0
	return s1 == s2
}

// String implements fmt.Stringer.
func (s ClusterSpec) String() string {
	str := fmt.Sprintf("n%dcpu%d", s.NodeCount, s.CPUs)
	if s.Geo {
		str += "-Geo"
	}
	return str
}

func firstZone(zones string) string {
	return strings.SplitN(zones, ",", 2)[0]
}

// Args are the arguments to pass to `roachprod create` in order
// to create the cluster described in the spec.
func (s *ClusterSpec) Args(extra ...string) []string {
	var args []string

	switch s.Cloud {
	case AWS:
		args = append(args, "--clouds=aws")
	case GCE:
		args = append(args, "--clouds=gce")
	case Azure:
		args = append(args, "--clouds=azure")
	}

	if s.Cloud != Local && s.CPUs != 0 {
		// Use the machine type specified as a CLI flag.
		machineType := s.InstanceType
		if len(machineType) == 0 {
			// If no machine type was specified, choose one
			// based on the cloud and CPU count.
			switch s.Cloud {
			case AWS:
				machineType = AWSMachineType(s.CPUs)
			case GCE:
				machineType = GCEMachineType(s.CPUs)
			case Azure:
				machineType = AzureMachineType(s.CPUs)
			}
		}

		var isAWSSSD bool // true if we're on AWS and local SSD is requested
		if s.Cloud == AWS {
			typeAndSize := strings.Split(machineType, ".")
			if len(typeAndSize) == 2 {
				awsType := typeAndSize[0]
				// All SSD machine types that we use end in 'd or begins with i3 (e.g. i3, i3en).
				isAWSSSD = strings.HasPrefix(awsType, "i3") || strings.HasSuffix(awsType, "d")
			}
		}

		if s.Cloud == AWS {
			if s.PreferLocalSSD && isAWSSSD {
				args = append(args, "--local-ssd=true")
			} else {
				args = append(args, "--local-ssd=false")
			}
		}

		var arg string
		switch s.Cloud {
		case AWS:
			if isAWSSSD {
				arg = "--aws-machine-type-ssd"
			} else {
				arg = "--aws-machine-type"
			}
		case GCE:
			arg = "--gce-machine-type"
		case Azure:
			arg = "--azure-machine-type"
		default:
			panic(fmt.Sprintf("unsupported cloud: %s\n", s.Cloud))
		}

		args = append(args, arg+"="+machineType)
	}

	if s.Cloud != Local && s.VolumeSize != 0 {
		fmt.Fprintln(os.Stdout, "test specification requires non-local SSDs, ignoring roachtest --local-ssd flag")
		// Set network disk options.
		args = append(args, "--local-ssd=false")

		var arg string
		switch s.Cloud {
		case GCE:
			arg = fmt.Sprintf("--gce-pd-volume-size=%d", s.VolumeSize)
		default:
			fmt.Fprintf(os.Stderr, "specifying volume size is not yet supported on %s", s.Cloud)
			os.Exit(1)
		}
		args = append(args, arg)
	}

	if s.Cloud != Local && s.SSDs != 0 {
		var arg string
		switch s.Cloud {
		case GCE:
			arg = fmt.Sprintf("--gce-local-SSD-count=%d", s.SSDs)
		default:
			fmt.Fprintf(os.Stderr, "specifying SSD count is not yet supported on %s", s.Cloud)
			os.Exit(1)
		}
		args = append(args, arg)
	}

	if s.Cloud != Local {
		zones := s.Zones
		if zones != "" {
			if !s.Geo {
				zones = firstZone(zones)
			}
			var arg string
			switch s.Cloud {
			case AWS:
				arg = "--aws-zones=" + zones
			case GCE:
				arg = "--gce-zones=" + zones
			case Azure:
				arg = "--azure-locations=" + zones
			default:
				fmt.Fprintf(os.Stderr, "specifying Zones is not yet supported on %s", s.Cloud)
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

// Expiration is the lifetime of the cluster. It may be destroyed after
// the expiration has passed.
func (s *ClusterSpec) Expiration() time.Time {
	l := s.Lifetime
	if l == 0 {
		l = 12 * time.Hour
	}
	return timeutil.Now().Add(l)
}
