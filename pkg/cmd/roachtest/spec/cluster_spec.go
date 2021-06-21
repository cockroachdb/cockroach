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
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
func (s *ClusterSpec) Args(extra ...string) ([]string, error) {
	var args []string

	switch s.Cloud {
	case AWS:
		args = append(args, "--clouds=aws")
	case GCE:
		args = append(args, "--clouds=gce")
	case Azure:
		args = append(args, "--clouds=azure")
	}

	var localSSD bool
	if s.CPUs != 0 {
		// Default to the user-supplied machine type, if any.
		// Otherwise, pick based on requested CPU count.
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
			case Local:
			// Don't need to set machineType.
			default:
				return nil, errors.Errorf("unsupported cloud %v", s.Cloud)
			}
		}

		// Local SSD can only be requested
		// - if configured to prefer doing so,
		// - if not running locally,
		// - if no particular volume size is requested, and,
		// - on AWS, if the machine type supports it.
		localSSD = s.PreferLocalSSD && s.Cloud != Local && s.VolumeSize == 0
		if s.Cloud == AWS {
			typeAndSize := strings.Split(machineType, ".")
			localSSD = localSSD && len(typeAndSize) == 2 &&
				// All SSD machine types that we use end in 'd or begins with i3 (e.g. i3, i3en).
				(strings.HasPrefix(typeAndSize[0], "i3") || strings.HasSuffix(typeAndSize[0], "d"))
		}
		// NB: emit the arg either way; changes to roachprod's default for the flag have
		// caused problems in the past (at time of writing, the default is 'true').
		args = append(args, "--local-ssd="+strconv.FormatBool(localSSD))

		switch s.Cloud {
		case AWS:
			if localSSD {
				args = append(args, "--aws-machine-type-ssd="+machineType)
			} else {
				args = append(args, "--aws-machine-type="+machineType)
			}
		case GCE:
			args = append(args, "--gce-machine-type="+machineType)
		case Azure:
			args = append(args, "--azure-machine-type="+machineType)
		case Local:
			// No flag needed.
		default:
			return nil, errors.Errorf("unsupported cloud: %s\n", s.Cloud)
		}
	}

	if s.VolumeSize != 0 {
		switch s.Cloud {
		case GCE:
			args = append(args, fmt.Sprintf("--gce-pd-volume-size=%d", s.VolumeSize))
		case Local:
			// Ignore the volume size.
		default:
			return nil, errors.Errorf("specifying volume size is not yet supported on %s", s.Cloud)
		}
	}

	// Note that localSSD implies that Cloud != Local.
	if localSSD && s.SSDs != 0 {
		switch s.Cloud {
		case GCE:
			args = append(args, fmt.Sprintf("--gce-local-ssd-count=%d", s.SSDs))
		default:
			return nil, errors.Errorf("specifying SSD count is not yet supported on %s", s.Cloud)
		}
	}

	zones := s.Zones
	if zones != "" {
		if !s.Geo {
			zones = firstZone(zones)
		}
		switch s.Cloud {
		case AWS:
			args = append(args, "--aws-zones="+zones)
		case GCE:
			args = append(args, "--gce-zones="+zones)
		case Azure:
			args = append(args, "--azure-locations="+zones)
		case Local:
			// Do nothing.
		default:
			return nil, errors.Errorf("specifying Zones is not yet supported on %s", s.Cloud)
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
	return args, nil
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
