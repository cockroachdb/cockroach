// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"io"
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"testing"
	"testing/quick"
	"time"

	computepb "cloud.google.com/go/compute/apiv1/computepb"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestAllowedLocalSSDCount(t *testing.T) {
	for _, c := range []struct {
		machineType string
		expected    []int
		unsupported bool
	}{
		// N1 has the same ssd counts for all cpu counts.
		{"n1-standard-4", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-highcpu-64", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-highmem-96", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},

		{"n2-standard-4", []int{1, 2, 4, 8, 16, 24}, false},
		{"n2-standard-8", []int{1, 2, 4, 8, 16, 24}, false},
		{"n2-standard-16", []int{2, 4, 8, 16, 24}, false},
		// N.B. n2-standard-30 doesn't exist, but we still get the ssd counts based on cpu count.
		{"n2-standard-30", []int{4, 8, 16, 24}, false},
		{"n2-standard-32", []int{4, 8, 16, 24}, false},
		{"n2-standard-48", []int{8, 16, 24}, false},
		{"n2-standard-64", []int{8, 16, 24}, false},
		{"n2-standard-80", []int{8, 16, 24}, false},
		{"n2-standard-96", []int{16, 24}, false},
		{"n2-standard-128", []int{16, 24}, false},

		{"c2-standard-4", []int{1, 2, 4, 8}, false},
		{"c2-standard-8", []int{1, 2, 4, 8}, false},
		{"c2-standard-16", []int{2, 4, 8}, false},
		{"c2-standard-30", []int{4, 8}, false},
		{"c2-standard-60", []int{8}, false},
		// N.B. n2-standard-64 doesn't exist, but we still get the ssd counts based on cpu count.
		{"c2-standard-64", []int{8}, false},

		{"c4a-standard-4", nil, true},
		{"c4a-standard-4-lssd", []int{1}, false},
		{"c4a-standard-16-lssd", []int{4}, false},
		{"c4a-standard-1-lssd", nil, true},
		{"c4a-highmem-32-lssd", []int{6}, false},
		{"c4a-highcpu-32-lssd", nil, true},
	} {
		t.Run(c.machineType, func(t *testing.T) {
			actual, err := AllowedLocalSSDCount(c.machineType)
			if c.unsupported {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.EqualValues(t, c.expected, actual)
			}
		})
	}
}

func TestBuildInstancePropertiesLocalSSDDisks(t *testing.T) {
	l, err := (&logger.Config{Stdout: io.Discard, Stderr: io.Discard}).NewLogger("")
	if err != nil {
		t.Fatal(err)
	}
	p := &Provider{Projects: []string{"test-project"}, defaultProject: "default-project"}

	testCases := []struct {
		name                 string
		machineType          string
		ssdCount             int
		expectedScratchDisks int
		expectedSSDCount     int
	}{
		{
			name:                 "requestable single-count local SSD machine attaches scratch disks",
			machineType:          "c2-standard-60",
			ssdCount:             8,
			expectedScratchDisks: 8,
			expectedSSDCount:     8,
		},
		{
			name:                 "requestable local SSD machine bumps below-min count",
			machineType:          "c2-standard-60",
			ssdCount:             1,
			expectedScratchDisks: 8,
			expectedSSDCount:     8,
		},
		{
			name:                 "requestable local SSD machine passes through above-min invalid count",
			machineType:          "c2-standard-30",
			ssdCount:             5,
			expectedScratchDisks: 5,
			expectedSSDCount:     5,
		},
		{
			name:                 "auto-attached local SSD machine does not request scratch disks",
			machineType:          "c4a-standard-4-lssd",
			ssdCount:             1,
			expectedScratchDisks: 0,
			expectedSSDCount:     1,
		},
		{
			name:                 "auto-attached local SSD machine normalizes mismatched count",
			machineType:          "c4a-standard-4-lssd",
			ssdCount:             2,
			expectedScratchDisks: 0,
			expectedSSDCount:     1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			opts := vm.DefaultCreateOpts()
			opts.SSDOpts.UseLocalSSD = true
			providerOpts := DefaultProviderOpts()
			providerOpts.MachineType = tc.machineType
			providerOpts.SSDCount = tc.ssdCount

			props, err := p.buildInstanceProperties(
				l, opts, providerOpts, "startup-script", "us-east1-b", nil,
			)
			assert.NoError(t, err)
			if err != nil {
				return
			}

			scratchDisks := 0
			for _, disk := range props.GetDisks() {
				if disk.GetType() == computepb.AttachedDisk_SCRATCH.String() {
					scratchDisks++
				}
			}
			assert.Equal(t, tc.expectedScratchDisks, scratchDisks)
			assert.Equal(t, tc.expectedSSDCount, providerOpts.SSDCount)
		})
	}
}

func TestParseGCECapacityError(t *testing.T) {
	const details = `ERROR: (gcloud.compute.instances.create) Could not fetch resource:
---
code: ZONE_RESOURCE_POOL_EXHAUSTED_WITH_DETAILS
errorDetails:
- localizedMessage:
    locale: en-US
    message: A t2a-standard-8 VM instance is currently unavailable in the us-central1-a
      zone. Consider trying your request in the us-central1-f zone(s), which currently
      has capacity to accommodate your request.
- errorInfo:
    domain: compute.googleapis.com
    metadatas:
      vmType: t2a-standard-8
      zone: us-central1-a
      zonesAvailable: us-central1-f
    reason: resource_availability
message: The zone 'projects/cockroach-ephemeral/zones/us-central1-a' does not have
  enough resources available to fulfill the request.`

	capacityErr := parseGCECapacityError(details)
	assert.NotNil(t, capacityErr)
	assert.Equal(t, vm.CreateCapacityClassZone, capacityErr.CapacityClass)
	assert.Equal(t, ProviderName, capacityErr.Provider)
	assert.Equal(t, "t2a-standard-8", capacityErr.MachineType)
	assert.Equal(t, []string{"us-central1-a"}, capacityErr.FailedZones)
	assert.Equal(t, []string{"us-central1-f"}, capacityErr.SuggestedZones)
}

func TestAnnotateGCECapacityErrorAddsZone(t *testing.T) {
	const details = `ERROR: (gcloud.compute.instance-groups.managed.wait-until) The zone does not have enough resources available to fulfill the request.`

	err := maybeGCECapacityError(errors.New("wait-until failed"), []byte(details))
	err = annotateGCECapacityError(err, "us-central1-a")

	var capacityErr *vm.CreateCapacityError
	assert.True(t, errors.As(err, &capacityErr))
	assert.Equal(t, vm.CreateCapacityClassZone, capacityErr.CapacityClass)
	assert.Equal(t, []string{"us-central1-a"}, capacityErr.FailedZones)
}

func TestDefaultC4AZonesExcludesUnsupportedZones(t *testing.T) {
	for _, geo := range []bool{false, true} {
		for i := 0; i < 20; i++ {
			zones := DefaultC4AZones(geo)
			if !IsSupportedC4AZone(zones) {
				t.Errorf("DefaultC4AZones(geo=%v) returned zones unsupported by C4A: %v", geo, zones)
			}
		}
	}
}

func TestC4AZoneValidation(t *testing.T) {
	for _, tc := range []struct {
		name      string
		zones     []string
		supported bool
	}{
		{name: "empty", supported: true},
		{name: "supported default", zones: []string{"us-east1-b", "us-west1-c", "europe-central2-a"}, supported: true},
		{name: "unsupported west1 b", zones: []string{"us-east1-b", "us-west1-b"}, supported: false},
		{name: "unsupported asia northeast1 a", zones: []string{"asia-northeast1-a"}, supported: false},
		{name: "unsupported europe central2 b", zones: []string{"europe-central2-b"}, supported: false},
		{name: "unsupported europe central2 c", zones: []string{"europe-central2-c"}, supported: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.supported, IsSupportedC4AZone(tc.zones))
		})
	}
}

func TestComputeZonesRejectsUnsupportedC4AExplicitZones(t *testing.T) {
	_, err := computeZones(vm.CreateOpts{GeoDistributed: true}, &ProviderOpts{
		MachineType: "c4a-standard-4-lssd",
		Zones:       []string{"us-east1-b", "us-west1-b"},
	})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "C4A instances are not supported")
}

func TestDefaultRetryZoneCandidates(t *testing.T) {
	assert.Equal(
		t,
		[]string{"us-east1-b", "us-east1-c", "us-east1-d"},
		DefaultRetryZoneCandidates("n2-standard-4"),
	)
	assert.Equal(
		t,
		[]string{"us-east1-b", "us-east1-c", "us-east1-d"},
		DefaultRetryZoneCandidates("c4a-standard-4-lssd"),
	)
	assert.Equal(
		t,
		[]string{"us-central1-a", "us-central1-b", "us-central1-f"},
		DefaultRetryZoneCandidates("t2a-standard-4"),
	)
}

func Test_buildFilterPreemptionCliArgs(t *testing.T) {
	type args struct {
		vms         vm.List
		projectName string
		since       time.Time
	}
	tests := []struct {
		name        string
		args        args
		wantCliArgs string
		wantErr     error
	}{
		{
			name: "One VM",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
				},
				projectName: "test-project",
				since:       timeutil.Now().Add(-time.Hour * 3),
			},
			wantCliArgs: "logging read --project=test-project --format=json --freshness=4h resource.type=gce_instance AND " +
				"(protoPayload.methodName=compute.instances.preempted) AND " +
				"(protoPayload.resourceName=projects/test-project/zones/us-west1-a/instances/test-vm)",
			wantErr: nil,
		},
		{name: "Two VMs + different project name + since 7 hrs",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
					{
						Name: "test-vm1",
						Zone: "us-west1-a",
					},
				},
				projectName: "test-project-z",
				since:       timeutil.Now().Add(-time.Hour * 7),
			},
			wantCliArgs: "logging read --project=test-project-z --format=json --freshness=8h resource.type=gce_instance AND " +
				"(protoPayload.methodName=compute.instances.preempted) AND " +
				"(protoPayload.resourceName=projects/test-project-z/zones/us-west1-a/instances/test-vm OR " +
				"protoPayload.resourceName=projects/test-project-z/zones/us-west1-a/instances/test-vm1)",
			wantErr: nil,
		},
		{name: "Two VMs from different zones + since 4 hrs",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
					{
						Name: "test-vm1",
						Zone: "us-east1-a",
					},
				},
				projectName: "test-project",
				since:       timeutil.Now().Add(-time.Hour * 4),
			},
			wantCliArgs: "logging read --project=test-project --format=json --freshness=5h resource.type=gce_instance AND " +
				"(protoPayload.methodName=compute.instances.preempted) AND " +
				"(protoPayload.resourceName=projects/test-project/zones/us-west1-a/instances/test-vm OR " +
				"protoPayload.resourceName=projects/test-project/zones/us-east1-a/instances/test-vm1)",
			wantErr: nil,
		},
		{name: "Nil VMs",
			args: args{
				vms:         nil,
				projectName: "test-project",
				since:       timeutil.Now().Add(-time.Hour * 4),
			},
			wantCliArgs: "",
			wantErr:     errors.New("vms cannot be nil"),
		},
		{name: "Empty Project",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
				},
				projectName: "",
				since:       timeutil.Now().Add(-time.Hour * 4),
			},
			wantCliArgs: "",
			wantErr:     errors.New("project name cannot be empty"),
		},
		{name: "Since in future",
			args: args{
				vms: []vm.VM{
					{
						Name: "test-vm",
						Zone: "us-west1-a",
					},
				},
				projectName: "test",
				since:       timeutil.Now().Add(time.Hour * 1),
			},
			wantCliArgs: "",
			wantErr:     errors.New("since cannot be in the future"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cliArgs, err := buildFilterPreemptionCliArgs(tt.args.vms, tt.args.projectName, tt.args.since)
			if tt.wantErr == nil {
				joinedString := strings.Join(cliArgs, " ")
				assert.Equalf(t, tt.wantCliArgs, joinedString, "buildFilterPreemptionCliArgs(%v, %v, %v)", tt.args.vms, tt.args.projectName, tt.args.since)
				assert.Equalf(t, tt.wantErr, err, "buildFilterPreemptionCliArgs(%v, %v, %v)", tt.args.vms, tt.args.projectName, tt.args.since)
			} else {
				assert.Equalf(t, []string(nil), cliArgs, "buildFilterPreemptionCliArgs(%v, %v, %v)", tt.args.vms, tt.args.projectName, tt.args.since)
				assert.Equalf(t, tt.wantErr.Error(), err.Error(), "buildFilterPreemptionCliArgs(%v, %v, %v)", tt.args.vms, tt.args.projectName, tt.args.since)
			}
		})
	}
}

func randInstanceGroupSizes(r *rand.Rand) []jsonManagedInstanceGroup {
	// We do not test empty sets, hence the +1.
	count := r.Intn(10) + 1
	groups := make([]jsonManagedInstanceGroup, count)
	for i := 0; i < count; i++ {
		groups[i].Size = r.Intn(32)
	}
	return groups
}

func TestComputeGrowDistribution(t *testing.T) {
	rng, _ := randutil.NewTestRand()
	c := quick.Config{MaxCount: 128,
		Rand: rng,
		Values: func(values []reflect.Value, r *rand.Rand) {
			values[0] = reflect.ValueOf(randInstanceGroupSizes(r))
		}}

	testDistribution := func(groups []jsonManagedInstanceGroup) bool {
		// Generate a random number of new nodes to add to the groups.
		newNodeCount := rng.Intn(24) + 1

		// Compute the total number of nodes before the distribution and
		// the maximum distance between the number of nodes in the groups.
		totalNodesBefore := 0
		curMax, curMin := 0.0, math.MaxFloat64
		for _, g := range groups {
			totalNodesBefore += g.Size
			curMax = math.Max(curMax, float64(g.Size))
			curMin = math.Min(curMin, float64(g.Size))
		}
		maxDistanceBefore := curMax - curMin

		// Sort the groups, compute the new distribution and apply it to the
		// group sizes.
		sort.Slice(groups, func(i, j int) bool {
			return groups[i].Size < groups[j].Size
		})
		newTargetSize := computeGrowDistribution(groups, newNodeCount)
		for idx := range newTargetSize {
			groups[idx].Size += newTargetSize[idx]
		}

		// Compute the total number of nodes after the distribution and the maximum
		// distance between the number of nodes in the groups.
		totalNodesAfter := 0
		curMax, curMin = 0.0, math.MaxFloat64
		for _, g := range groups {
			totalNodesAfter += g.Size
			curMax = math.Max(curMax, float64(g.Size))
			curMin = math.Min(curMin, float64(g.Size))
		}
		maxDistanceAfter := curMax - curMin

		// The total number of nodes should be the sum of the new node count and the
		// total number of nodes before the distribution.
		if totalNodesAfter != totalNodesBefore+newNodeCount {
			return false
		}
		// The maximum distance between the number of nodes in the groups should not
		// increase by more than 1, otherwise the new distribution was not fair.
		if maxDistanceAfter > maxDistanceBefore+1.0 {
			return false
		}
		return true
	}
	if err := quick.Check(testDistribution, &c); err != nil {
		t.Error(err)
	}
}
