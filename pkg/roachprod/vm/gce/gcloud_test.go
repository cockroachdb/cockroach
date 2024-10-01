// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"math"
	"math/rand"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"testing/quick"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

func TestAllowedLocalSSDCount(t *testing.T) {
	for i, c := range []struct {
		machineType string
		expected    []int
		unsupported bool
	}{
		// N1 has the same ssd counts for all cpu counts.
		{"n1-standard-4", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-highcpu-64", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},
		{"n1-higmem-96", []int{1, 2, 3, 4, 5, 6, 7, 8, 16, 24}, false},

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
		// c2-standard-64 doesn't exist and exceed cpu count, so we expect an error.
		{"c2-standard-64", nil, true},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
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
