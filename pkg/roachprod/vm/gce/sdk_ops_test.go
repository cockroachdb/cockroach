// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gce

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
)

func TestBuildVMResourceName(t *testing.T) {
	testCases := []struct {
		name         string
		project      string
		zone         string
		instanceName string
		expected     string
	}{
		{
			name:         "standard VM resource name",
			project:      "test-project",
			zone:         "us-east1-b",
			instanceName: "my-instance",
			expected:     "projects/test-project/zones/us-east1-b/instances/my-instance",
		},
		{
			name:         "europe zone",
			project:      "prod-project",
			zone:         "europe-west2-a",
			instanceName: "web-server-1",
			expected:     "projects/prod-project/zones/europe-west2-a/instances/web-server-1",
		},
		{
			name:         "instance with numbers and hyphens",
			project:      "my-project-123",
			zone:         "asia-southeast1-c",
			instanceName: "app-server-001",
			expected:     "projects/my-project-123/zones/asia-southeast1-c/instances/app-server-001",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildVMResourceName(tc.project, tc.zone, tc.instanceName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildPreemptionLogFilter(t *testing.T) {
	testCases := []struct {
		name     string
		project  string
		vms      vm.List
		contains []string
	}{
		{
			name:    "single VM",
			project: "test-project",
			vms: vm.List{
				{Name: "vm1", Zone: "us-east1-b"},
			},
			contains: []string{
				`resource.type="gce_instance"`,
				`protoPayload.methodName="compute.instances.preempted"`,
				`protoPayload.resourceName="projects/test-project/zones/us-east1-b/instances/vm1"`,
			},
		},
		{
			name:    "multiple VMs",
			project: "prod-project",
			vms: vm.List{
				{Name: "web-1", Zone: "us-east1-b"},
				{Name: "web-2", Zone: "us-east1-c"},
			},
			contains: []string{
				`resource.type="gce_instance"`,
				`protoPayload.methodName="compute.instances.preempted"`,
				`protoPayload.resourceName="projects/prod-project/zones/us-east1-b/instances/web-1"`,
				`protoPayload.resourceName="projects/prod-project/zones/us-east1-c/instances/web-2"`,
				" OR ",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildPreemptionLogFilter(tc.project, tc.vms)

			for _, expectedSubstring := range tc.contains {
				assert.Contains(t, result, expectedSubstring)
			}
		})
	}
}

func TestBuildHostErrorLogFilter(t *testing.T) {
	testCases := []struct {
		name     string
		project  string
		vms      vm.List
		contains []string
	}{
		{
			name:    "single VM",
			project: "test-project",
			vms: vm.List{
				{Name: "vm1", Zone: "us-east1-b"},
			},
			contains: []string{
				`resource.type="gce_instance"`,
				`protoPayload.methodName="compute.instances.hostError"`,
				`logName="projects/test-project/logs/cloudaudit.googleapis.com%2Fsystem_event"`,
				`protoPayload.resourceName="projects/test-project/zones/us-east1-b/instances/vm1"`,
			},
		},
		{
			name:    "multiple VMs with different zones",
			project: "prod-project",
			vms: vm.List{
				{Name: "db-1", Zone: "europe-west2-a"},
				{Name: "db-2", Zone: "europe-west2-b"},
			},
			contains: []string{
				`resource.type="gce_instance"`,
				`protoPayload.methodName="compute.instances.hostError"`,
				`logName="projects/prod-project/logs/cloudaudit.googleapis.com%2Fsystem_event"`,
				`protoPayload.resourceName="projects/prod-project/zones/europe-west2-a/instances/db-1"`,
				`protoPayload.resourceName="projects/prod-project/zones/europe-west2-b/instances/db-2"`,
				" OR ",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := buildHostErrorLogFilter(tc.project, tc.vms)

			for _, expectedSubstring := range tc.contains {
				assert.Contains(t, result, expectedSubstring)
			}
		})
	}
}
