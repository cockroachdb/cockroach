// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/assert"
)

func TestCluster_IsProvisioningManaged(t *testing.T) {
	tests := []struct {
		name    string
		cluster Cluster
		want    bool
	}{{
		name: "field true, no VMs",
		cluster: Cluster{
			Name:                  "test-cluster",
			ManagedByProvisioning: true,
		},
		want: true,
	}, {
		name: "field false, VM has provisioning_identifier label",
		cluster: Cluster{
			Name: "test-cluster",
			VMs: vm.List{
				{Name: "vm-1", Labels: map[string]string{
					vm.TagProvisioningIdentifier: "abc123",
				}},
			},
		},
		want: true,
	}, {
		name: "field false, no VMs",
		cluster: Cluster{
			Name: "test-cluster",
		},
		want: false,
	}, {
		name: "field false, VMs without provisioning_identifier",
		cluster: Cluster{
			Name: "test-cluster",
			VMs: vm.List{
				{Name: "vm-1", Labels: map[string]string{
					"roachprod": "true",
				}},
				{Name: "vm-2", Labels: map[string]string{}},
			},
		},
		want: false,
	}, {
		name: "field false, VM with empty provisioning_identifier",
		cluster: Cluster{
			Name: "test-cluster",
			VMs: vm.List{
				{Name: "vm-1", Labels: map[string]string{
					vm.TagProvisioningIdentifier: "",
				}},
			},
		},
		want: false,
	}, {
		name: "field false, one VM has label and one does not",
		cluster: Cluster{
			Name: "test-cluster",
			VMs: vm.List{
				{Name: "vm-1", Labels: map[string]string{}},
				{Name: "vm-2", Labels: map[string]string{
					vm.TagProvisioningIdentifier: "abc123",
				}},
			},
		},
		want: true,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.cluster.IsProvisioningManaged()
			assert.Equal(t, tt.want, got)
		})
	}
}
