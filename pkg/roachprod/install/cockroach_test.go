// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"testing"

	cloudcluster "github.com/cockroachdb/cockroach/pkg/roachprod/cloud/types"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/stretchr/testify/require"
)

func TestVirtualClusterLabel(t *testing.T) {
	testCases := []struct {
		name               string
		virtualClusterName string
		sqlInstance        int
		expectedLabel      string
	}{
		{
			name:               "empty virtual cluster name",
			virtualClusterName: "",
			expectedLabel:      "cockroach-system",
		},
		{
			name:               "system interface name",
			virtualClusterName: "system",
			expectedLabel:      "cockroach-system",
		},
		{
			name:               "simple virtual cluster name",
			virtualClusterName: "a",
			sqlInstance:        1,
			expectedLabel:      "cockroach-a_1",
		},
		{
			name:               "virtual cluster name with hyphens",
			virtualClusterName: "virtual-cluster-a-1",
			sqlInstance:        1,
			expectedLabel:      "cockroach-virtual-cluster-a-1_1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			label := VirtualClusterLabel(tc.virtualClusterName, tc.sqlInstance)
			require.Equal(t, tc.expectedLabel, label)

			nameFromLabel, instanceFromLabel, err := VirtualClusterInfoFromLabel(label)
			require.NoError(t, err)

			expectedVirtualClusterName := tc.virtualClusterName
			if tc.virtualClusterName == "" {
				expectedVirtualClusterName = "system"
			}
			require.Equal(t, expectedVirtualClusterName, nameFromLabel)

			require.Equal(t, tc.sqlInstance, instanceFromLabel)
		})
	}
}

func TestShouldAdvertisePublicIP(t *testing.T) {
	testCases := []struct {
		name     string
		vms      vm.List
		expected bool
	}{
		{
			name: "same VPC uses private IPs",
			vms: vm.List{
				{PublicIP: "1.2.3.4", PrivateIP: "10.0.0.1", VPC: "vpc-1"},
				{PublicIP: "1.2.3.5", PrivateIP: "10.0.0.2", VPC: "vpc-1"},
				{PublicIP: "1.2.3.6", PrivateIP: "10.0.0.3", VPC: "vpc-1"},
			},
			expected: false,
		},
		{
			name: "different VPCs uses public IPs",
			vms: vm.List{
				{PublicIP: "1.2.3.4", PrivateIP: "10.0.0.1", VPC: "vpc-1"},
				{PublicIP: "1.2.3.5", PrivateIP: "10.0.0.2", VPC: "vpc-2"},
			},
			expected: true,
		},
		{
			name: "single node uses private IP",
			vms: vm.List{
				{PublicIP: "1.2.3.4", PrivateIP: "10.0.0.1", VPC: "vpc-1"},
			},
			expected: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &SyncedCluster{
				Cluster: cloudcluster.Cluster{
					Name: "test",
					VMs:  tc.vms,
				},
				Nodes: allNodes(len(tc.vms)),
			}
			require.Equal(t, tc.expected, c.shouldAdvertisePublicIP())
		})
	}
}

func TestJoinAddressConsistentWithAdvertiseAddr(t *testing.T) {
	testCases := []struct {
		name             string
		vms              vm.List
		expectedJoinHost string
	}{
		{
			name: "same VPC joins on private IPs",
			vms: vm.List{
				{PublicIP: "1.2.3.4", PrivateIP: "10.0.0.1", VPC: "vpc-1"},
				{PublicIP: "1.2.3.5", PrivateIP: "10.0.0.2", VPC: "vpc-1"},
				{PublicIP: "1.2.3.6", PrivateIP: "10.0.0.3", VPC: "vpc-1"},
			},
			expectedJoinHost: "10.0.0.1",
		},
		{
			name: "different VPCs joins on public IPs",
			vms: vm.List{
				{PublicIP: "1.2.3.4", PrivateIP: "10.0.0.1", VPC: "vpc-1"},
				{PublicIP: "1.2.3.5", PrivateIP: "10.0.0.2", VPC: "vpc-2"},
			},
			expectedJoinHost: "1.2.3.4",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := &SyncedCluster{
				Cluster: cloudcluster.Cluster{
					Name: "test",
					VMs:  tc.vms,
				},
				Nodes: allNodes(len(tc.vms)),
			}
			joinNode := Node(1)
			var joinHost string
			if c.shouldAdvertisePublicIP() {
				joinHost = c.Host(joinNode)
			} else {
				joinHost = c.VMs[joinNode-1].PrivateIP
			}
			require.Equal(t, tc.expectedJoinHost, joinHost)

			var advertiseHost string
			if c.shouldAdvertisePublicIP() {
				advertiseHost = c.Host(joinNode)
			} else {
				advertiseHost = c.VMs[joinNode-1].PrivateIP
			}
			require.Equal(t, advertiseHost, joinHost,
				"join host should be consistent with advertise host")
		})
	}
}
