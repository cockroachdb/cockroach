// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package install

import (
	"context"
	"net"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/stretchr/testify/require"
)

type testProvider struct {
	vm.Provider
	vm.DNSProvider
}

func TestServicePorts(t *testing.T) {
	ctx := context.Background()
	clusterName := "tc"
	z1NS := local.NewDNSProvider(t.TempDir(), "z1")
	vm.Providers["p1"] = &testProvider{DNSProvider: z1NS}
	z2NS := local.NewDNSProvider(t.TempDir(), "z2")
	vm.Providers["p2"] = &testProvider{DNSProvider: z2NS}

	err := z1NS.CreateRecords(ctx,
		vm.CreateSRVRecord(serviceDNSName(z1NS, "t1", ServiceTypeSQL, clusterName), net.SRV{
			Target: "host1.rp.",
			Port:   12345,
		}),
	)
	require.NoError(t, err)

	err = z2NS.CreateRecords(ctx,
		vm.CreateSRVRecord(serviceDNSName(z2NS, "t1", ServiceTypeSQL, clusterName), net.SRV{
			Target: "host1.rp.",
			Port:   12346,
		}),
	)
	require.NoError(t, err)

	c := &SyncedCluster{
		Cluster: cloud.Cluster{
			Name: clusterName,
			VMs: vm.List{
				vm.VM{
					Provider:    "p1",
					DNSProvider: "p1",
					PublicDNS:   "host1.rp",
				},
				vm.VM{
					Provider:    "p2",
					DNSProvider: "p2",
					PublicDNS:   "host2.rp",
				},
			},
		},
		Nodes: allNodes(2),
	}

	descriptors, err := c.DiscoverServices(context.Background(), "t1", ServiceTypeSQL, ServiceNodePredicate(c.Nodes...))
	sort.Slice(descriptors, func(i, j int) bool {
		return descriptors[i].Port < descriptors[j].Port
	})
	require.NoError(t, err)
	require.Len(t, descriptors, 2)
	require.Equal(t, 12345, descriptors[0].Port)
	require.Equal(t, 12346, descriptors[1].Port)
}

func TestStringToIntegers(t *testing.T) {
	integers, err := stringToIntegers(" 20    333 4 5\n 89\n\n")
	require.NoError(t, err)
	require.Equal(t, []int{20, 333, 4, 5, 89}, integers)
}

func TestServiceNameComponents(t *testing.T) {
	z := local.NewDNSProvider(t.TempDir(), "z1")
	dnsName := serviceDNSName(z, "tenant-100", ServiceTypeSQL, "test-cluster")
	tenantName, serviceType, err := serviceNameComponents(dnsName)
	require.NoError(t, err)
	require.Equal(t, "tenant-100", tenantName)
	require.Equal(t, ServiceTypeSQL, serviceType)
}
