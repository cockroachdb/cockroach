// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"reflect"
	"sort"
	"testing"
	"testing/quick"

	"github.com/cockroachdb/cockroach/pkg/roachprod/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce/testutils"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
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
	virtualClusterName, serviceType, err := serviceNameComponents(dnsName)
	require.NoError(t, err)
	require.Equal(t, "tenant-100", virtualClusterName)
	require.Equal(t, ServiceTypeSQL, serviceType)
}

func TestMaybeRegisterServices(t *testing.T) {
	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	dnsServer, dnsProvider, providerName := testutils.ProviderWithTestDNSServer(rng)

	// Create a cluster with 3 nodes.
	makeVM := func(clusterName string, i int) vm.VM {
		return vm.VM{
			Provider:    providerName,
			DNSProvider: providerName,
			PublicDNS:   fmt.Sprintf("%s.%s", vm.Name(clusterName, i), dnsProvider.Domain()),
		}
	}
	makeCluster := func() *SyncedCluster {
		clusterName := fmt.Sprintf("cluster-%d", rng.Uint32())
		c := &SyncedCluster{
			Cluster: cloud.Cluster{
				Name: clusterName,
			},
		}
		nodeCount := 3
		for i := 1; i <= nodeCount; i++ {
			c.VMs = append(c.VMs, makeVM(c.Name, i))
			c.Nodes = append(c.Nodes, Node(i))
		}
		return c
	}

	// Create a portFunc that returns a list of ports starting from 500.
	portFunc := func(ctx context.Context, l *logger.Logger, node Node, startPort, count int) ([]int, error) {
		ports := make([]int, count)
		for i := 0; i < count; i++ {
			ports[i] = 500 + i
		}
		return ports, nil
	}

	c := makeCluster()
	startOpts := StartOpts{
		Target:      StartDefault,
		AdminUIPort: 22222,
	}

	// Register services for the first time.
	err := c.maybeRegisterServices(ctx, nilLogger(), startOpts, portFunc)
	require.NoError(t, err)

	// Expect only 2 create calls to the DNS server, one for the SQL service and one for the AdminUI service.
	require.Equal(t, 2, dnsServer.Metrics().CreateCalls)

	// Register services again, this time no new services should be created, the existing services should be used.
	err = c.maybeRegisterServices(ctx, nilLogger(), startOpts, portFunc)
	require.NoError(t, err)

	// Expect the create call count to remain the same.
	require.Equal(t, 2, dnsServer.Metrics().CreateCalls)
}

func TestMultipleRegistrations(t *testing.T) {
	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	_, testDNS, providerName := testutils.ProviderWithTestDNSServer(rng)

	generator := func(values []reflect.Value, rng *rand.Rand) {
		makeVM := func(clusterName string, i int) vm.VM {
			return vm.VM{
				Provider:    providerName,
				DNSProvider: providerName,
				PublicDNS:   fmt.Sprintf("%s.%s", vm.Name(clusterName, i), testDNS.Domain()),
			}
		}
		clusterName := fmt.Sprintf("cluster-%d", rng.Uint32())
		c := &SyncedCluster{
			Cluster: cloud.Cluster{
				Name: clusterName,
			},
		}
		nodeCount := rng.Intn(5) + 3
		for i := 1; i <= nodeCount; i++ {
			c.VMs = append(c.VMs, makeVM(c.Name, i))
			c.Nodes = append(c.Nodes, Node(i))
		}
		randomServiceType := func(rng *rand.Rand) ServiceType {
			serviceTypes := []ServiceType{ServiceTypeSQL, ServiceTypeUI}
			return serviceTypes[rng.Intn(len(serviceTypes))]
		}
		randomServiceMode := func(rng *rand.Rand) ServiceMode {
			serviceModes := []ServiceMode{ServiceModeExternal, ServiceModeShared}
			return serviceModes[rng.Intn(len(serviceModes))]
		}
		randomServiceName := func(rng *rand.Rand) string {
			return fmt.Sprintf("service-%d", rng.Intn(5))
		}

		deleteCount := rng.Intn(3) + 1
		deletePoints := make(map[int]struct{})
		for len(deletePoints) < deleteCount {
			deletePoints[rng.Intn(nodeCount)] = struct{}{}
		}

		servicesToRegister := make([][]ServiceDesc, rng.Intn(50)+10)
		for j := 0; j < len(servicesToRegister); j++ {
			if _, ok := deletePoints[j]; ok {
				// An empty slice means to delete all DNS records.
				continue
			}
			serviceName := randomServiceName(rng)
			groupSize := rng.Intn(20) + 3
			for i := 0; i < groupSize; i++ {
				// Register a random number of services.
				// Duplicate services are allowed.
				servicesToRegister[j] = append(servicesToRegister[j], ServiceDesc{
					VirtualClusterName: serviceName,
					ServiceType:        randomServiceType(rng),
					ServiceMode:        randomServiceMode(rng),
					Node:               Node(rng.Intn(len(c.Nodes)) + 1),
					Port:               500 + rng.Intn(5),
					Instance:           rng.Intn(2),
				})
			}
		}
		values[0] = reflect.ValueOf(c)
		values[1] = reflect.ValueOf(servicesToRegister)
	}

	// Verify no errors occur when registering multiple services.
	verify := func(c *SyncedCluster, servicesToRegister [][]ServiceDesc) bool {
		for _, services := range servicesToRegister {
			if len(services) == 0 {
				err := testDNS.DeleteRecordsBySubdomain(ctx, c.Name)
				require.NoError(t, err)
				continue
			}
			err := c.RegisterServices(ctx, services)
			require.NoError(t, err)
		}
		return true
	}

	require.NoError(t, quick.Check(verify, &quick.Config{
		MaxCount: 150,
		Rand:     rng,
		Values:   generator,
	}))
}
