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
	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/gce/testutils"
	"github.com/cockroachdb/cockroach/pkg/roachprod/vm/local"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
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

	descriptors, err := c.discoverServices(context.Background(), "t1", ServiceTypeSQL, ServiceNodePredicate(c.Nodes...))
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

type serviceRegistryTest struct {
	test         *testing.T
	rng          *rand.Rand
	provider     vm.DNSProvider
	providerName string
	mu           struct {
		syncutil.Mutex
		// Port selection is run in parallel on multiple nodes at once.
		nextOpenPort int
	}
}

func newServiceRegistryTest(
	t *testing.T, rng *rand.Rand, provider vm.DNSProvider, providerName string,
) serviceRegistryTest {
	return serviceRegistryTest{
		test:         t,
		rng:          rng,
		provider:     provider,
		providerName: providerName,
		mu: struct {
			syncutil.Mutex
			nextOpenPort int
		}{
			Mutex:        syncutil.Mutex{},
			nextOpenPort: 500,
		},
	}
}

func (t *serviceRegistryTest) makeVM(clusterName string, i int) vm.VM {
	return vm.VM{
		Provider:    t.providerName,
		DNSProvider: t.providerName,
		PublicDNS:   fmt.Sprintf("%s.%s", vm.Name(clusterName, i), t.provider.Domain()),
	}
}

func (t *serviceRegistryTest) newCluster(nodeCount int) *SyncedCluster {
	clusterName := fmt.Sprintf("cluster-%d", t.rng.Uint32())
	c := &SyncedCluster{
		Cluster: cloud.Cluster{
			Name: clusterName,
		},
	}
	for i := 1; i <= nodeCount; i++ {
		c.VMs = append(c.VMs, t.makeVM(c.Name, i))
		c.Nodes = append(c.Nodes, Node(i))
	}
	return c
}

func (t *serviceRegistryTest) randomServiceType() ServiceType {
	serviceTypes := []ServiceType{ServiceTypeSQL, ServiceTypeUI}
	return serviceTypes[t.rng.Intn(len(serviceTypes))]
}

func (t *serviceRegistryTest) randomServiceMode() ServiceMode {
	serviceModes := []ServiceMode{ServiceModeExternal, ServiceModeShared}
	return serviceModes[t.rng.Intn(len(serviceModes))]
}

// randomStartTargets generates a random set of start targets for a cluster.
// It attempts to mimic potential ways a cockroachdb cluster can be deployed:
//  1. system-only: Only a single system tenant is started.
//  2. shared-process: A single system tenant and shared process virtual cluster is started.
//  3. separate-process: A system tenant is started, and multiple separate process virtual clusters
//     may be started externally.
func (t *serviceRegistryTest) randomStartTargets() []StartTarget {
	// We must always need to start the system tenant.
	targets := []StartTarget{StartDefault}
	deploymentModes := []StartTarget{StartDefault, StartServiceForVirtualCluster, StartSharedProcessForVirtualCluster}
	randomDeploymentMode := deploymentModes[t.rng.Intn(len(deploymentModes))]

	switch randomDeploymentMode {
	case StartDefault:
		// System only deployment, we already added StartDefault above.
	case StartSharedProcessForVirtualCluster:
		targets = append(targets, randomDeploymentMode)
	case StartServiceForVirtualCluster:
		// We can start multiple separate process clusters on a single cluster.
		numTenants := t.rng.Intn(5) + 1 // Randomly choose between 1 and 5 tenants.
		for range numTenants {
			targets = append(targets, StartServiceForVirtualCluster)
		}
	}

	return targets
}

// portFunc returns the next open port. This is a much simplified version of
// the actual portFunc used in real VMs. Notably it does not allow for the same
// port to be reused across multiple nodes/clusters. This is done so we don't have
// to keep track of a cluster to node mapping, which would overcomplicate things.
func (t *serviceRegistryTest) portFunc(
	ctx context.Context, l *logger.Logger, node Node, startPort, count int,
) ([]int, error) {
	ports := make([]int, count)
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := 0; i < count; i++ {
		ports[i] = t.mu.nextOpenPort + i
	}
	t.mu.nextOpenPort += count
	return ports, nil
}

// randomPorts returns a pair of random ports used for service registration.
func (t *serviceRegistryTest) randomPorts(target StartTarget, startPort int) (sql, adminUI int) {
	// When starting a virtual cluster, the user can choose to:
	//	1. Not specify any ports, in which case we use the default port selection method for the virtual cluster type.
	//	2. Specify custom ports, which should be used. Note that there is a special case for the system tenant when
	//	the custom ports are the same as the default ports. In this case we want to test that service registration
	//	handles it as if they were unspecified (i.e. skips service registration).
	type portSelectionMethod int
	const (
		UnspecifiedPorts portSelectionMethod = iota
		CustomPorts
		CustomDefaultPorts
	)

	methods := []portSelectionMethod{UnspecifiedPorts, CustomPorts, CustomDefaultPorts}
	method := methods[t.rng.Intn(len(methods))]
	switch method {
	case UnspecifiedPorts:
		return 0, 0
	case CustomPorts:
		return startPort, startPort + 1
	case CustomDefaultPorts:
		// This edge case only applies to the system tenant, so return the unspecified
		// case for other targets.
		if target == StartDefault {
			return config.DefaultSQLPort, config.DefaultAdminUIPort
		}
		return 0, 0
	default:
		t.test.Fatalf("unexpected port selection method: %d", method)
	}
	return 0, 0
}

func (t *serviceRegistryTest) virtualClusterName(startTarget StartTarget) string {
	switch startTarget {
	case StartDefault:
		return SystemInterfaceName
	case StartServiceForVirtualCluster:
		return fmt.Sprintf("separate-process-%s", randutil.RandString(t.rng, 10, randutil.PrintableKeyAlphabet))
	case StartSharedProcessForVirtualCluster:
		return fmt.Sprintf("shared-process-%s", randutil.RandString(t.rng, 10, randutil.PrintableKeyAlphabet))
	}
	return ""
}

// TestDuplicateServiceRegistration tests that registering the same services multiple times does not
// create duplicate DNS records. servicesWithOpenPortSelection should find the service is already
// registered and return 0 services to register.
func TestDuplicateServiceRegistration(t *testing.T) {
	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	dnsServer, dnsProvider, providerName := testutils.ProviderWithTestDNSServer(rng)

	srt := newServiceRegistryTest(t, rng, dnsProvider, providerName)

	// Create a cluster with 3 nodes.
	c := srt.newCluster(3)
	startOpts := StartOpts{
		Target:      StartDefault,
		AdminUIPort: 22222,
	}

	// Register services for the first time.
	err := c.maybeRegisterServices(ctx, nilLogger(), startOpts, srt.portFunc)
	require.NoError(t, err)

	// Expect only 2 create calls to the DNS server, one for the SQL service and one for the AdminUI service.
	require.Equal(t, 2, dnsServer.Metrics().CreateCalls)

	// Register services again, this time no new services should be created, the existing services should be used.
	err = c.maybeRegisterServices(ctx, nilLogger(), startOpts, srt.portFunc)
	require.NoError(t, err)

	// Expect the create call count to remain the same.
	require.Equal(t, 2, dnsServer.Metrics().CreateCalls)
}

func TestMultipleRegistrations(t *testing.T) {
	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	_, testDNS, providerName := testutils.ProviderWithTestDNSServer(rng)

	generator := func(values []reflect.Value, rng *rand.Rand) {
		srt := newServiceRegistryTest(t, rng, testDNS, providerName)

		nodeCount := rng.Intn(5) + 3
		c := srt.newCluster(nodeCount)

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
					ServiceType:        srt.randomServiceType(),
					ServiceMode:        srt.randomServiceMode(),
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

// TestServiceDescriptors tests that we are able to find service descriptors for
// both registered and unregistered services.
//
// The test repeatedly creates and registers cockroach clusters. A given cluster
// can contain just the system interface, or potentially multiple secondary tenants
// as well. It then attempts to retrieve service descriptors for each of the virtual
// clusters in a given cluster. We then cross verify based on the start options
// for each virtual cluster, that our service descriptors are as expected.
func TestServiceDescriptors(t *testing.T) {
	ctx := context.Background()
	rng, _ := randutil.NewTestRand()
	_, testDNS, providerName := testutils.ProviderWithTestDNSServer(rng)

	srt := newServiceRegistryTest(t, rng, testDNS, providerName)
	// We make some simplifications to the port selection logic for testing purposes.
	// We don't bother keeping track of which ports are used by which cluster/node
	// and instead just sequentially assign ports, e.g. the port 500 can be used once
	// by every machine in a cluster, but we don't allow that. One side effect of this
	// is that we will eventually run out of ports if we don't reset nextOpenPort
	// between clusters.
	resetPorts := func() {
		srt.mu.Lock()
		srt.mu.nextOpenPort = 500
		srt.mu.Unlock()
	}

	// One invocation of generator will register a cluster with a random number of
	// virtual clusters.
	generator := func(values []reflect.Value, rng *rand.Rand) {
		defer resetPorts()
		nodeCount := rng.Intn(5) + 1
		c := srt.newCluster(nodeCount)

		startTargets := srt.randomStartTargets()

		var opts []StartOpts

		startPort := config.DefaultOpenPortStart
		for _, target := range startTargets {
			sqlPort, adminUIPort := srt.randomPorts(target, startPort)
			startPort += 2
			startOpts := StartOpts{
				Target:             target,
				SQLPort:            sqlPort,
				AdminUIPort:        adminUIPort,
				VirtualClusterName: srt.virtualClusterName(target),
			}
			require.NoError(t, c.maybeRegisterServices(ctx, nilLogger(), startOpts, srt.portFunc))
			opts = append(opts, startOpts)
		}

		values[0] = reflect.ValueOf(c)
		values[1] = reflect.ValueOf(opts)
	}

	verify := func(c *SyncedCluster, startOpts []StartOpts) bool {
		var openPortsUsed int
		for _, opts := range startOpts {
			randomNode := Node(rng.Intn(len(c.Nodes)) + 1)
			desc, err := c.ServiceDescriptor(ctx, randomNode, opts.VirtualClusterName, ServiceTypeSQL, 0)
			require.NoError(t, err)
			require.Equal(t, opts.VirtualClusterName, desc.VirtualClusterName)
			require.Equal(t, ServiceTypeSQL, desc.ServiceType)
			require.Equal(t, randomNode, desc.Node)
			expectedServiceMode := ServiceModeShared
			if opts.Target == StartServiceForVirtualCluster {
				expectedServiceMode = ServiceModeExternal
			}
			require.Equal(t, expectedServiceMode, desc.ServiceMode)

			// Assert that the port is as expected.
			switch opts.Target {
			case StartDefault:
				switch opts.SQLPort {
				case 0:
					// System tenant with no custom ports should use the default SQL port.
					require.Equal(t, config.DefaultSQLPort, desc.Port)
				default:
					// If a custom port is specified, we expect it to be used.
					require.Equal(t, opts.SQLPort, desc.Port)
				}
			case StartServiceForVirtualCluster:
				switch opts.SQLPort {
				case 0:
					// Separate process nodes should use the open port selection logic. The port selection
					// logic is performed in parallel for each node, so we can at best assert that our port
					// is within a given range.
					expectedPortMin := 500 + openPortsUsed
					openPortsUsed += 2 * len(c.Nodes)
					expectedPortMax := 500 + openPortsUsed
					require.GreaterOrEqual(t, desc.Port, expectedPortMin)
					require.LessOrEqual(t, desc.Port, expectedPortMax)
				default:
					// If a custom port is specified, we expect it to be used.
					require.Equal(t, opts.SQLPort, desc.Port)
				}
			case StartSharedProcessForVirtualCluster:
				// A shared process virtual cluster resolves to the system tenant, so we have to cross
				// validate our expected port with the system tenant's start options.
				systemStartOpts := startOpts[0]
				switch systemStartOpts.SQLPort {
				case 0:
					require.Equal(t, config.DefaultSQLPort, desc.Port)
				default:
					require.Equal(t, systemStartOpts.SQLPort, desc.Port)
				}
			}
		}
		return true
	}

	// The TestDNSServer finds records by iterating over all records and checking
	// for any matches. Listing n records from a size m DNS server will take O(n*m) time,
	// so we limit the number of iterations to 1000.
	require.NoError(t, quick.Check(verify, &quick.Config{
		MaxCount: 1000,
		Rand:     rng,
		Values:   generator,
	}))
}
