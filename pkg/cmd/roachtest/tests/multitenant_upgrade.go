// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tests

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/mixedversion"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/task"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

func registerMultiTenantUpgrade(r registry.Registry) {
	r.Add(registry.TestSpec{
		Name:             "multitenant-upgrade",
		Timeout:          5 * time.Hour,
		Cluster:          r.MakeClusterSpec(7),
		CompatibleClouds: registry.CloudsWithServiceRegistration,
		Suites:           registry.Suites(registry.MixedVersion, registry.Nightly),
		Owner:            registry.OwnerServer,
		Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
			runMultitenantUpgrade(ctx, t, c)
		},
	})
}

// tenantUpgradeStatus encapsulates data about each tenant created
// during `multitenant-upgrade`.
type tenantUpgradeStatus struct {
	name    string
	nodes   option.NodeListOption
	running bool
}

func (tus tenantUpgradeStatus) pgurl() string {
	return fmt.Sprintf("{pgurl%s:%s}", tus.nodes, tus.name)
}

type tenantUpgradeStatuses []*tenantUpgradeStatus

func (ts tenantUpgradeStatuses) String() string {
	var tenantLines []string
	for _, t := range ts {
		tenantLines = append(tenantLines, fmt.Sprintf("- %s, deployed on nodes %v", t.name, t.nodes))
	}

	return fmt.Sprintf("%d tenants:\n%s", len(ts), strings.Join(tenantLines, "\n"))
}

// runMultitenantUpgrade exercises upgrades when there are multiple
// separate-process tenants being upgraded at the same time. We start
// a random number of tenants when the storage cluster may be in
// mixed-version then, once the storage cluster finalizes the
// uprgade, we upgrade every tenant in parallel. The test also runs
// TPCC on the tenants for some time to make sure they continue to
// serve traffic without problems.
func runMultitenantUpgrade(ctx context.Context, t test.Test, c cluster.Cluster) {
	const (
		numStorageNodes           = 4
		numWarehouses             = 10
		minTenants, maxTenants    = 3, 5
		startOnStartupProbability = 0.5
	)

	var (
		storageNodes = c.Range(1, numStorageNodes)
		tenantNodes  = c.Range(numStorageNodes+1, len(c.CRDBNodes()))
		tpccDuration = 5 * time.Minute
	)

	mvt := mixedversion.NewTest(ctx, t, t.L(), c, storageNodes,
		// 23.1 releases have a frequent "use of Span after finish" bug
		// in separate-process tenants. Avoid hitting that.
		mixedversion.MinimumSupportedVersion("v23.2.0"),
		// We only upgrade the system. Creation and upgrades of tenants is
		// managed by the test.
		mixedversion.EnabledDeploymentModes(mixedversion.SystemOnlyDeployment),
		// Closer to a Serverless deployment.
		mixedversion.AlwaysUseLatestPredecessors,
	)

	tenants := makeTenants(mvt.RNG(), minTenants, maxTenants, tenantNodes)
	t.L().Printf("%s", tenants)

	// startTenant starts the SQL server process for the given `tenant`
	// on the passed version. If `initWorkload` is true, we also
	// initialize the `tpcc` workload on the tenant.
	startTenant := func(
		ctx context.Context, l *logger.Logger, tenant *tenantUpgradeStatus, v *clusterupgrade.Version, initWorkload bool,
	) error {
		l.Printf("starting %s on version %s", tenant.name, v)
		startOpts := option.StartVirtualClusterOpts(
			tenant.name, tenant.nodes, option.NoBackupSchedule,
		)
		binaryPath, err := clusterupgrade.UploadCockroach(ctx, t, l, c, tenant.nodes, v)
		if err != nil {
			return errors.Wrapf(err, "uploading cockroach %s", v)
		}

		if err := c.StartServiceForVirtualClusterE(
			ctx, l, startOpts, install.MakeClusterSettings(install.BinaryOption(binaryPath)),
		); err != nil {
			return err
		}

		tenant.running = true

		if initWorkload {
			l.Printf("initializing tpcc on %s", tenant.name)
			cmd := fmt.Sprintf(
				"%s workload init tpcc --warehouses %d %s",
				binaryPath, numWarehouses, tenant.pgurl(),
			)
			return c.RunE(ctx, option.WithNodes(c.Node(tenant.nodes[0])), cmd)
		}

		return nil
	}

	// stopTenant performs a graceful stop of the SQL server process of
	// every node where it is running.
	stopTenant := func(
		ctx context.Context, l *logger.Logger, tenant *tenantUpgradeStatus,
	) error {
		l.Printf("stopping %s", tenant.name)
		stopOpts := option.StopVirtualClusterOpts(
			tenant.name, tenant.nodes, option.Graceful(shutdownGracePeriod),
		)
		return c.StopServiceForVirtualClusterE(ctx, l, stopOpts)
	}

	// forEachTenant is a helper function that runs the function `fn` in
	// parallel for every tenant created for this test. Returns a
	// channel that is closed once `fn` finishes on every tenant.
	forEachTenant := func(
		desc string,
		ctx context.Context,
		h *mixedversion.Helper,
		fn func(context.Context, *logger.Logger, *tenantUpgradeStatus) error,
	) chan struct{} {
		var wg sync.WaitGroup
		wg.Add(len(tenants))

		for _, tenant := range tenants {
			h.Go(func(ctx context.Context, l *logger.Logger) error {
				defer wg.Done()
				return fn(ctx, l, tenant)
			}, task.Name(fmt.Sprintf("%s: %s", desc, tenant.name)))
		}

		returnCh := make(chan struct{})
		h.Go(func(context.Context, *logger.Logger) error {
			wg.Wait()
			close(returnCh)
			return nil
		})

		return returnCh
	}

	// runTPCC runs a light TPCC workload on every tenant in
	// parallel. The returned channel is closed once the workload
	// finishes running on every tenant.
	runTPCC := func(
		ctx context.Context, c cluster.Cluster, binaryPath string, h *mixedversion.Helper,
	) chan struct{} {
		return forEachTenant(
			"run tpcc",
			ctx,
			h,
			func(ctx context.Context, l *logger.Logger, tenant *tenantUpgradeStatus) error {
				cmd := fmt.Sprintf(
					"%s workload run tpcc --warehouses %d --duration %s %s",
					binaryPath, numWarehouses, tpccDuration, tenant.pgurl(),
				)

				return c.RunE(ctx, option.WithNodes(c.Node(tenant.nodes[0])), cmd)
			},
		)
	}

	// After the cluster starts up, we start *some* of the tenants we
	// will create for this test. The remaining tenants will be started
	// in mixed version.
	mvt.OnStartup(
		"maybe create some tenants",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			for _, tenant := range tenants {
				if rng.Float64() < startOnStartupProbability {
					t.L().Printf("starting %s", tenant.name)
					if err := startTenant(ctx, l, tenant, h.Context().FromVersion, true); err != nil {
						return err
					}
				}
			}

			return nil
		},
	)

	// When the storage cluster is in mixed-version, we start the
	// tenants for which we didn't create a SQL server yet, and then run
	// a TPCC workload on them.
	mvt.InMixedVersion(
		"run workload on tenants",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			for _, tenant := range tenants {
				if !tenant.running {
					if h.IsFinalizing() {
						// If the upgrading service is finalizing, we need to stage the tenants with the upgraded
						// binary to allow the tenant to start successfully.
						if err := startTenant(ctx, l, tenant, h.Context().ToVersion, true); err != nil {
							return err
						}
					} else {
						// For all other upgrade stages, we can stage the tenant with the previous binary version i.e.
						// the version from which the system tenant is being upgraded. This tests the scenario that
						// in a mixed version state, tenants on the previous version can continue to connect
						// to the cluster.
						if err := startTenant(ctx, l, tenant, h.Context().FromVersion, true); err != nil {
							return err
						}
					}
				}
			}

			binaryPath := clusterupgrade.BinaryPathForVersion(t, h.Context().FromVersion, "cockroach")
			l.Printf("waiting for tpcc to run on tenants...")
			<-runTPCC(ctx, c, binaryPath, h)
			return nil
		},
	)

	// Finally, once the storage cluster is upgraded, we perform a
	// rolling upgrade on every tenant, and run a TPCC workload while
	// the migrations are running on the tenant.
	mvt.AfterUpgradeFinalized(
		"upgrade tenants",
		func(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *mixedversion.Helper) error {
			for _, tenant := range tenants {
				if err := stopTenant(ctx, l, tenant); err != nil {
					return err
				}

				if err := startTenant(ctx, l, tenant, h.Context().ToVersion, false); err != nil {
					return err
				}
			}

			binaryPath := clusterupgrade.BinaryPathForVersion(t, h.Context().ToVersion, "cockroach")
			tpccFinished := runTPCC(ctx, c, binaryPath, h)

			upgradeFinished := forEachTenant(
				"finalize upgrade",
				ctx,
				h,
				func(ctx context.Context, l *logger.Logger, tenant *tenantUpgradeStatus) error {
					n := tenant.nodes[0]
					tenantDB, err := c.ConnE(ctx, l, n, option.VirtualClusterName(tenant.name))
					if err != nil {
						return errors.Wrapf(err, "failed to connect to %s on n%d", tenant.name, n)
					}
					defer tenantDB.Close()

					binaryVersion, err := clusterupgrade.BinaryVersion(ctx, tenantDB)
					if err != nil {
						return errors.Wrapf(err, "failed to query binary version for %s on n%d", tenant.name, n)
					}

					l.Printf("finalizing upgrade to '%s'", binaryVersion)
					_, err = tenantDB.Exec("SET CLUSTER SETTING version = $1", binaryVersion.String())

					l.Printf("upgrade finalized on %s", tenant.name)
					return err
				},
			)

			g := h.NewGroup()
			g.Go(func(_ context.Context, l *logger.Logger) error {
				<-tpccFinished
				l.Printf("tpcc workload finished running on tenants")
				return nil
			})
			g.Go(func(_ context.Context, l *logger.Logger) error {
				<-upgradeFinished
				l.Printf("tenant upgrades finished")
				return nil
			})
			g.Wait()

			return nil
		},
	)

	mvt.Run()
}

// makeTenants creates a list of tenants to run on the given
// `tenantNodes`. The number of tenants to be created, and the number
// of SQL servers to create for each are randomized.
func makeTenants(
	rng *rand.Rand, minTenants, maxTenants int, tenantNodes option.NodeListOption,
) tenantUpgradeStatuses {
	numTenants := rng.Intn(maxTenants-minTenants+1) + minTenants
	tenants := make(tenantUpgradeStatuses, numTenants)

	for j := 0; j < numTenants; j++ {
		numNodes := rng.Intn(len(tenantNodes)) + 1
		rng.Shuffle(len(tenantNodes), func(i, j int) {
			tenantNodes[i], tenantNodes[j] = tenantNodes[j], tenantNodes[i]
		})

		tenants[j] = &tenantUpgradeStatus{
			name:  fmt.Sprintf("tenant-%s", string('a'+rune(j))),
			nodes: append(option.NodeListOption{}, tenantNodes[:numNodes]...),
		}
	}

	return tenants
}
