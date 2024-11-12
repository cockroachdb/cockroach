// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
)

// installFixturesStep is the step that copies the fixtures from
// `pkg/cmd/roachtest/fixtures` for a specific version into the nodes'
// store dir.
type installFixturesStep struct {
	version *clusterupgrade.Version
}

func (s installFixturesStep) Background() shouldStop { return nil }

func (s installFixturesStep) Description() string {
	return fmt.Sprintf("install fixtures for version %q", s.version.String())
}

func (s installFixturesStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	return clusterupgrade.InstallFixtures(
		ctx, l, h.runner.cluster, h.System.Descriptor.Nodes, s.version,
	)
}

// startStep is the step that starts the cluster from a specific
// `version`.
type startStep struct {
	rt         test.Test
	version    *clusterupgrade.Version
	initTarget int
	settings   []install.ClusterSettingOption
}

func (s startStep) Background() shouldStop { return nil }

func (s startStep) Description() string {
	return fmt.Sprintf("start cluster at version %q", s.version)
}

// Run uploads the binary associated with the given version and starts
// the cockroach binary on the nodes.
func (s startStep) Run(ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper) error {
	systemNodes := h.System.Descriptor.Nodes
	binaryPath, err := clusterupgrade.UploadCockroach(
		ctx, s.rt, l, h.runner.cluster, systemNodes, s.version,
	)
	if err != nil {
		return err
	}

	clusterSettings := append(
		append([]install.ClusterSettingOption{}, s.settings...),
		install.BinaryOption(binaryPath),
	)

	opts := startOpts(option.WithInitTarget(s.initTarget))
	return clusterupgrade.StartWithSettings(
		ctx, l, h.runner.cluster, systemNodes, opts, clusterSettings...,
	)
}

// startSharedProcessVirtualCluster step creates a new shared-process
// virtual cluster with the given name, and starts it. At the end of
// this step, the virtual cluster should be ready to receive requests.
type startSharedProcessVirtualClusterStep struct {
	name       string
	initTarget int
	settings   []install.ClusterSettingOption
}

func (s startSharedProcessVirtualClusterStep) Background() shouldStop { return nil }

func (s startSharedProcessVirtualClusterStep) Description() string {
	return fmt.Sprintf("start shared-process tenant %q", s.name)
}

func (s startSharedProcessVirtualClusterStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	l.Printf("starting shared process virtual cluster %s", s.name)
	startOpts := option.StartSharedVirtualClusterOpts(s.name, startStopOpts(option.WithInitTarget(s.initTarget))...)

	if err := h.runner.cluster.StartServiceForVirtualClusterE(
		ctx, l, startOpts, install.MakeClusterSettings(s.settings...),
	); err != nil {
		return err
	}

	// When we first start the shared-process on the cluster, we wait
	// until we are able to connect to the tenant on every node before
	// moving on. The test runner infrastructure relies on that ability.
	return waitForSharedProcess(ctx, l, h, h.Tenant.Descriptor.Nodes)
}

// waitForStableClusterVersionStep implements the process of waiting
// for the `version` cluster setting being the same on all nodes of
// the cluster and equal to the binary version of the first node in
// the `nodes` field.
type waitForStableClusterVersionStep struct {
	nodes              option.NodeListOption
	desiredVersion     string
	timeout            time.Duration
	virtualClusterName string
}

func (s waitForStableClusterVersionStep) Background() shouldStop { return nil }

func (s waitForStableClusterVersionStep) Description() string {
	return fmt.Sprintf(
		"wait for %s tenant on nodes %v to reach cluster version %s",
		s.virtualClusterName, s.nodes, quoteVersionForPresentation(s.desiredVersion),
	)
}

func (s waitForStableClusterVersionStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	return clusterupgrade.WaitForClusterUpgrade(
		ctx, l, s.nodes, serviceByName(h, s.virtualClusterName).Connect, s.timeout,
	)
}

// preserveDowngradeOptionStep sets the `preserve_downgrade_option`
// cluster setting to the binary version running in a random node in
// the cluster.
type preserveDowngradeOptionStep struct {
	virtualClusterName string
}

func (s preserveDowngradeOptionStep) Background() shouldStop { return nil }

func (s preserveDowngradeOptionStep) Description() string {
	return fmt.Sprintf(
		"prevent auto-upgrades on %s tenant by setting `preserve_downgrade_option`",
		s.virtualClusterName,
	)
}

func (s preserveDowngradeOptionStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	service := serviceByName(h, s.virtualClusterName)
	node, db := service.RandomDB(rng)
	l.Printf("checking binary version (via node %d)", node)
	bv, err := clusterupgrade.BinaryVersion(ctx, db)
	if err != nil {
		return err
	}

	return service.Exec(rng, "SET CLUSTER SETTING cluster.preserve_downgrade_option = $1", bv.String())
}

// restartWithNewBinaryStep restarts a certain `node` with a new
// cockroach binary. Any existing `cockroach` process will be stopped,
// then the new binary will be uploaded and the `cockroach` process
// will restart using the new binary.
type restartWithNewBinaryStep struct {
	version              *clusterupgrade.Version
	rt                   test.Test
	node                 int
	settings             []install.ClusterSettingOption
	initTarget           int
	sharedProcessStarted bool
}

func (s restartWithNewBinaryStep) Background() shouldStop { return nil }

func (s restartWithNewBinaryStep) Description() string {
	return fmt.Sprintf("restart node %d with binary version %s", s.node, s.version.String())
}

func (s restartWithNewBinaryStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	h.ExpectDeath()
	if err := clusterupgrade.RestartNodesWithNewBinary(
		ctx,
		s.rt,
		l,
		h.runner.cluster,
		h.runner.cluster.Node(s.node),
		startOpts(option.WithInitTarget(s.initTarget)),
		s.version,
		s.settings...,
	); err != nil {
		return err
	}

	if s.sharedProcessStarted {
		// If we are in shared-process mode and the tenant is already
		// running at this point, we wait for the server on the restarted
		// node to be up before moving on.
		return waitForSharedProcess(ctx, l, h, h.runner.cluster.Node(s.node))
	}

	return nil
}

// allowUpgradeStep resets the `preserve_downgrade_option` cluster
// setting, allowing the upgrade migrations to run and the cluster
// version to eventually reach the binary version on the nodes.
type allowUpgradeStep struct {
	virtualClusterName string
}

func (s allowUpgradeStep) Background() shouldStop { return nil }

func (s allowUpgradeStep) Description() string {
	return fmt.Sprintf(
		"allow upgrade to happen on %s tenant by resetting `preserve_downgrade_option`",
		s.virtualClusterName,
	)
}

func (s allowUpgradeStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	return serviceByName(h, s.virtualClusterName).Exec(
		rng, "RESET CLUSTER SETTING cluster.preserve_downgrade_option",
	)
}

// setTenantClusterVersionStep will `set` the `version` setting on the
// tenant with the associated name. Used in older versions where
// auto-upgrading does not work reliably; in those cases, we block
// and wait for the migrations to run before proceeding.
type setTenantClusterVersionStep struct {
	nodes              option.NodeListOption
	targetVersion      string
	virtualClusterName string
}

func (s setTenantClusterVersionStep) Background() shouldStop { return nil }

func (s setTenantClusterVersionStep) Description() string {
	return fmt.Sprintf(
		"run upgrades on tenant %s by explicitly setting cluster version to %s",
		s.virtualClusterName, quoteVersionForPresentation(s.targetVersion),
	)
}

func (s setTenantClusterVersionStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	node, db := h.RandomDB(rng)

	targetVersion := s.targetVersion
	if s.targetVersion == clusterupgrade.CurrentVersionString {
		l.Printf("querying binary version on node %d", node)
		bv, err := clusterupgrade.BinaryVersion(ctx, db)
		if err != nil {
			return err
		}

		targetVersion = fmt.Sprintf("%d.%d", bv.Major, bv.Minor)
		if !bv.IsFinal() {
			targetVersion += fmt.Sprintf("-%d", bv.Internal)
		}
	}

	l.Printf(
		"setting version on tenant %s to %s via node %d",
		s.virtualClusterName, targetVersion, node,
	)

	stmt := fmt.Sprintf("SET CLUSTER SETTING version = '%s'", targetVersion)
	return serviceByName(h, s.virtualClusterName).ExecWithGateway(
		rng, h.runner.cluster.Node(node), stmt,
	)
}

// waitStep does nothing but sleep for the provided duration. Most
// commonly used to allow the cluster to stay in a certain state
// before attempting node restarts or other upgrade events.
type waitStep struct {
	dur time.Duration
}

func (s waitStep) Background() shouldStop { return nil }

func (s waitStep) Description() string {
	return fmt.Sprintf("wait for %s", s.dur)
}

func (s waitStep) Run(ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper) error {
	l.Printf("waiting for %s", s.dur)
	select {
	case <-time.After(s.dur):
	case <-ctx.Done():
	}

	return nil
}

// runHookStep is a step used to run a user-provided hook (i.e.,
// callbacks passed to `OnStartup`, `InMixedVersion`, or `AfterTest`).
type runHookStep struct {
	hook     versionUpgradeHook
	stopChan shouldStop
}

func (s runHookStep) Background() shouldStop { return s.stopChan }

func (s runHookStep) Description() string {
	return fmt.Sprintf("run %q", s.hook.name)
}

func (s runHookStep) Run(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {
	return s.hook.fn(ctx, l, rng, h)
}

// setClusterSettingStep sets the cluster setting `name` to `value`.
type setClusterSettingStep struct {
	minVersion         *clusterupgrade.Version
	name               string
	value              interface{}
	virtualClusterName string
}

func (s setClusterSettingStep) Background() shouldStop { return nil }

func (s setClusterSettingStep) Description() string {
	return fmt.Sprintf(
		"set cluster setting %q to '%v' on %s tenant",
		s.name, s.value, s.virtualClusterName,
	)
}

func (s setClusterSettingStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	var stmt string
	var args []interface{}
	// We do a type switch on common types to avoid errors when using
	// placeholders, as type detection is often not implemented for some
	// private cluster settings.
	switch val := s.value.(type) {
	case string:
		stmt = fmt.Sprintf("SET CLUSTER SETTING %s = '%s'", s.name, val)
	case bool:
		stmt = fmt.Sprintf("SET CLUSTER SETTING %s = %t", s.name, val)
	case int:
		stmt = fmt.Sprintf("SET CLUSTER SETTING %s = %d", s.name, val)
	default:
		// If not using any of these types, do a best-effort attempt using
		// a placeholder.
		stmt = fmt.Sprintf("SET CLUSTER SETTING %s = $1", s.name)
		args = []interface{}{val}
	}

	return serviceByName(h, s.virtualClusterName).ExecWithGateway(
		rng, nodesRunningAtLeast(s.virtualClusterName, s.minVersion, h), stmt, args...,
	)
}

// resetClusterSetting resets cluster setting `name`.
type resetClusterSettingStep struct {
	minVersion         *clusterupgrade.Version
	name               string
	virtualClusterName string
}

func (s resetClusterSettingStep) Background() shouldStop { return nil }

func (s resetClusterSettingStep) Description() string {
	return fmt.Sprintf("reset cluster setting %q on %s tenant", s.name, s.virtualClusterName)
}

func (s resetClusterSettingStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	stmt := fmt.Sprintf("RESET CLUSTER SETTING %s", s.name)
	return serviceByName(h, s.virtualClusterName).ExecWithGateway(
		rng, nodesRunningAtLeast(s.virtualClusterName, s.minVersion, h), stmt,
	)
}

// deleteAllTenantsVersionOverrideStep is a hack that deletes bad data
// from the `system.tenant_settings` table; specifically an
// all-tenants (tenant_id = 0) override for the 'version' key. See
// #125702 for more details.
//
// This allows us to continue testing virtual cluster upgrades to
// versions older than 24.2 without hitting this (already fixed) bug.
type deleteAllTenantsVersionOverrideStep struct{}

func (s deleteAllTenantsVersionOverrideStep) Background() shouldStop { return nil }

func (s deleteAllTenantsVersionOverrideStep) Description() string {
	return "delete all-tenants override for the `version` key"
}

func (s deleteAllTenantsVersionOverrideStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	const stmt = "DELETE FROM system.tenant_settings WHERE tenant_id = $1 and name = $2"
	return h.System.Exec(rng, stmt, 0, "version")
}

// nodesRunningAtLeast returns a list of nodes running a system or
// tenant virtual cluster in a version that is guaranteed to be at
// least `minVersion`. It assumes that the caller made sure that there
// *is* one such node.
func nodesRunningAtLeast(
	virtualClusterName string, minVersion *clusterupgrade.Version, h *Helper,
) option.NodeListOption {
	service := serviceByName(h, virtualClusterName)

	// If we don't have a minimum version set, or if we are upgrading
	// from a version that is at least `minVersion`, then every node is
	// valid.
	if minVersion == nil || service.FromVersion.AtLeast(minVersion) {
		return service.Descriptor.Nodes
	}

	// This case should correspond to the scenario where are upgrading
	// from a release in the same series as `minVersion`. The valid
	// nodes should be those that are running the next version.
	return service.NodesInNextVersion()
}

func serviceByName(h *Helper, virtualClusterName string) *Service {
	if virtualClusterName == install.SystemInterfaceName {
		return h.System
	}

	return h.Tenant
}

// waitForSharedProcess waits for the shared-process created for this
// test to be ready to accept connections on the `nodes` provided.
func waitForSharedProcess(
	ctx context.Context, l *logger.Logger, h *Helper, nodes option.NodeListOption,
) error {
	group := ctxgroup.WithContext(ctx)
	for _, n := range nodes {
		group.GoCtx(func(ctx context.Context) error {
			// We use a new connection pool in this function as the runner's
			// connection pool might not be initialized.
			db, err := h.runner.cluster.ConnE(
				ctx, l, n, option.VirtualClusterName(h.Tenant.Descriptor.Name),
			)
			if err != nil {
				return errors.Wrap(err, "waitForSharedProcess: failed to connect to tenant")
			}
			defer db.Close()

			retryOpts := retry.Options{MaxRetries: 5}
			var nonRetryableErr error

			// Retry the dummy `SELECT 1` statement if we keep getting the
			// service unavailable error. However, any other error is
			// unexpected and should cause the test to fail.
			err = retryOpts.Do(ctx, func(ctx context.Context) error {
				_, err := db.ExecContext(ctx, "SELECT 1")
				err = errors.Wrapf(err, "waiting for shared-process tenant on n%d", n)

				if err != nil && strings.Contains(err.Error(), "service unavailable for target tenant") {
					l.Printf("failed to connect to shared-process tenant, retrying: %v", err)
					return err
				}

				nonRetryableErr = err
				return nil
			})

			return errors.CombineErrors(err, nonRetryableErr)
		})
	}

	return group.Wait()
}

func quoteVersionForPresentation(v string) string {
	quotedV := v
	if v != clusterupgrade.CurrentVersionString {
		quotedV = fmt.Sprintf("'%s'", v)
	}

	return quotedV
}

// startOpts returns the start options used when starting (or
// restarting) cockroach processes in mixedversion tests. We disable
// regular backups as some tests check for running jobs and the
// scheduled backup may make things non-deterministic. In the future,
// we should change the default and add an API for tests to opt-out of
// the default scheduled backup if necessary.
func startOpts(opts ...option.StartStopOption) option.StartOpts {
	return option.NewStartOpts(
		startStopOpts(opts...)...,
	)
}

// startStopOpts does the same as `startOpts` but returns StartStopOptions
// instead. This is required when starting virtual clusters.
func startStopOpts(opts ...option.StartStopOption) []option.StartStopOption {
	return append([]option.StartStopOption{
		option.NoBackupSchedule,
	}, opts...)
}
