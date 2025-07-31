// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package mixedversion

import (
	"context"
	"fmt"
	"math"
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

// systemTag is the roachprod tag applied to the cockroach process
// associated with the storage cluster. This is necessary in
// separate-process deployments where, when performing a rolling
// restart, we want to be able to stop *only* the process for the
// system tenant on a node. By default, roachtest's `Stop` function
// will stop every cockroach process.
const systemTag = "mixedversion-system"

// startTimeout is the maximum amount of time we will wait for a node
// to start up (including restarts). Especially useful in cases where
// we wait for a 3x replication after a restart, to fail early in
// situations where the cluster is not recovering.
var startTimeout = 30 * time.Minute

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
	rt                 test.Test
	version            *clusterupgrade.Version
	initTarget         int
	waitForReplication bool
	settings           []install.ClusterSettingOption
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
		install.TagOption(systemTag),
	)

	customStartOpts := []option.StartStopOption{option.WithInitTarget(s.initTarget)}
	if s.waitForReplication {
		customStartOpts = append(customStartOpts, option.WaitForReplication())
	}

	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	return clusterupgrade.StartWithSettings(
		startCtx, l, h.runner.cluster, systemNodes, startOpts(customStartOpts...), clusterSettings...,
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
	return waitForTenantProcess(ctx, l, h, h.Tenant.Descriptor.Nodes, h.DeploymentMode())
}

// startSeparateProcessVirtualCluster step creates a new separate-process
// virtual cluster with the given name, and starts it.
type startSeparateProcessVirtualClusterStep struct {
	name     string
	rt       test.Test
	version  *clusterupgrade.Version
	settings []install.ClusterSettingOption
}

func (s startSeparateProcessVirtualClusterStep) Background() shouldStop { return nil }

func (s startSeparateProcessVirtualClusterStep) Description() string {
	return fmt.Sprintf(
		"start separate process virtual cluster %s with binary version %s",
		s.name, s.version,
	)
}

func (s startSeparateProcessVirtualClusterStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	l.Printf("starting separate process virtual cluster %s at version %s", s.name, s.version)
	startOpts := option.StartVirtualClusterOpts(
		s.name,
		h.Tenant.Descriptor.Nodes,
		startStopOpts(option.StorageCluster(h.System.Descriptor.Nodes))...,
	)

	binaryPath := clusterupgrade.BinaryPathForVersion(s.rt, s.version, "cockroach")
	settings := install.MakeClusterSettings(append(s.settings, install.BinaryOption(binaryPath))...)

	if err := h.runner.cluster.StartServiceForVirtualClusterE(ctx, l, startOpts, settings); err != nil {
		return err
	}
	h.runner.cluster.SetDefaultVirtualCluster(s.name)

	return waitForTenantProcess(ctx, l, h, h.Tenant.Descriptor.Nodes, h.DeploymentMode())
}

type restartVirtualClusterStep struct {
	virtualCluster string
	version        *clusterupgrade.Version
	rt             test.Test
	node           int
	settings       []install.ClusterSettingOption
}

func (s restartVirtualClusterStep) Background() shouldStop { return nil }

func (s restartVirtualClusterStep) Description() string {
	return fmt.Sprintf(
		"restart %s server on node %d with binary version %s",
		s.virtualCluster, s.node, s.version,
	)
}

func (s restartVirtualClusterStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	const maxWait = 300 // 5 minutes

	l.Printf("restarting node %d (tenant %s) into version %s", s.node, s.virtualCluster, s.version)
	node := h.runner.cluster.Node(s.node)

	h.ExpectDeath()
	stopOpts := option.StopVirtualClusterOpts(s.virtualCluster, node, option.Graceful(maxWait))
	if err := h.runner.cluster.StopServiceForVirtualClusterE(ctx, l, stopOpts); err != nil {
		return errors.Wrap(err, "failed to stop cockroach process for tenant")
	}

	// Assume the binary already exists on the node as this step should
	// only be scheduled after the storage cluster has already upgraded.
	binaryPath := clusterupgrade.BinaryPathForVersion(s.rt, s.version, "cockroach")
	opts := startStopOpts()
	// Specify the storage cluster if it's separate process.
	if h.DeploymentMode() == SeparateProcessDeployment {
		opts = append(opts, option.StorageCluster(h.System.Descriptor.Nodes))
	}
	startOpts := option.StartVirtualClusterOpts(s.virtualCluster, node, opts...)
	settings := install.MakeClusterSettings(append(s.settings, install.BinaryOption(binaryPath))...)
	return h.runner.cluster.StartServiceForVirtualClusterE(ctx, l, startOpts, settings)
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
		"wait for all nodes (%v) to acknowledge cluster version %s on %s tenant",
		s.nodes, quoteVersionForPresentation(s.desiredVersion), s.virtualClusterName,
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
// will restart using the new binary. This step only applies to the
// system tenant. For the (separate-process) multitenant equivalent,
// see `restartVirtualClusterStep`.
type restartWithNewBinaryStep struct {
	version            *clusterupgrade.Version
	rt                 test.Test
	node               int
	settings           []install.ClusterSettingOption
	initTarget         int
	waitForReplication bool
	tenantRunning      bool // whether the test tenant is running when this step is called
	deploymentMode     DeploymentMode
}

func (s restartWithNewBinaryStep) Background() shouldStop { return nil }

func (s restartWithNewBinaryStep) Description() string {
	var systemDesc string
	if s.deploymentMode == SeparateProcessDeployment {
		systemDesc = " system server on"
	}

	return fmt.Sprintf(
		"restart%s node %d with binary version %s",
		systemDesc, s.node, s.version,
	)
}

func (s restartWithNewBinaryStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	customStartOpts := []option.StartStopOption{option.WithInitTarget(s.initTarget)}
	if s.waitForReplication {
		customStartOpts = append(customStartOpts, option.WaitForReplication())
	}

	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	settings := append([]install.ClusterSettingOption{
		install.TagOption(systemTag),
	}, s.settings...)

	h.ExpectDeath()
	if err := clusterupgrade.RestartNodesWithNewBinary(
		startCtx,
		s.rt,
		l,
		h.runner.cluster,
		h.runner.cluster.Node(s.node),
		startOpts(customStartOpts...),
		s.version,
		settings...,
	); err != nil {
		return err
	}

	if s.deploymentMode == SharedProcessDeployment && s.tenantRunning {
		// If we are in shared-process mode and the tenant is already
		// running at this point, we wait for the server on the restarted
		// node to be up before moving on.
		return waitForTenantProcess(ctx, l, h, h.runner.cluster.Node(s.node), s.deploymentMode)
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
	systemVisible      bool
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
	// We set the cluster setting on the corresponding `virtualClusterName`.
	// However, if `systemVisible` is true, it means the setting
	// is only settable via the system interface.
	var tenantPrefix string
	serviceName := s.virtualClusterName
	if s.systemVisible {
		tenantPrefix = fmt.Sprintf("ALTER TENANT %q ", s.virtualClusterName)
		serviceName = install.SystemInterfaceName
	}

	var stmt string
	var args []interface{}
	// We do a type switch on common types to avoid errors when using
	// placeholders, as type detection is often not implemented for some
	// private cluster settings.
	switch val := s.value.(type) {
	case string:
		stmt = fmt.Sprintf("%sSET CLUSTER SETTING %s = '%s'", tenantPrefix, s.name, val)
	case bool:
		stmt = fmt.Sprintf("%sSET CLUSTER SETTING %s = %t", tenantPrefix, s.name, val)
	case int:
		stmt = fmt.Sprintf("%sSET CLUSTER SETTING %s = %d", tenantPrefix, s.name, val)
	default:
		// If not using any of these types, do a best-effort attempt using
		// a placeholder.
		stmt = fmt.Sprintf("%sSET CLUSTER SETTING %s = $1", tenantPrefix, s.name)
		args = []interface{}{val}
	}

	return serviceByName(h, serviceName).ExecWithGateway(
		rng, nodesRunningAtLeast(s.virtualClusterName, s.minVersion, h), stmt, args...,
	)
}

// setClusterVersionStep sets the special `version` cluster setting to
// the provided version.
type setClusterVersionStep struct {
	v                  *clusterupgrade.Version
	virtualClusterName string
}

func (s setClusterVersionStep) Background() shouldStop { return nil }

func (s setClusterVersionStep) Description() string {
	value := versionToClusterVersion(s.v)
	if !s.v.IsCurrent() {
		value = fmt.Sprintf("'%s'", value)
	}

	return fmt.Sprintf(
		"set `version` to %s on %s tenant",
		value, s.virtualClusterName,
	)
}

func (s setClusterVersionStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	service := serviceByName(h, s.virtualClusterName)
	binaryVersion := versionToClusterVersion(s.v)
	if s.v.IsCurrent() {
		node, db := service.RandomDB(rng)
		l.Printf("fetching binary version via n%d", node)

		bv, err := clusterupgrade.BinaryVersion(ctx, db)
		if err != nil {
			return errors.Wrapf(err, "getting binary version on n%d", node)
		}

		binaryVersion = bv.String()
	}

	l.Printf("setting cluster version to '%s'", binaryVersion)
	return service.Exec(rng, "SET CLUSTER SETTING version = $1", binaryVersion)
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

// disableRateLimitersStep disables both the KV and the tenant(SQL) rate limiter
// for the given virtual cluster. This step is necessary for separate process
// tenants to avoid rate limiting which can cause tests to hang and fail.
//
// N.B. This step is not needed for shared process tenants as they already opt out
// of both rate limiters. Shared process tenants are by default not subject to the
// tenant rate limiter, as they create a `noopTenantSideCostController`. Shared process
// tenants are automatically granted all tenant capabilities as of v24.1 which includes
// exemption from the kv rate limiter. For older versions, the mvt framework already
// handles granting said capabilities (see: TenantsAndSystemAlignedSettingsVersion).
type disableRateLimitersStep struct {
	virtualClusterName string
}

func (s disableRateLimitersStep) Background() shouldStop { return nil }

func (s disableRateLimitersStep) Description() string {
	return fmt.Sprintf("disable KV and tenant(SQL) rate limiter on %s tenant", s.virtualClusterName)
}

func (s disableRateLimitersStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	// Disable the KV rate limiter.
	stmt := fmt.Sprintf(
		"ALTER TENANT %q GRANT CAPABILITY exempt_from_rate_limiting = true",
		s.virtualClusterName,
	)
	if err := h.System.Exec(rng, stmt); err != nil {
		return err
	}

	const (
		availableTokens = math.MaxFloat64
		refillRate      = math.MaxFloat64
		maxBurstTokens  = 0 // 0 disables the limit.
	)

	// Disable the tenant rate limiter. Unlike the KV rate limiter, there is no way
	// to outright disable the tenant rate limiter. Instead, we instead set the tenant's
	// resource limits to an arbitrarily high value.
	stmt = fmt.Sprintf(
		"SELECT crdb_internal.update_tenant_resource_limits('%s', %v, %v, %d, now(), 0);",
		s.virtualClusterName, availableTokens, refillRate, maxBurstTokens,
	)

	if h.System.FromVersion.AtLeast(updateTenantResourceLimitsDeprecatedArgsVersion) {
		stmt = fmt.Sprintf(
			"SELECT crdb_internal.update_tenant_resource_limits('%s', %v, %v, %d);",
			s.virtualClusterName, availableTokens, refillRate, maxBurstTokens,
		)
	}

	return h.System.Exec(rng, stmt)
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

// waitForTenantProcess waits for the tenant-process created for this
// test to be ready to accept connections on the `nodes` provided.
func waitForTenantProcess(
	ctx context.Context,
	l *logger.Logger,
	h *Helper,
	nodes option.NodeListOption,
	deployment DeploymentMode,
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
				return errors.Wrapf(err, "waitForTenantProcess: failed to connect to %s tenant", deployment)
			}
			defer db.Close()

			retryOpts := retry.Options{MaxRetries: 5}
			var nonRetryableErr error

			// Retry the dummy `SELECT 1` statement if we keep getting the
			// service unavailable error. However, any other error is
			// unexpected and should cause the test to fail.
			err = retryOpts.Do(ctx, func(ctx context.Context) error {
				_, err := db.ExecContext(ctx, "SELECT 1")
				err = errors.Wrapf(err, "waiting for %s tenant on n%d", deployment, n)

				if err != nil && strings.Contains(err.Error(), "service unavailable for target tenant") {
					l.Printf("failed to connect to %s tenant, retrying: %v", deployment, err)
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
