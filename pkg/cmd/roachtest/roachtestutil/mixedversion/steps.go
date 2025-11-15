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
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/roachtestutil/clusterupgrade"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod"
	"github.com/cockroachdb/cockroach/pkg/roachprod/failureinjection/failures"
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

// restartSystemSettings provides the custom start options
// necessary for restarting the system interface on a node.
func restartSystemSettings(waitForReplication bool, initTarget int) []option.StartStopOption {
	customStartOpts := []option.StartStopOption{option.WithInitTarget(initTarget)}
	if waitForReplication {
		customStartOpts = append(customStartOpts, option.WaitForReplication())
	}
	customStartOpts = append(customStartOpts, option.SkipInit)
	return customStartOpts
}

// installFixturesStep is the step that copies the fixtures from
// `pkg/cmd/roachtest/fixtures` for a specific version into the nodes'
// store dir.
type installFixturesStep struct {
	version *clusterupgrade.Version
}

func (s installFixturesStep) Background() shouldStop { return nil }

func (s installFixturesStep) Description(debug bool) string {
	return fmt.Sprintf("install fixtures for version %q", s.version.String())
}

func (s installFixturesStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	return clusterupgrade.InstallFixtures(
		ctx, l, h.runner.cluster, h.System.Descriptor.Nodes, s.version,
	)
}

func (s installFixturesStep) ConcurrencyDisabled() bool {
	return false
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

func (s startStep) Description(debug bool) string {
	return fmt.Sprintf("start cluster at version %q", s.version)
}

// Run implements the Step interface for startStep. It stages the cockroach
// binary of the current cluster version on all nodes and starts the cockroach
// binary on the cluster nodes.
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
func (s startStep) ConcurrencyDisabled() bool {
	return true
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

func (s startSharedProcessVirtualClusterStep) Description(debug bool) string {
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

func (s startSharedProcessVirtualClusterStep) ConcurrencyDisabled() bool {
	return true
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

func (s startSeparateProcessVirtualClusterStep) Description(debug bool) string {
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

func (s startSeparateProcessVirtualClusterStep) ConcurrencyDisabled() bool {
	return true
}

type restartVirtualClusterStep struct {
	virtualCluster string
	version        *clusterupgrade.Version
	rt             test.Test
	node           int
	settings       []install.ClusterSettingOption
}

func (s restartVirtualClusterStep) Background() shouldStop { return nil }

func (s restartVirtualClusterStep) Description(debug bool) string {
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

func (s restartVirtualClusterStep) ConcurrencyDisabled() bool {
	return true
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

func (s waitForStableClusterVersionStep) Description(debug bool) string {
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

func (s waitForStableClusterVersionStep) ConcurrencyDisabled() bool {
	return false
}

// preserveDowngradeOptionStep sets the `preserve_downgrade_option`
// cluster setting to the binary version running in a random node in
// the cluster.
type preserveDowngradeOptionStep struct {
	virtualClusterName string
}

func (s preserveDowngradeOptionStep) Background() shouldStop { return nil }

func (s preserveDowngradeOptionStep) Description(debug bool) string {
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
	bv, err := clusterupgrade.BinaryVersion(ctx, l, db)
	if err != nil {
		return err
	}

	return service.ExecWithRetry(rng, service.Descriptor.Nodes, roachtestutil.ClusterSettingRetryOpts,
		"SET CLUSTER SETTING cluster.preserve_downgrade_option = $1", bv.String(),
	)
}

func (s preserveDowngradeOptionStep) ConcurrencyDisabled() bool {
	return false
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

func (s restartWithNewBinaryStep) Description(debug bool) string {
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
	customStartOpts := restartSystemSettings(s.waitForReplication, s.initTarget)

	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	settings := append([]install.ClusterSettingOption{
		install.TagOption(systemTag),
	}, s.settings...)

	node := h.runner.cluster.Node(s.node)
	if err := clusterupgrade.RestartNodesWithNewBinary(
		startCtx,
		s.rt,
		l,
		h.runner.cluster,
		node,
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
		return waitForTenantProcess(ctx, l, h, node, s.deploymentMode)
	}

	return nil
}

func (s restartWithNewBinaryStep) ConcurrencyDisabled() bool {
	return true
}

// allowUpgradeStep resets the `preserve_downgrade_option` cluster
// setting, allowing the upgrade migrations to run and the cluster
// version to eventually reach the binary version on the nodes.
type allowUpgradeStep struct {
	virtualClusterName string
}

func (s allowUpgradeStep) Background() shouldStop { return nil }

func (s allowUpgradeStep) Description(debug bool) string {
	return fmt.Sprintf(
		"allow upgrade to happen on %s tenant by resetting `preserve_downgrade_option`",
		s.virtualClusterName,
	)
}

func (s allowUpgradeStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	service := serviceByName(h, s.virtualClusterName)
	return service.ExecWithRetry(
		rng, service.Descriptor.Nodes, roachtestutil.ClusterSettingRetryOpts,
		"RESET CLUSTER SETTING cluster.preserve_downgrade_option",
	)
}

func (s allowUpgradeStep) ConcurrencyDisabled() bool {
	return false
}

// waitStep does nothing but sleep for the provided duration. Most
// commonly used to allow the cluster to stay in a certain state
// before attempting node restarts or other upgrade events.
type waitStep struct {
	dur time.Duration
}

func (s waitStep) Background() shouldStop { return nil }

func (s waitStep) Description(debug bool) string {
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

func (s waitStep) ConcurrencyDisabled() bool {
	return false
}

// runHookStep is a step used to run a user-provided hook (i.e.,
// callbacks passed to `OnStartup`, `InMixedVersion`, or `AfterTest`).
type runHookStep struct {
	hook     versionUpgradeHook
	stopChan shouldStop
}

func (s runHookStep) Background() shouldStop { return s.stopChan }

func (s runHookStep) Description(debug bool) string {
	if debug {
		return fmt.Sprintf("run %q hookId=%q", s.hook.name, s.hook.id)
	}
	return fmt.Sprintf("run %q", s.hook.name)
}

func (s runHookStep) Run(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {
	return s.hook.fn(ctx, l, rng, h)
}

func (s runHookStep) ConcurrencyDisabled() bool {
	return false
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

func (s setClusterSettingStep) Description(debug bool) string {
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

	return serviceByName(h, serviceName).ExecWithRetry(
		rng, nodesRunningAtLeast(s.virtualClusterName, s.minVersion, h), roachtestutil.ClusterSettingRetryOpts, stmt, args...,
	)
}

func (s setClusterSettingStep) ConcurrencyDisabled() bool {
	return false
}

// setClusterVersionStep sets the special `version` cluster setting to
// the provided version.
type setClusterVersionStep struct {
	v                  *clusterupgrade.Version
	virtualClusterName string
}

func (s setClusterVersionStep) Background() shouldStop { return nil }

func (s setClusterVersionStep) Description(debug bool) string {
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

		bv, err := clusterupgrade.BinaryVersion(ctx, l, db)
		if err != nil {
			return errors.Wrapf(err, "getting binary version on n%d", node)
		}

		binaryVersion = bv.String()
	}

	l.Printf("setting cluster version to '%s'", binaryVersion)
	return service.ExecWithRetry(rng, service.Descriptor.Nodes, roachtestutil.ClusterSettingRetryOpts,
		"SET CLUSTER SETTING version = $1", binaryVersion,
	)
}

func (s setClusterVersionStep) ConcurrencyDisabled() bool {
	return false
}

// resetClusterSetting resets cluster setting `name`.
type resetClusterSettingStep struct {
	minVersion         *clusterupgrade.Version
	name               string
	virtualClusterName string
}

func (s resetClusterSettingStep) Background() shouldStop { return nil }

func (s resetClusterSettingStep) Description(debug bool) string {
	return fmt.Sprintf("reset cluster setting %q on %s tenant", s.name, s.virtualClusterName)
}

func (s resetClusterSettingStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	stmt := fmt.Sprintf("RESET CLUSTER SETTING %s", s.name)
	return serviceByName(h, s.virtualClusterName).ExecWithRetry(
		rng, nodesRunningAtLeast(s.virtualClusterName, s.minVersion, h), roachtestutil.ClusterSettingRetryOpts, stmt,
	)
}

func (s resetClusterSettingStep) ConcurrencyDisabled() bool {
	return false
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

func (s deleteAllTenantsVersionOverrideStep) Description(debug bool) string {
	return "delete all-tenants override for the `version` key"
}

func (s deleteAllTenantsVersionOverrideStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	const stmt = "DELETE FROM system.tenant_settings WHERE tenant_id = $1 and name = $2"
	return h.System.Exec(rng, stmt, 0, "version")
}

func (s deleteAllTenantsVersionOverrideStep) ConcurrencyDisabled() bool {
	return false
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

func (s disableRateLimitersStep) Description(debug bool) string {
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

func (s disableRateLimitersStep) ConcurrencyDisabled() bool {
	return false
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
	startOpts := option.NewStartOpts(
		startStopOpts(opts...)...,
	)
	// Enable verbose logging for auto_upgrade to help debug upgrade-related issues.
	startOpts.RoachprodOpts.ExtraArgs = append(startOpts.RoachprodOpts.ExtraArgs, "--vmodule=auto_upgrade=2,allocator*=3")
	return startOpts
}

// startStopOpts does the same as `startOpts` but returns StartStopOptions
// instead. This is required when starting virtual clusters.
func startStopOpts(opts ...option.StartStopOption) []option.StartStopOption {
	return append([]option.StartStopOption{
		option.NoBackupSchedule,
	}, opts...)
}

// TODO(kyleli): This step currently only affects the system tenant, should support panicking secondary tenants as well.
type panicNodeStep struct {
	initTarget int
	targetNode option.NodeListOption
}

func (s panicNodeStep) Background() shouldStop { return nil }

func (s panicNodeStep) Description(debug bool) string {
	return fmt.Sprintf("panicking system interface on node %d", s.targetNode[0])
}

func (s panicNodeStep) Run(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {

	h.runner.monitor.ExpectProcessDead(s.targetNode)

	// ExecWithGateway cannot be used here because the monitor marks the target node as expected
	// dead, and it will be filtered out of the list of available nodes. This a unique case, so
	// we manually log the SQL statement and execute it directly on the target node.
	const query = "SELECT crdb_internal.force_panic('expected panic from panicNodeMutator')"
	db := h.System.Connect(s.targetNode[0])

	v, err := h.System.NodeVersion(s.targetNode[0])
	if err != nil {
		return errors.Wrapf(err, "failed to get node version for %d", s.targetNode[0])
	}
	logSQL(
		h.System.stepLogger, s.targetNode[0], v, h.System.Descriptor.Name, query,
	)

	if _, err = db.ExecContext(h.System.ctx, query); err == nil {
		return errors.Errorf("expected panic statement to fail, but it succeeded on %s", s.targetNode)
	}

	return nil
}

func (s panicNodeStep) ConcurrencyDisabled() bool {
	return true
}

// Restarts a dead node on the same binary version it was running, unlike
// `restartWithNewBinaryStep` which restarts an alive node with a new binary.
type restartNodeStep struct {
	initTarget  int
	targetNode  option.NodeListOption
	rt          test.Test
	description string
}

func (restartNodeStep) Background() shouldStop { return nil }

func (s restartNodeStep) Description(debug bool) string {
	return s.description
}

func (s restartNodeStep) Run(ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper) error {
	nodeVersion, err := h.System.NodeVersion(s.targetNode[0])
	if err != nil {
		return errors.Wrapf(err, "failed to get node version for %s", s.targetNode)
	}
	binary := clusterupgrade.CockroachPathForVersion(s.rt, nodeVersion)
	settings := install.MakeClusterSettings(
		install.BinaryOption(binary),
		install.TagOption(systemTag),
	)
	customStartOpts := restartSystemSettings(true, s.initTarget)

	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	err = h.runner.cluster.StartE(
		startCtx,
		l,
		startOpts(customStartOpts...),
		settings,
		s.targetNode,
	)
	if err != nil {
		return errors.Wrapf(
			err, "failed to restart node %d with binary %s", s.targetNode[0], binary,
		)
	}
	return nil

}

func (s restartNodeStep) ConcurrencyDisabled() bool {
	return true
}

type networkPartitionInjectStep struct {
	f                *failures.Failer
	partitions       []failures.NetworkPartition
	unavailableNodes option.NodeListOption
	protectedNodes   option.NodeListOption
}

func (s networkPartitionInjectStep) Background() shouldStop { return nil }

func (s networkPartitionInjectStep) Description(debug bool) string {
	return fmt.Sprintf("creating network partition(s): %v", s.partitions)
}

func (s networkPartitionInjectStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	// Wait for all ranges to have voter replicas on protected nodes before injecting partition.
	// We already waited when creating the initial zone configs, but we may have run operations
	// that caused ranges to be placed outside our config, e.g. importing from a fixture.
	retryOpts := retry.Options{
		MaxDuration:    30 * time.Minute,
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     5 * time.Second,
	}
	if err := waitForVoterReplicasOnNodes(ctx, l, rng, h, s.protectedNodes, retryOpts); err != nil {
		return errors.Wrap(err, "ranges not ready for partition")
	}

	h.runner.monitor.ExpectProcessDead(s.unavailableNodes)
	if h.DeploymentMode() == SeparateProcessDeployment {
		opt := option.VirtualClusterName(h.Tenant.Descriptor.Name)
		h.runner.monitor.ExpectProcessDead(s.unavailableNodes, opt)
	}

	args := failures.NetworkPartitionArgs{Partitions: s.partitions}

	if err := s.f.Setup(ctx, l, args); err != nil {
		return errors.Wrapf(err, "failed to setup failure %s", failures.IPTablesNetworkPartitionName)
	}

	if err := s.f.Inject(ctx, l, args); err != nil {
		return errors.Wrapf(err, "failed to inject failure %s", failures.IPTablesNetworkPartitionName)
	}

	return s.f.WaitForFailureToPropagate(ctx, l)
}

// waitForVoterReplicasOnNodes blocks until all ranges have voter replicas on the specified nodes.
// If the retry loop fails, it logs non-compliant ranges before returning an error.
func waitForVoterReplicasOnNodes(
	ctx context.Context,
	l *logger.Logger,
	rng *rand.Rand,
	h *Helper,
	protectedNodes option.NodeListOption,
	retryOpts retry.Options,
) error {
	// Only wait/check if we have protected nodes
	if len(protectedNodes) == 0 {
		return nil
	}

	// Build the WHERE clause to check for ranges with voters on protected nodes
	var nodeConditions []string
	for _, node := range protectedNodes {
		nodeConditions = append(nodeConditions, fmt.Sprintf("array_position(voting_replicas, %d) IS NOT NULL", node))
	}
	nodeFilter := strings.Join(nodeConditions, " AND ")

	// Block until all ranges have voter replicas on our protected nodes.
	// Make sure we exclude dropped tables which will show when querying
	// ranges, but may not respect our zone configs.
	return retryOpts.Do(ctx, func(ctx context.Context) error {
		query := fmt.Sprintf(`
		WITH undropped AS (
			SELECT table_id
			FROM crdb_internal.tables
			WHERE state != 'DROP'
		)
		SELECT
			count(*) AS total_ranges,
			count(*) FILTER (WHERE %s) AS ranges_with_all_voters
		FROM [SHOW CLUSTER RANGES WITH TABLES, DETAILS] AS r
		JOIN undropped AS t
			ON r.table_id = t.table_id;
		`, nodeFilter)

		var totalRanges, rangesWithAllVoters int
		row := h.System.QueryRow(rng, query)
		if err := row.Scan(&totalRanges, &rangesWithAllVoters); err != nil {
			return errors.Wrap(err, "failed to query voter replicas")
		}

		if rangesWithAllVoters != totalRanges {
			l.Printf("protected nodes %v have voter replicas on %d/%d ranges", protectedNodes, rangesWithAllVoters, totalRanges)
			return errors.Errorf("protected nodes %v have voter replicas on %d/%d ranges", protectedNodes, rangesWithAllVoters, totalRanges)
		}
		l.Printf("all protected nodes %v have voter replicas on all %d ranges", protectedNodes, totalRanges)
		return nil
	})
}

func (s networkPartitionInjectStep) ConcurrencyDisabled() bool {
	return true
}

type networkPartitionRecoveryStep struct {
	f                *failures.Failer
	partitions       []failures.NetworkPartition
	unavailableNodes option.NodeListOption
}

func (s networkPartitionRecoveryStep) Background() shouldStop { return nil }

func (s networkPartitionRecoveryStep) Description(debug bool) string {
	return fmt.Sprintf("recovering network partition(s): %v", s.partitions)
}

func (s networkPartitionRecoveryStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	if err := s.f.Recover(ctx, l); err != nil {
		return errors.Wrapf(err, "failed to recover failure %s", failures.IPTablesNetworkPartitionName)
	}
	if err := s.f.WaitForFailureToRecover(ctx, l); err != nil {
		return errors.Wrapf(err, "failed to wait for recovery of failure %s", failures.IPTablesNetworkPartitionName)
	}

	h.runner.monitor.ExpectProcessAlive(s.unavailableNodes)
	if h.DeploymentMode() == SeparateProcessDeployment {
		opt := option.VirtualClusterName(h.Tenant.Descriptor.Name)
		h.runner.monitor.ExpectProcessAlive(s.unavailableNodes, opt)
	}
	return s.f.Cleanup(ctx, l)
}

func (s networkPartitionRecoveryStep) ConcurrencyDisabled() bool {
	return false
}

type alterReplicationFactorStep struct {
	replicationFactor int
	targetNode        option.NodeListOption
}

func (s alterReplicationFactorStep) Background() shouldStop { return nil }

func (s alterReplicationFactorStep) Description(debug bool) string {
	return fmt.Sprintf("alter replication factor to %d", s.replicationFactor)
}

func (s alterReplicationFactorStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	stmt := fmt.Sprintf("ALTER RANGE default CONFIGURE ZONE USING num_replicas = %d", s.replicationFactor)
	if err := h.System.Exec(
		rng,
		stmt,
	); err != nil {
		return errors.Wrap(err, "failed to change replication factor")
	}

	replicationLogger, loggerName, err := roachtestutil.LoggerForCmd(l, s.targetNode, "range-replication")
	if err != nil {
		return errors.Wrapf(err, "failed to create logger %s", loggerName)
	}

	l.Printf("waiting to reach replication factor of %dX; details in %s.log", s.replicationFactor, loggerName)
	db := h.System.Connect(s.targetNode[0])
	if err := roachtestutil.WaitForReplication(ctx, replicationLogger, db, s.replicationFactor, roachprod.AtLeastReplicationFactor); err != nil {
		return errors.Wrapf(err, "failed to reach replication factor of %dX", s.replicationFactor)
	}
	return nil
}

func (s alterReplicationFactorStep) ConcurrencyDisabled() bool {
	return false
}

// stageAllWorkloadBinariesStep stages new binaries on workload node(s)
type stageAllWorkloadBinariesStep struct {
	versions      []*clusterupgrade.Version
	rt            test.Test
	workloadNodes option.NodeListOption
}

func (s stageAllWorkloadBinariesStep) Background() shouldStop { return nil }
func (s stageAllWorkloadBinariesStep) Description(debug bool) string {
	versionStrings := make([]string, len(s.versions))
	for i, v := range s.versions {
		versionStrings[i] = v.String()
	}
	return fmt.Sprintf("stage workload binary on workload node(s) %s for version(s) %s",
		s.workloadNodes.String(), strings.Join(versionStrings, ", "))
}

// Run stages all the cockroach binaries needed for workload for this test
// on the workload node(s) to keep the workload binary version in sync with the
// cluster version because the workload binary is no longer backwards
// compatible with certain workloads.
func (s stageAllWorkloadBinariesStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {

	for _, version := range s.versions {
		_, err := clusterupgrade.UploadCockroach(
			ctx, s.rt, l, h.runner.cluster, s.workloadNodes, version)
		if err != nil {
			return err
		}
	}
	return nil
}
func (s stageAllWorkloadBinariesStep) ConcurrencyDisabled() bool {
	return true
}

type pinVoterReplicasStep struct {
	protectedNodes option.NodeListOption
}

func (s pinVoterReplicasStep) Background() shouldStop { return nil }

func (s pinVoterReplicasStep) Description(debug bool) string {
	return fmt.Sprintf("pin voter replicas to nodes %v", s.protectedNodes)
}

func (s pinVoterReplicasStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	// We build a zone constraint that requires every node in `protectedNodes`
	// to have a voter replica of every range.
	var constraintParts []string
	for _, node := range s.protectedNodes {
		constraintParts = append(constraintParts, fmt.Sprintf(`"+node%d": 1`, node))
	}
	constraintsStr := fmt.Sprintf(`'{%s}'`, strings.Join(constraintParts, ", "))
	// Setting voter constraints forces us to also set the replication factor.
	replicationFactor := 2*len(s.protectedNodes) - 1
	zoneConfig := fmt.Sprintf("constraints = %s, voter_constraints = %s, num_replicas = %d, num_voters = %d",
		constraintsStr, constraintsStr, replicationFactor, replicationFactor)

	// Apply the zone config for the default+system ranges and databases.
	//
	// N.B. If we start the cluster using fixtures, it will have additional databases such as
	// `lotsatables` and `persistent_db`. However, these are small enough that they should be
	// reallocated quickly.
	for _, rangeName := range []string{"default", "system", "timeseries", "liveness", "meta", "tenants"} {
		stmt := fmt.Sprintf("ALTER RANGE %s CONFIGURE ZONE USING %s", rangeName, zoneConfig)
		if err := h.System.Exec(rng, stmt); err != nil {
			return errors.Wrapf(err, "failed to set replica constraints for %s range", rangeName)
		}
	}

	for _, databaseName := range []string{"defaultdb", "system"} {
		stmt := fmt.Sprintf("ALTER DATABASE %s CONFIGURE ZONE USING %s", databaseName, zoneConfig)
		if err := h.System.Exec(rng, stmt); err != nil {
			return errors.Wrapf(err, "failed to set replica constraints for system database")
		}
	}

	retryOpts := retry.Options{
		MaxDuration:    5 * time.Minute,
		InitialBackoff: 5 * time.Second,
		MaxBackoff:     5 * time.Second,
	}

	return waitForVoterReplicasOnNodes(ctx, l, rng, h, s.protectedNodes, retryOpts)
}

func (s pinVoterReplicasStep) ConcurrencyDisabled() bool {
	return false
}
