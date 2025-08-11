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
	Version *clusterupgrade.Version
}

func (s installFixturesStep) Background(_ *Helper) shouldStop { return nil }

func (s installFixturesStep) Description() string {
	return fmt.Sprintf("install fixtures for version %q", s.Version.String())
}

func (s installFixturesStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	return clusterupgrade.InstallFixtures(
		ctx, l, h.runner.cluster, h.System.Descriptor.Nodes, s.Version,
	)
}

func (s installFixturesStep) ConcurrencyDisabled() bool {
	return false
}

func (s installFixturesStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.InstallFixturesStep)
}

// startStep is the step that starts the cluster from a specific
// `version`.
type startStep struct {
	Version            *clusterupgrade.Version
	InitTarget         int
	WaitForReplication bool
	Settings           install.ClusterSettingOptionList `yaml:",omitempty"`
}

func (s startStep) Background(_ *Helper) shouldStop { return nil }

func (s startStep) Description() string {
	return fmt.Sprintf("start cluster at version %q", s.Version)
}

// Run uploads the binary associated with the given version and starts
// the cockroach binary on the nodes.
func (s startStep) Run(ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper) error {
	systemNodes := h.System.Descriptor.Nodes
	binaryPath, err := clusterupgrade.UploadCockroach(
		ctx, h.runner.rt, l, h.runner.cluster, systemNodes, s.Version,
	)
	if err != nil {
		return err
	}

	clusterSettings := append(
		append([]install.ClusterSettingOption{}, s.Settings...),
		install.BinaryOption(binaryPath),
		install.TagOption(systemTag),
	)

	customStartOpts := []option.StartStopOption{option.WithInitTarget(s.InitTarget)}
	if s.WaitForReplication {
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

func (s startStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.StartStep)
}

// startSharedProcessVirtualCluster step creates a new shared-process
// virtual cluster with the given name, and starts it. At the end of
// this step, the virtual cluster should be ready to receive requests.
type startSharedProcessVirtualClusterStep struct {
	Name       string
	InitTarget int
	Settings   install.ClusterSettingOptionList `yaml:",omitempty"`
}

func (s startSharedProcessVirtualClusterStep) Background(_ *Helper) shouldStop { return nil }

func (s startSharedProcessVirtualClusterStep) Description() string {
	return fmt.Sprintf("start shared-process tenant %q", s.Name)
}

func (s startSharedProcessVirtualClusterStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	l.Printf("starting shared process virtual cluster %s", s.Name)
	startOpts := option.StartSharedVirtualClusterOpts(s.Name, startStopOpts(option.WithInitTarget(s.InitTarget))...)

	if err := h.runner.cluster.StartServiceForVirtualClusterE(
		ctx, l, startOpts, install.MakeClusterSettings(s.Settings...),
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

func (s startSharedProcessVirtualClusterStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.StartSharedProcessVirtualClusterStep)
}

// startSeparateProcessVirtualCluster step creates a new separate-process
// virtual cluster with the given name, and starts it.
type startSeparateProcessVirtualClusterStep struct {
	Name     string
	Version  *clusterupgrade.Version
	Settings install.ClusterSettingOptionList `yaml:",omitempty"`
}

func (s startSeparateProcessVirtualClusterStep) Background(_ *Helper) shouldStop { return nil }

func (s startSeparateProcessVirtualClusterStep) Description() string {
	return fmt.Sprintf(
		"start separate process virtual cluster %s with binary version %s",
		s.Name, s.Version,
	)
}

func (s startSeparateProcessVirtualClusterStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	l.Printf("starting separate process virtual cluster %s at version %s", s.Name, s.Version)
	startOpts := option.StartVirtualClusterOpts(
		s.Name,
		h.Tenant.Descriptor.Nodes,
		startStopOpts(option.StorageCluster(h.System.Descriptor.Nodes))...,
	)

	binaryPath := clusterupgrade.BinaryPathForVersion(h.runner.rt, s.Version, "cockroach")
	settings := install.MakeClusterSettings(append(s.Settings, install.BinaryOption(binaryPath))...)

	if err := h.runner.cluster.StartServiceForVirtualClusterE(ctx, l, startOpts, settings); err != nil {
		return err
	}
	h.runner.cluster.SetDefaultVirtualCluster(s.Name)

	return waitForTenantProcess(ctx, l, h, h.Tenant.Descriptor.Nodes, h.DeploymentMode())
}

func (s startSeparateProcessVirtualClusterStep) ConcurrencyDisabled() bool {
	return true
}

func (s startSeparateProcessVirtualClusterStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.StartSeparateProcessVirtualClusterStep)
}

type restartVirtualClusterStep struct {
	VirtualCluster string
	Version        *clusterupgrade.Version
	Node           int
	Settings       install.ClusterSettingOptionList `yaml:",omitempty"`
}

func (s restartVirtualClusterStep) Background(_ *Helper) shouldStop { return nil }

func (s restartVirtualClusterStep) Description() string {
	return fmt.Sprintf(
		"restart %s server on node %d with binary version %s",
		s.VirtualCluster, s.Node, s.Version,
	)
}

func (s restartVirtualClusterStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	const maxWait = 300 // 5 minutes

	l.Printf("restarting node %d (tenant %s) into version %s", s.Node, s.VirtualCluster, s.Version)
	node := h.runner.cluster.Node(s.Node)

	stopOpts := option.StopVirtualClusterOpts(s.VirtualCluster, node, option.Graceful(maxWait))
	if err := h.runner.cluster.StopServiceForVirtualClusterE(ctx, l, stopOpts); err != nil {
		return errors.Wrap(err, "failed to stop cockroach process for tenant")
	}

	// Assume the binary already exists on the node as this step should
	// only be scheduled after the storage cluster has already upgraded.
	binaryPath := clusterupgrade.BinaryPathForVersion(h.runner.rt, s.Version, "cockroach")
	opts := startStopOpts()
	// Specify the storage cluster if it's separate process.
	if h.DeploymentMode() == SeparateProcessDeployment {
		opts = append(opts, option.StorageCluster(h.System.Descriptor.Nodes))
	}
	startOpts := option.StartVirtualClusterOpts(s.VirtualCluster, node, opts...)
	settings := install.MakeClusterSettings(append(s.Settings, install.BinaryOption(binaryPath))...)
	return h.runner.cluster.StartServiceForVirtualClusterE(ctx, l, startOpts, settings)
}

func (s restartVirtualClusterStep) ConcurrencyDisabled() bool {
	return true
}

func (s restartVirtualClusterStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.RestartVirtualClusterStep)
}

// waitForStableClusterVersionStep implements the process of waiting
// for the `version` cluster setting being the same on all nodes of
// the cluster and equal to the binary version of the first node in
// the `nodes` field.
type waitForStableClusterVersionStep struct {
	Nodes              option.NodeListOption
	DesiredVersion     string
	Timeout            time.Duration
	VirtualClusterName string
}

func (s waitForStableClusterVersionStep) Background(_ *Helper) shouldStop { return nil }

func (s waitForStableClusterVersionStep) Description() string {
	return fmt.Sprintf(
		"wait for all nodes (%v) to acknowledge cluster version %s on %s tenant",
		s.Nodes, quoteVersionForPresentation(s.DesiredVersion), s.VirtualClusterName,
	)
}

func (s waitForStableClusterVersionStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	return clusterupgrade.WaitForClusterUpgrade(
		ctx, l, s.Nodes, serviceByName(h, s.VirtualClusterName).Connect, s.Timeout,
	)
}

func (s waitForStableClusterVersionStep) ConcurrencyDisabled() bool {
	return false
}

func (s waitForStableClusterVersionStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.WaitForStableClusterVersionStep)
}

// preserveDowngradeOptionStep sets the `preserve_downgrade_option`
// cluster setting to the binary version running in a random node in
// the cluster.
type preserveDowngradeOptionStep struct {
	VirtualClusterName string
}

func (s preserveDowngradeOptionStep) Background(_ *Helper) shouldStop { return nil }

func (s preserveDowngradeOptionStep) Description() string {
	return fmt.Sprintf(
		"prevent auto-upgrades on %s tenant by setting `preserve_downgrade_option`",
		s.VirtualClusterName,
	)
}

func (s preserveDowngradeOptionStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	service := serviceByName(h, s.VirtualClusterName)
	node, db := service.RandomDB(rng)
	l.Printf("checking binary version (via node %d)", node)
	bv, err := clusterupgrade.BinaryVersion(ctx, db)
	if err != nil {
		return err
	}

	return service.Exec(rng, "SET CLUSTER SETTING cluster.preserve_downgrade_option = $1", bv.String())
}

func (s preserveDowngradeOptionStep) ConcurrencyDisabled() bool {
	return false
}

func (s preserveDowngradeOptionStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.PreserveDowngradeOptionStep)
}

// restartWithNewBinaryStep restarts a certain `node` with a new
// cockroach binary. Any existing `cockroach` process will be stopped,
// then the new binary will be uploaded and the `cockroach` process
// will restart using the new binary. This step only applies to the
// system tenant. For the (separate-process) multitenant equivalent,
// see `restartVirtualClusterStep`.
type restartWithNewBinaryStep struct {
	Version            *clusterupgrade.Version
	Node               int
	Settings           install.ClusterSettingOptionList `yaml:",omitempty"`
	InitTarget         int
	WaitForReplication bool
	TenantRunning      bool // whether the test tenant is running when this step is called
	DeploymentMode     DeploymentMode
}

func (s restartWithNewBinaryStep) Background(_ *Helper) shouldStop { return nil }

func (s restartWithNewBinaryStep) Description() string {
	var systemDesc string
	if s.DeploymentMode == SeparateProcessDeployment {
		systemDesc = " system server on"
	}

	return fmt.Sprintf(
		"restart%s node %d with binary version %s",
		systemDesc, s.Node, s.Version,
	)
}

func (s restartWithNewBinaryStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	customStartOpts := restartSystemSettings(s.WaitForReplication, s.InitTarget)

	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	settings := append([]install.ClusterSettingOption{
		install.TagOption(systemTag),
	}, s.Settings...)

	node := h.runner.cluster.Node(s.Node)
	if err := clusterupgrade.RestartNodesWithNewBinary(
		startCtx,
		h.runner.rt,
		l,
		h.runner.cluster,
		node,
		startOpts(customStartOpts...),
		s.Version,
		settings...,
	); err != nil {
		return err
	}

	if s.DeploymentMode == SharedProcessDeployment && s.TenantRunning {
		// If we are in shared-process mode and the tenant is already
		// running at this point, we wait for the server on the restarted
		// node to be up before moving on.
		return waitForTenantProcess(ctx, l, h, node, s.DeploymentMode)
	}

	return nil
}

func (s restartWithNewBinaryStep) ConcurrencyDisabled() bool {
	return true
}

func (s restartWithNewBinaryStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.RestartWithNewBinaryStep)
}

// allowUpgradeStep resets the `preserve_downgrade_option` cluster
// setting, allowing the upgrade migrations to run and the cluster
// version to eventually reach the binary version on the nodes.
type allowUpgradeStep struct {
	VirtualClusterName string
}

func (s allowUpgradeStep) Background(_ *Helper) shouldStop { return nil }

func (s allowUpgradeStep) Description() string {
	return fmt.Sprintf(
		"allow upgrade to happen on %s tenant by resetting `preserve_downgrade_option`",
		s.VirtualClusterName,
	)
}

func (s allowUpgradeStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	return serviceByName(h, s.VirtualClusterName).Exec(
		rng, "RESET CLUSTER SETTING cluster.preserve_downgrade_option",
	)
}

func (s allowUpgradeStep) ConcurrencyDisabled() bool {
	return false
}

func (s allowUpgradeStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.AllowUpgradeStep)
}

// waitStep does nothing but sleep for the provided duration. Most
// commonly used to allow the cluster to stay in a certain state
// before attempting node restarts or other upgrade events.
type waitStep struct {
	Dur time.Duration
}

func (s waitStep) Background(_ *Helper) shouldStop { return nil }

func (s waitStep) Description() string {
	return fmt.Sprintf("wait for %s", s.Dur)
}

func (s waitStep) Run(ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper) error {
	l.Printf("waiting for %s", s.Dur)
	select {
	case <-time.After(s.Dur):
	case <-ctx.Done():
	}

	return nil
}

func (s waitStep) ConcurrencyDisabled() bool {
	return false
}

func (s waitStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.WaitStep)
}

// runHookStep is a step used to run a user-provided hook (i.e.,
// callbacks passed to `OnStartup`, `InMixedVersion`, or `AfterTest`).
type runHookStep struct {
	Desc        string
	StepFuncRef stepFuncRef
}

func (s runHookStep) Background(h *Helper) shouldStop {
	sf := h.getStepFuncFromRef(s.StepFuncRef)
	return sf.shouldStop
}

func (s runHookStep) Description() string {
	return fmt.Sprintf("run %q", s.Desc)
}

func (s runHookStep) Run(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {
	sf := h.getStepFuncFromRef(s.StepFuncRef)
	return sf.fn(ctx, l, rng, h)
}

func (s runHookStep) ConcurrencyDisabled() bool {
	return false
}

func (s runHookStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.RunHookStep)
}

// setClusterSettingStep sets the cluster setting `name` to `value`.
type setClusterSettingStep struct {
	MinVersion         *clusterupgrade.Version
	Name               string
	Value              interface{}
	VirtualClusterName string
	SystemVisible      bool
}

func (s setClusterSettingStep) Background(_ *Helper) shouldStop { return nil }

func (s setClusterSettingStep) Description() string {
	return fmt.Sprintf(
		"set cluster setting %q to '%v' on %s tenant",
		s.Name, s.Value, s.VirtualClusterName,
	)
}

func (s setClusterSettingStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	// We set the cluster setting on the corresponding `virtualClusterName`.
	// However, if `systemVisible` is true, it means the setting
	// is only settable via the system interface.
	var tenantPrefix string
	serviceName := s.VirtualClusterName
	if s.SystemVisible {
		tenantPrefix = fmt.Sprintf("ALTER TENANT %q ", s.VirtualClusterName)
		serviceName = install.SystemInterfaceName
	}

	var stmt string
	var args []interface{}
	// We do a type switch on common types to avoid errors when using
	// placeholders, as type detection is often not implemented for some
	// private cluster settings.
	switch val := s.Value.(type) {
	case string:
		stmt = fmt.Sprintf("%sSET CLUSTER SETTING %s = '%s'", tenantPrefix, s.Name, val)
	case bool:
		stmt = fmt.Sprintf("%sSET CLUSTER SETTING %s = %t", tenantPrefix, s.Name, val)
	case int:
		stmt = fmt.Sprintf("%sSET CLUSTER SETTING %s = %d", tenantPrefix, s.Name, val)
	default:
		// If not using any of these types, do a best-effort attempt using
		// a placeholder.
		stmt = fmt.Sprintf("%sSET CLUSTER SETTING %s = $1", tenantPrefix, s.Name)
		args = []interface{}{val}
	}

	return serviceByName(h, serviceName).ExecWithGateway(
		rng, nodesRunningAtLeast(s.VirtualClusterName, s.MinVersion, h), stmt, args...,
	)
}

func (s setClusterSettingStep) ConcurrencyDisabled() bool {
	return false
}

func (s setClusterSettingStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.SetClusterSettingStep)
}

// setClusterVersionStep sets the special `version` cluster setting to
// the provided version.
type setClusterVersionStep struct {
	Version            *clusterupgrade.Version
	VirtualClusterName string
}

func (s setClusterVersionStep) Background(_ *Helper) shouldStop { return nil }

func (s setClusterVersionStep) Description() string {
	value := versionToClusterVersion(s.Version)
	if !s.Version.IsCurrent() {
		value = fmt.Sprintf("'%s'", value)
	}

	return fmt.Sprintf(
		"set `version` to %s on %s tenant",
		value, s.VirtualClusterName,
	)
}

func (s setClusterVersionStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	service := serviceByName(h, s.VirtualClusterName)
	binaryVersion := versionToClusterVersion(s.Version)
	if s.Version.IsCurrent() {
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

func (s setClusterVersionStep) ConcurrencyDisabled() bool {
	return false
}

func (s setClusterVersionStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.SetClusterVersionStep)
}

// resetClusterSetting resets cluster setting `name`.
type resetClusterSettingStep struct {
	MinVersion         *clusterupgrade.Version
	Name               string
	VirtualClusterName string
}

func (s resetClusterSettingStep) Background(_ *Helper) shouldStop { return nil }

func (s resetClusterSettingStep) Description() string {
	return fmt.Sprintf("reset cluster setting %q on %s tenant", s.Name, s.VirtualClusterName)
}

func (s resetClusterSettingStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	stmt := fmt.Sprintf("RESET CLUSTER SETTING %s", s.Name)
	return serviceByName(h, s.VirtualClusterName).ExecWithGateway(
		rng, nodesRunningAtLeast(s.VirtualClusterName, s.MinVersion, h), stmt,
	)
}

func (s resetClusterSettingStep) ConcurrencyDisabled() bool {
	return false
}

func (s resetClusterSettingStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.ResetClusterSettingStep)
}

// deleteAllTenantsVersionOverrideStep is a hack that deletes bad data
// from the `system.tenant_settings` table; specifically an
// all-tenants (tenant_id = 0) override for the 'version' key. See
// #125702 for more details.
//
// This allows us to continue testing virtual cluster upgrades to
// versions older than 24.2 without hitting this (already fixed) bug.
type deleteAllTenantsVersionOverrideStep struct{}

func (s deleteAllTenantsVersionOverrideStep) Background(_ *Helper) shouldStop { return nil }

func (s deleteAllTenantsVersionOverrideStep) Description() string {
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

func (s deleteAllTenantsVersionOverrideStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.DeleteAllTenantsVersionOverrideStep)
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
	VirtualClusterName string
}

func (s disableRateLimitersStep) Background(_ *Helper) shouldStop { return nil }

func (s disableRateLimitersStep) Description() string {
	return fmt.Sprintf("disable KV and tenant(SQL) rate limiter on %s tenant", s.VirtualClusterName)
}

func (s disableRateLimitersStep) Run(
	ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper,
) error {
	// Disable the KV rate limiter.
	stmt := fmt.Sprintf(
		"ALTER TENANT %q GRANT CAPABILITY exempt_from_rate_limiting = true",
		s.VirtualClusterName,
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
		s.VirtualClusterName, availableTokens, refillRate, maxBurstTokens,
	)

	if h.System.FromVersion.AtLeast(updateTenantResourceLimitsDeprecatedArgsVersion) {
		stmt = fmt.Sprintf(
			"SELECT crdb_internal.update_tenant_resource_limits('%s', %v, %v, %d);",
			s.VirtualClusterName, availableTokens, refillRate, maxBurstTokens,
		)
	}

	return h.System.Exec(rng, stmt)
}

func (s disableRateLimitersStep) ConcurrencyDisabled() bool {
	return false
}

func (s disableRateLimitersStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.DisableRateLimitersStep)
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

// TODO(kyleli): This step currently only affects the system tenant, should support panicking secondary tenants as well.
type panicNodeStep struct {
	InitTarget int
	TargetNode option.NodeListOption
}

func (s panicNodeStep) Background(_ *Helper) shouldStop { return nil }

func (s panicNodeStep) Description() string {
	return fmt.Sprintf("panicking system interface on node %d", s.TargetNode[0])
}

func (s panicNodeStep) Run(ctx context.Context, l *logger.Logger, rng *rand.Rand, h *Helper) error {

	h.runner.monitor.ExpectProcessDead(s.TargetNode)

	// ExecWithGateway cannot be used here because the monitor marks the target node as expected
	// dead, and it will be filtered out of the list of available nodes. This a unique case, so
	// we manually log the SQL statement and execute it directly on the target node.
	const query = "SELECT crdb_internal.force_panic('expected panic from panicNodeMutator')"
	db := h.System.Connect(s.TargetNode[0])

	v, err := h.System.NodeVersion(s.TargetNode[0])
	if err != nil {
		return errors.Wrapf(err, "failed to get node version for %d", s.TargetNode[0])
	}
	logSQL(
		h.System.stepLogger, s.TargetNode[0], v, h.System.Descriptor.Name, query,
	)

	if _, err = db.ExecContext(h.System.ctx, query); err == nil {
		return errors.Errorf("expected panic statement to fail, but it succeeded on %s", s.TargetNode)
	}

	return nil
}

func (s panicNodeStep) ConcurrencyDisabled() bool {
	return true
}

func (s panicNodeStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.PanicNodeStep)
}

// Restarts a dead node on the same binary version it was running, unlike
// `restartWithNewBinaryStep` which restarts an alive node with a new binary.
type restartNodeStep struct {
	InitTarget     int
	TargetNode     option.NodeListOption
	ExtDescription string
}

func (restartNodeStep) Background(_ *Helper) shouldStop { return nil }

func (s restartNodeStep) Description() string {
	return s.ExtDescription
}

func (s restartNodeStep) Run(ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper) error {
	nodeVersion, err := h.System.NodeVersion(s.TargetNode[0])
	if err != nil {
		return errors.Wrapf(err, "failed to get node version for %s", s.TargetNode)
	}
	binary := clusterupgrade.CockroachPathForVersion(h.runner.rt, nodeVersion)
	settings := install.MakeClusterSettings(
		install.BinaryOption(binary),
		install.TagOption(systemTag),
	)
	customStartOpts := restartSystemSettings(true, s.InitTarget)

	startCtx, cancel := context.WithTimeout(ctx, startTimeout)
	defer cancel()

	err = h.runner.cluster.StartE(
		startCtx,
		l,
		startOpts(customStartOpts...),
		settings,
		s.TargetNode,
	)
	if err != nil {
		return errors.Wrapf(
			err, "failed to restart node %d with binary %s", s.TargetNode[0], binary,
		)
	}
	return nil

}

func (s restartNodeStep) ConcurrencyDisabled() bool {
	return true
}

func (s restartNodeStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.RestartNodeStep)
}

type networkPartitionInjectStep struct {
	FailureRef string
	Partition  failures.NetworkPartition
	TargetNode option.NodeListOption
}

func (s networkPartitionInjectStep) Background(_ *Helper) shouldStop { return nil }

func (s networkPartitionInjectStep) Description() string {
	var desc string
	switch s.Partition.Type {
	case failures.Bidirectional:
		desc = fmt.Sprintf("setting up bidirectional network partition: dropping connections between nodes %d and %v", s.Partition.Source, s.Partition.Destination)
	case failures.Incoming:
		desc = fmt.Sprintf("setting up incoming network partition: dropping connections from nodes %v to %d", s.Partition.Destination, s.Partition.Source)
	case failures.Outgoing:
		desc = fmt.Sprintf("setting up outgoing network partition: dropping connections from nodes %d to %v", s.Partition.Source, s.Partition.Destination)
	}
	return desc
}

func (s networkPartitionInjectStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	f, err := h.getFailureFromRef(s.FailureRef, l)
	if err != nil {
		return err
	}

	h.runner.monitor.ExpectProcessDead(s.TargetNode)
	if h.Tenant != nil {
		opt := option.VirtualClusterName(h.Tenant.Descriptor.Name)
		h.runner.monitor.ExpectProcessDead(s.TargetNode, opt)
	}

	args := failures.NetworkPartitionArgs{Partitions: []failures.NetworkPartition{s.Partition}}

	if err := f.Setup(ctx, l, args); err != nil {
		return errors.Wrapf(err, "failed to setup failure %s", failures.IPTablesNetworkPartitionName)
	}

	if err := f.Inject(ctx, l, args); err != nil {
		return errors.Wrapf(err, "failed to inject failure %s", failures.IPTablesNetworkPartitionName)
	}

	return f.WaitForFailureToPropagate(ctx, l)
}

func (s networkPartitionInjectStep) ConcurrencyDisabled() bool {
	return true
}

func (s networkPartitionInjectStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.NetworkPartitionInjectStep)
}

type networkPartitionRecoveryStep struct {
	FailureRef string
	Partition  failures.NetworkPartition
	TargetNode option.NodeListOption
}

func (s networkPartitionRecoveryStep) Background(_ *Helper) shouldStop { return nil }

func (s networkPartitionRecoveryStep) Description() string {
	var desc string
	switch s.Partition.Type {
	case failures.Bidirectional:
		desc = fmt.Sprintf("recovering from bidirectional network partition: allowing connections between nodes %d and %v", s.Partition.Source, s.Partition.Destination)
	case failures.Incoming:
		desc = fmt.Sprintf("recovering from incoming network partition: allowing connections from nodes %v to %d", s.Partition.Destination, s.Partition.Source)
	case failures.Outgoing:
		desc = fmt.Sprintf("recovering from outgoing network partition: allowing connections from nodes %d to %v", s.Partition.Source, s.Partition.Destination)
	}
	return desc
}

func (s networkPartitionRecoveryStep) Run(
	ctx context.Context, l *logger.Logger, _ *rand.Rand, h *Helper,
) error {
	f, err := h.getFailureFromRef(s.FailureRef, l)
	if err != nil {
		return err
	}

	if err := f.Recover(ctx, l); err != nil {
		return errors.Wrapf(err, "failed to recover failure %s", failures.IPTablesNetworkPartitionName)
	}

	if err := f.WaitForFailureToRecover(ctx, l); err != nil {
		return errors.Wrapf(err, "failed to wait for recovery of failure %s", failures.IPTablesNetworkPartitionName)
	}

	h.runner.monitor.ExpectProcessAlive(s.TargetNode)
	if h.Tenant != nil {
		opt := option.VirtualClusterName(h.Tenant.Descriptor.Name)
		h.runner.monitor.ExpectProcessAlive(s.TargetNode, opt)
	}
	return f.Cleanup(ctx, l)

}

func (s networkPartitionRecoveryStep) ConcurrencyDisabled() bool {
	return false
}

func (s networkPartitionRecoveryStep) getTypeName(t *stepProtocolTypes) (string, error) {
	return t.getTypeName(s, &t.NetworkPartitionRecoveryStep)
}
