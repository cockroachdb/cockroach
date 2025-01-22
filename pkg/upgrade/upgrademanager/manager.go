// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package upgrademanager provides an implementation of upgrade.Manager
// for use on kv nodes.
package upgrademanager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/license"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/settingswatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/migrationstable"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradecluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradejob"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"github.com/cockroachdb/redact"
)

// Manager is the instance responsible for executing upgrades across the
// cluster.
type Manager struct {
	deps      upgrade.SystemDeps
	lm        *lease.Manager
	ie        isql.Executor
	jr        *jobs.Registry
	codec     keys.SQLCodec
	settings  *cluster.Settings
	knobs     upgradebase.TestingKnobs
	clusterID *base.ClusterIDContainer
	le        *license.Enforcer
}

// GetUpgrade returns the upgrade associated with this key.
func (m *Manager) GetUpgrade(key roachpb.Version) (upgradebase.Upgrade, bool) {
	if m.knobs.RegistryOverride != nil {
		if m, ok := m.knobs.RegistryOverride(key); ok {
			return m, ok
		}
	}
	return upgrades.GetUpgrade(key)
}

// SystemDeps returns dependencies to run system upgrades for the cluster
// associated with this manager. It may be the zero value in a secondary tenant.
func (m *Manager) SystemDeps() upgrade.SystemDeps {
	return m.deps
}

// NewManager constructs a new Manager. The SystemDeps parameter may be zero in
// secondary tenants. The testingKnobs parameter may be nil.
//
// TODO(ajwerner): Remove the ie argument given the isql.DB in deps.
func NewManager(
	deps upgrade.SystemDeps,
	lm *lease.Manager,
	ie isql.Executor,
	jr *jobs.Registry,
	codec keys.SQLCodec,
	settings *cluster.Settings,
	clusterID *base.ClusterIDContainer,
	testingKnobs *upgradebase.TestingKnobs,
	le *license.Enforcer,
) *Manager {
	var knobs upgradebase.TestingKnobs
	if testingKnobs != nil {
		knobs = *testingKnobs
	}
	return &Manager{
		deps:      deps,
		lm:        lm,
		ie:        ie,
		jr:        jr,
		codec:     codec,
		settings:  settings,
		clusterID: clusterID,
		le:        le,
		knobs:     knobs,
	}
}

var _ upgrade.JobDeps = (*Manager)(nil)

// safeToUpgradeTenant ensures that we don't allow for tenant upgrade if it's
// not safe to do so. Safety is defined as preventing a secondary tenant's
// cluster version from ever exceeding the storage cluster version. It is always
// safe to upgrade the system tenant.
func safeToUpgradeTenant(
	ctx context.Context,
	codec keys.SQLCodec,
	overrides cluster.OverridesInformer,
	tenantClusterVersion clusterversion.ClusterVersion,
) (bool, error) {
	if codec.ForSystemTenant() {
		// Unconditionally run upgrades for the system tenant.
		return true, nil
	}
	// The overrides informer can be nil, but only in a system tenant.
	if overrides == nil {
		return false, errors.AssertionFailedf("overrides informer is nil in secondary tenant")
	}
	storageClusterVersion := overrides.(*settingswatcher.SettingsWatcher).GetStorageClusterActiveVersion()
	if storageClusterVersion.Less(tenantClusterVersion.Version) {
		// We assert here if we find a tenant with a higher cluster version than
		// the storage cluster. It's dangerous to run in this mode because the
		// tenant may expect upgrades to be present on the storage cluster which
		// haven't yet been run. It's also not clear how we could get into this
		// state, since a tenant can't be created at a higher level
		// than the storage cluster, and will be prevented from upgrading beyond
		// the storage cluster version by the code below.
		return false, errors.AssertionFailedf("tenant found at higher cluster version "+
			"than storage cluster: storage cluster at version %v, tenant at version"+
			" %v.", storageClusterVersion, tenantClusterVersion)
	}
	if tenantClusterVersion == storageClusterVersion {
		// The cluster version of the tenant is equal to the cluster version of
		// the storage cluster. If we allow the upgrade it will push the tenant
		// to a higher cluster version than the storage cluster. Block the upgrade.
		return false, errors.Newf("preventing tenant upgrade from running "+
			"as the storage cluster has not yet been upgraded: "+
			"storage cluster version = %v, tenant cluster version = %v",
			storageClusterVersion, tenantClusterVersion)
	}

	// The cluster version of the tenant is less than that of the storage
	// cluster. It's safe to run the upgrade.
	log.Infof(ctx, "safe to upgrade tenant: storage cluster at version %v, tenant at version"+
		" %v", storageClusterVersion, tenantClusterVersion)
	return true, nil
}

// RunPermanentUpgrades runs all the upgrades associated with cluster versions
// <= upToVersion that are marked as permanent. Upgrades that have already run
// to completion, or that are currently running, are not run again, but the call
// will block for their completion.
//
// NOTE: All upgrades, permanent and non-permanent, end up running in order.
// RunPermanentUpgrades(v1) is called before Migrate(v1,v2). Of course,
// non-permanent upgrades for versions <= v1 are not run at all; they're assumed
// to be baked into the bootstrap metadata.
func (m *Manager) RunPermanentUpgrades(ctx context.Context, upToVersion roachpb.Version) error {
	log.Infof(ctx, "running permanent upgrades up to version: %v", upToVersion)
	defer func() {
		if fn := m.knobs.AfterRunPermanentUpgrades; fn != nil {
			fn()
		}
	}()
	vers := m.listBetween(roachpb.Version{}, upToVersion)
	var permanentUpgrades []upgradebase.Upgrade
	for _, v := range vers {
		u, exists := m.GetUpgrade(v)
		if !exists || !u.Permanent() {
			continue
		}
		_, isSystemUpgrade := u.(*upgrade.SystemUpgrade)
		if isSystemUpgrade && !m.codec.ForSystemTenant() {
			continue
		}
		permanentUpgrades = append(permanentUpgrades, u)
	}

	user := username.NodeUserName()

	if len(permanentUpgrades) == 0 {
		// If we didn't find any permanent migrations, it must be that a test used
		// some the testing knobs to inhibit us from finding the migrations.
		// However, we must run the permanent migrations (at least the one writing
		// the value of the cluster version to the system.settings table); the test
		// did not actually mean to inhibit running these. So we'll run them anyway,
		// without using jobs, so that the side effect of the migrations are
		// minimized and tests continue to be happy as they were before the
		// permanent migrations were introduced.
		// We use WithoutChecks here as this is test only code and we don't want
		// to blindly retry multiple migrations in one go.
		return m.runPermanentMigrationsWithoutJobsForTests(startup.WithoutChecks(ctx), user)
	}

	// Do a best-effort check to see if all upgrades have already executed and so
	// there's nothing for us to do. Probably the most common case is that a node
	// is started in a cluster that has already run all the relevant upgrades, in
	// which case we'll figure this out cheaply.
	// We look at whether the last permanent upgrade that we need to run has
	// already completed successfully. Looking only at the last one is sufficient
	// because upgrades run in order.
	latest := permanentUpgrades[len(permanentUpgrades)-1]
	lastVer := latest.Version()
	enterpriseEnabled := base.CCLDistributionAndEnterpriseEnabled(m.settings)
	lastUpgradeCompleted, err := startup.RunIdempotentWithRetryEx(ctx,
		m.deps.Stopper.ShouldQuiesce(),
		"check if migration completed",
		func(ctx context.Context) (bool, error) {
			return migrationstable.CheckIfMigrationCompleted(
				ctx, lastVer, nil /* txn */, m.ie,
				// We'll do a follower read. This is all best effort anyway, and the
				// follower read should keep the startup time low in the common case where
				// all upgrades have run a long time ago before this node start.
				enterpriseEnabled,
				migrationstable.StaleRead)
		})
	if err != nil {
		return err
	}
	if lastUpgradeCompleted {
		log.Infof(ctx,
			"detected the last permanent upgrade (v%s) to have already completed; no permanent upgrades will run",
			lastVer)
		return nil
	} else {
		// The stale read said that the upgrades had not run. Let's try a consistent read too since
		log.Infof(ctx,
			"the last permanent upgrade (v%s) does not appear to have completed; attempting to run all upgrades",
			lastVer)
	}

	for _, u := range permanentUpgrades {
		log.Infof(ctx, "running permanent upgrade for version %s", u.Version())
		if err := m.runMigration(ctx, u, user, u.Version(), !m.knobs.DontUseJobs); err != nil {
			return err
		}
	}
	return nil
}

// runPermanentMigrationsWithoutJobsForTests runs all permanent migrations up to
// VBootstrapMax. They are run without jobs, in order to minimize the side
// effects left on cluster.
//
// NOTE: VBootstrapMax was chosen arbitrarily, since we don't have a great way
// to tell which migrations are needed and which aren't on the code path leading
// here.
func (m *Manager) runPermanentMigrationsWithoutJobsForTests(
	ctx context.Context, user username.SQLUsername,
) error {
	log.Infof(ctx, "found test configuration that eliminated all upgrades; running permanent upgrades anyway")
	vers := clusterversion.ListBetween(roachpb.Version{}, clusterversion.VBootstrapMax.Version())
	for _, v := range vers {
		upg, exists := upgrades.GetUpgrade(v)
		if !exists || !upg.Permanent() {
			continue
		}
		if err := m.runMigration(ctx, upg, user, upg.Version(), false /* useJob */); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) postToPauseChannelAndWaitForResume(ctx context.Context) {
	log.Infof(ctx, "pausing at pause point %d", m.knobs.InterlockPausePoint)
	// To handle the case where the pause point is hit on every migration (which
	// is the common case), reset the interlock pause point when woken up so
	// that we won't sleep again.
	m.knobs.InterlockPausePoint = upgradebase.NoPause

	// Post to the pause point channel.
	select {
	case *m.knobs.InterlockReachedPausePointChannel <- struct{}{}:
	case <-m.deps.Stopper.ShouldQuiesce():
		return
	}

	// Wait on the resume channel.
	select {
	case <-*m.knobs.InterlockResumeChannel:
	case <-m.deps.Stopper.ShouldQuiesce():
		return
	}
}

func (m *Manager) Migrate(
	ctx context.Context,
	user username.SQLUsername,
	from, to clusterversion.ClusterVersion,
	updateSystemVersionSetting sql.UpdateVersionSystemSettingHook,
) (returnErr error) {
	// TODO(irfansharif): Should we inject every ctx here with specific labels
	// for each upgrade, so they log distinctly?
	ctx = logtags.AddTag(ctx, "migration-mgr", nil)
	defer func() {
		if returnErr != nil {
			log.Warningf(ctx, "error encountered during version upgrade: %v", returnErr)
		}
	}()

	if from == to {
		// Nothing to do here.
		log.Infof(ctx, "no need to migrate, cluster already at newest version")
		return nil
	}

	rng, _ := randutil.NewPseudoRand()

	// Validation functions for updating the settings table. We use this in the
	// tenant upgrade case to ensure that no new SQL servers were started
	// mid-upgrade, with versions that are incompatible with the attempted
	// upgrade (because their binary version is too low).
	validate := func(ctx context.Context, txn *kv.Txn) error {
		return m.deps.Cluster.ValidateAfterUpdateSystemVersion(ctx, txn)
	}
	skipValidation := func(ctx context.Context, txn *kv.Txn) error {
		return nil
	}

	// Determine whether it's safe to perform the upgrade for secondary tenants.
	if safe, err := safeToUpgradeTenant(ctx, m.codec, m.settings.OverridesInformer, from); !safe {
		return err
	}

	clusterVersions := m.listBetween(from.Version, to.Version)
	log.Infof(ctx, "migrating cluster from %s to %s (stepping through %s)", from, to, clusterVersions)
	if len(clusterVersions) == 0 {
		if buildutil.CrdbTestBuild && from.Version != to.Version {
			// This suggests a test is using bogus versions and didn't set up
			// ListBetweenOverride properly.
			panic(errors.AssertionFailedf("no valid versions in (%s, %s]", from.Version, to.Version))
		}
		return nil
	}

	// We only need to persist the first fence to the settings table for
	// secondary tenant upgrades, and even then, only for the first migration
	// performed in the loop below.
	mustPersistFenceVersion := !m.codec.ForSystemTenant()

	// Sanity check that we'll actually be able to perform the real
	// cluster version bump, cluster-wide, before potentially creating a job
	// that might be doomed to fail.
	{
		finalVersion := clusterVersions[len(clusterVersions)-1]
		if err := validateTargetClusterVersion(ctx, m.deps.Cluster, clusterversion.ClusterVersion{Version: finalVersion}); err != nil {
			return err
		}
		if m.knobs.InterlockPausePoint == upgradebase.AfterFirstCheckForInstances {
			m.postToPauseChannelAndWaitForResume(ctx)
		}
	}

	if err := m.checkPreconditions(ctx, clusterVersions); err != nil {
		return err
	}

	// The loop below runs the actual migrations and pushes out the version gate
	// to every server (SQL server in the case of secondary tenants, or
	// combined KV/Storage server in the case of the storage layer) in the
	// cluster. Each server will persist the version, bump the local
	// version gates, and then return. The upgrade associated with the specific
	// version is executed before any server in the cluster has the
	// corresponding version activated. Migrations that depend on a certain
	// version already being activated will need to register using a cluster
	// version greater than it.
	//
	// For each intermediate version, we'll need to first bump the fence
	// version before bumping the "real" one. Doing so allows us to provide
	// the invariant that whenever a cluster version is active, all servers
	// in the cluster (including ones added concurrently during version
	// upgrades) are running binaries that know about the version. This is
	// discussed in greater detail below in the [Fence versions] section.
	//
	// Note: tenants don't persist their version information anywhere aside from
	// in the settings table. Nodes however, persist the version information in
	// each store. As a result, the section below (up to "Fence versions")
	// applies to storage cluster upgrades only.
	//
	// # Storage cluster upgrades
	//
	// Below-raft upgrades mutate replica state, making use of the
	// Migrate(version=V) primitive which they issue against the entire
	// keyspace. These upgrades typically want to rely on the invariant
	// that there are no extant replicas in the system that haven't seen the
	// specific Migrate command.
	//
	// This is partly achieved through the implementation of the Migrate
	// command itself, which waits until it's applied on all followers[2]
	// before returning. This also addresses the concern of extant snapshots
	// with pre-migrated state possibly instantiating older version
	// replicas. The intended learner replicas are listed as part of the
	// range descriptor, and is also waited on for during command
	// application. As for stale snapshots, if they specify a replicaID
	// that's no longer part of the raft group, they're discarded by the
	// recipient. Snapshots are also discarded unless they move the LAI
	// forward.
	//
	// That still leaves rooms for replicas in the replica GC queue to evade
	// detection. To address this, below-raft upgrades typically take a
	// two-phrase approach (the TruncatedAndRangeAppliedStateMigration being
	// one example of this), where after having migrated the entire keyspace
	// to version V, and after having prevented subsequent snapshots
	// originating from replicas with versions < V, the upgrade sets out
	// to purge outdated replicas in the system[3]. Specifically it
	// processes all replicas in the GC queue with a version < V (which are
	// not accessible during the application of the Migrate command).
	//
	// [1]: See ReplicaState.Version.
	// [2]: See Replica.executeWriteBatch, specifically how proposals with the
	//      Migrate request are handled downstream of raft.
	// [3]: See PurgeOutdatedReplicas from the SystemUpgrade service.
	//
	// # Fence versions
	//
	// The upgrade infrastructure makes use of internal fence
	// versions when stepping through consecutive versions. It's
	// instructive to walk through how we expect a version upgrade
	// from v21.1 to v21.2 to take place, and how we behave in the
	// presence of new v21.1 or v21.2 servers being added to the cluster.
	//
	//   - All servers are running v21.1
	//   - All servers are rolled into v21.2 binaries, but with active
	//     cluster version still as v21.1
	//   - The first version bump will be into v21.2-1(fence), see the
	//     upgrade manager above for where that happens
	//
	// Then concurrently:
	//
	//   - A new server is added to the cluster, but running binary v21.1
	//   - We try bumping the cluster gates to v21.2-1(fence)
	//
	// If the v21.1 server manages to sneak in before the version bump,
	// it's fine as the version bump is a no-op one (all fence versions
	// are). Any subsequent bumps (including the "actual" one bumping to
	// v21.2) will fail during the validation step where we'll first
	// check to see that all servers are running v21.2 binaries.
	//
	// If the v21.1 server is only added after v21.2-1(fence) is active,
	// it won't be able to actually join the cluster (it'll be prevented
	// by the join RPC or, in the tenant case, by the check in preStart that
	// the binary version of the SQL server is compatible with that of the
	// tenant active version.
	//
	// All of which is to say that once we've seen the servers list
	// stabilize (as UntilClusterStable enforces), any new servers that
	// can join the cluster will run a release that support the fence
	// version, and by design also supports the actual version (which is
	// the direct successor of the fence).
	for _, clusterVersion := range clusterVersions {
		log.Infof(ctx, "stepping through %s", clusterVersion)

		cv := clusterversion.ClusterVersion{Version: clusterVersion}

		fenceVersion := cv.FenceVersion()
		if err := bumpClusterVersion(ctx, m.deps.Cluster, fenceVersion); err != nil {
			return err
		}
		if m.knobs.InterlockPausePoint == upgradebase.AfterFenceRPC {
			m.postToPauseChannelAndWaitForResume(ctx)
		}

		// In the case where we're upgrading secondary tenants there's an extra
		// dance that must be performed. After we write the fence version to
		// the settings table we must validate that the set of SQL servers
		// running, matches those that were present when we performed the bump
		// above. This is to handle the case where a SQL server with an old
		// binary starts up in between the bump and the persistence of the fence
		// version. In that case, the SQL server will be permitted to startup,
		// but the upgrade can not continue. The retry logic here will detect
		// the SQL server with the old binary, and the upgrade will fail. If
		// instead, the newly started SQL server has a binary version which is
		// upgrade-compatible, the retry will detect that and the upgrade will
		// proceed. Note that we only need to do this extra dance when we write
		// the first fence of a given upgrade process, since for subsequent
		// fences, the too-low-binary SQL server will be prevented from starting
		// by the check in preStart.
		if mustPersistFenceVersion {
			var err error
			for {
				err = updateSystemVersionSetting(ctx, fenceVersion, validate)
				if errors.Is(err, upgradecluster.InconsistentSQLServersError) {
					if err := bumpClusterVersion(ctx, m.deps.Cluster, fenceVersion); err != nil {
						return err
					}
					continue
				}
				if err != nil {
					return err
				}
				break
			}
			mustPersistFenceVersion = false
		}
		if m.knobs.InterlockPausePoint == upgradebase.AfterFenceWriteToSettingsTable {
			m.postToPauseChannelAndWaitForResume(ctx)
		}

		// Now sanity check that we'll actually be able to perform the real
		// cluster version bump, cluster-wide.
		if err := validateTargetClusterVersion(ctx, m.deps.Cluster, cv); err != nil {
			return err
		}
		if m.knobs.InterlockPausePoint == upgradebase.AfterSecondCheckForInstances {
			m.postToPauseChannelAndWaitForResume(ctx)
		}

		// Run the actual upgrade, if any.
		mig, exists := m.GetUpgrade(clusterVersion)
		if exists {
			for {
				if err := m.runMigration(ctx, mig, user, clusterVersion, !m.knobs.DontUseJobs); err != nil {
					return err
				}
				if !buildutil.CrdbTestBuild || rng.Float64() < 0.5 {
					// To ensure that migrations are idempotent we'll run each
					// migration random number of times in test builds.
					break
				}
			}
		}

		// Bump the version of the system database schema if this is the final
		// version for a release.
		// NB: The final version never has an associated migration, which is why we
		// bump the SystemDatabaseSchemaVersion here; we cannot do it inside of
		// runMigration.
		if clusterVersion.Equal(clusterversion.Latest.Version()) && clusterVersion.IsFinal() {
			if err := upgrade.BumpSystemDatabaseSchemaVersion(ctx, cv.Version, m.deps.DB); err != nil {
				return err
			}
		}

		if m.knobs.InterlockPausePoint == upgradebase.AfterMigration {
			m.postToPauseChannelAndWaitForResume(ctx)
		}

		// Finally, bump the real version cluster-wide.
		err := bumpClusterVersion(ctx, m.deps.Cluster, cv)
		if err != nil {
			return err
		}
		if m.knobs.InterlockPausePoint == upgradebase.AfterVersionBumpRPC {
			m.postToPauseChannelAndWaitForResume(ctx)
		}

		// Updates the version info inside the tenant or host cluster's
		// (system tenant) settings table.
		err = updateSystemVersionSetting(ctx, cv, skipValidation)
		if err != nil {
			return err
		}
		if m.knobs.InterlockPausePoint == upgradebase.AfterVersionWriteToSettingsTable {
			m.postToPauseChannelAndWaitForResume(ctx)
		}
	}

	return nil
}

// bumpClusterVersion will invoke the BumpClusterVersion rpc on every node
// until the cluster is stable.
func bumpClusterVersion(
	ctx context.Context, c upgrade.Cluster, clusterVersion clusterversion.ClusterVersion,
) error {
	req := &serverpb.BumpClusterVersionRequest{ClusterVersion: &clusterVersion}
	op := fmt.Sprintf("bump-cluster-version=%s", req.ClusterVersion.PrettyPrint())
	return forEveryNodeUntilClusterStable(ctx, op, c, func(
		ctx context.Context, client serverpb.MigrationClient,
	) error {
		_, err := client.BumpClusterVersion(ctx, req)
		return err
	})
}

// bumpClusterVersion will invoke the ValidateTargetClusterVersion rpc on
// every node until the cluster is stable.
func validateTargetClusterVersion(
	ctx context.Context, c upgrade.Cluster, clusterVersion clusterversion.ClusterVersion,
) error {
	req := &serverpb.ValidateTargetClusterVersionRequest{ClusterVersion: &clusterVersion}
	op := fmt.Sprintf("validate-cluster-version=%s", req.ClusterVersion.PrettyPrint())
	return forEveryNodeUntilClusterStable(ctx, op, c, func(
		tx context.Context, client serverpb.MigrationClient,
	) error {
		_, err := client.ValidateTargetClusterVersion(ctx, req)
		// The tenant upgrade interlock is new in 23.1, as a result, before
		// 23.1 there was no Migration RPC for tenants. If we see the error that
		// no Migration RPC has been started, we assume that the tenant pods
		// in question are running on a version that predates 23.1, and return
		// that friendlier error instead. This code can be removed when we no
		// longer expect to see tenant pods running below version 23.1.
		// TODO(ajstorm): once the multitenant-upgrade test runs cleanly, make
		//  this error more structured.
		if err != nil && strings.Contains(err.Error(), "unknown service cockroach.server.serverpb.Migration") {
			err = errors.HandledWithMessage(err, "validate cluster version failed: some tenant pods running on binary less than 23.1")
		}
		return errors.WithHint(errors.Wrapf(err, "error validating the version of one or more SQL server instances"),
			"check the binary versions of all running SQL server instances to ensure that they are compatible with the attempted upgrade version")
	})
}

func forEveryNodeUntilClusterStable(
	ctx context.Context,
	op string,
	c upgrade.Cluster,
	f func(ctx context.Context, client serverpb.MigrationClient) error,
) error {
	log.Infof(ctx, "executing operation %s", redact.Safe(op))
	return c.UntilClusterStable(ctx, retry.Options{
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     1 * time.Second,
		Multiplier:     1.0,
		MaxRetries:     60, // retry for 60 seconds
	}, func() error {
		return c.ForEveryNodeOrServer(ctx, op, f)
	})
}

func (m *Manager) runMigration(
	ctx context.Context,
	mig upgradebase.Upgrade,
	user username.SQLUsername,
	version roachpb.Version,
	useJob bool,
) error {
	_, isSystemMigration := mig.(*upgrade.SystemUpgrade)
	if isSystemMigration && !m.codec.ForSystemTenant() {
		return nil
	}
	if !useJob {
		// Some tests don't like it when jobs are run at server startup, because
		// they pollute the jobs table. So, we run the upgrade directly.
		// To run jobs synchronously we must also disable startup retry assertions
		// since this code doesn't do retries and we also don't want to complicate
		// test only code.
		ctx := startup.WithoutChecks(ctx)
		v := mig.Version()
		alreadyCompleted, err := migrationstable.CheckIfMigrationCompleted(
			ctx, version, nil /* txn */, m.ie, false /* enterpriseEnabled */, migrationstable.ConsistentRead,
		)
		if alreadyCompleted || err != nil {
			return err
		}

		switch upg := mig.(type) {
		case *upgrade.SystemUpgrade:
			if err := upg.Run(ctx, v, m.SystemDeps()); err != nil {
				return err
			}
		case *upgrade.TenantUpgrade:
			// The TenantDeps used here are incomplete, but enough for the "permanent
			// upgrades" that run under this testing knob.
			if err := upg.Run(ctx, v, upgrade.TenantDeps{
				KVDB:               m.deps.DB.KV(),
				DB:                 m.deps.DB,
				Codec:              m.codec,
				Settings:           m.settings,
				LeaseManager:       m.lm,
				InternalExecutor:   m.ie,
				JobRegistry:        m.jr,
				TestingKnobs:       &m.knobs,
				ClusterID:          m.clusterID.Get(),
				LicenseEnforcer:    m.le,
				TenantInfoAccessor: m.deps.TenantInfoAccessor,
			}); err != nil {
				return err
			}
		}

		if err := migrationstable.MarkMigrationCompleted(ctx, m.ie, v); err != nil {
			return err
		}
		// Bump the version of the system database schema whenever we run a
		// non-permanent migration.
		if !mig.Permanent() {
			if err := upgrade.BumpSystemDatabaseSchemaVersion(ctx, v, m.deps.DB); err != nil {
				return err
			}
		}
		return nil
	} else {
		// Run a job that, in turn, will run the upgrade. By running upgrades inside
		// jobs, we get some observability for them and we avoid multiple nodes
		// attempting to run the same upgrade all at once. Particularly for
		// long-running upgrades, this is useful.
		//
		// If the job already exists, we wait for it to finish.
		var (
			alreadyCompleted, alreadyExisting bool
			id                                jobspb.JobID
		)
		if err := startup.RunIdempotentWithRetry(ctx,
			m.deps.Stopper.ShouldQuiesce(),
			"upgrade create job", func(ctx context.Context) (err error) {
				alreadyCompleted, alreadyExisting, id, err = m.getOrCreateMigrationJob(ctx, user, version,
					mig.Name())
				return err
			}); alreadyCompleted || err != nil {
			return err
		}
		if alreadyExisting {
			log.Infof(ctx, "waiting for %s", redact.Safe(mig.Name()))
			return startup.RunIdempotentWithRetry(ctx,
				m.deps.Stopper.ShouldQuiesce(),
				"upgrade wait jobs", func(ctx context.Context) error {
					return m.jr.WaitForJobs(ctx, []jobspb.JobID{id})
				})
		} else {
			log.Infof(ctx, "running %s", redact.Safe(mig.Name()))
			return startup.RunIdempotentWithRetry(ctx,
				m.deps.Stopper.ShouldQuiesce(),
				"upgrade run jobs", func(ctx context.Context) error {
					return m.jr.Run(ctx, []jobspb.JobID{id})
				})
		}
	}
}

func (m *Manager) getOrCreateMigrationJob(
	ctx context.Context, user username.SQLUsername, version roachpb.Version, name string,
) (alreadyCompleted, alreadyExisting bool, jobID jobspb.JobID, _ error) {
	newJobID := m.jr.MakeJobID()
	if err := m.deps.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		enterpriseEnabled := base.CCLDistributionAndEnterpriseEnabled(m.settings)
		alreadyCompleted, err = migrationstable.CheckIfMigrationCompleted(
			ctx, version, txn.KV(), txn, enterpriseEnabled, migrationstable.ConsistentRead,
		)
		if err != nil && ctx.Err() == nil {
			log.Warningf(ctx, "failed to check if migration already completed: %v", err)
		}
		if err != nil || alreadyCompleted {
			return err
		}
		alreadyExisting, jobID, err = m.getRunningMigrationJob(ctx, txn, version)
		if err != nil || alreadyExisting {
			return err
		}

		jobID = newJobID
		_, err = m.jr.CreateJobWithTxn(ctx, upgradejob.NewRecord(version, user, name), jobID, txn)
		return err
	}); err != nil {
		return false, false, 0, err
	}
	return alreadyCompleted, alreadyExisting, jobID, nil
}

const (
	// PostJobInfoQuery avoids the crdb_internal.system_jobs table
	// to avoid expensive full scans.
	// Exported for testing.
	PostJobInfoTableQuery = `
WITH
running_migration_jobs AS (
    SELECT id, status
    FROM system.jobs
    WHERE status IN ` + jobs.NonTerminalStateTupleString + `
    AND job_type = 'MIGRATION'
),
payloads AS (
    SELECT job_id, value
    FROM system.job_info AS payload
    WHERE info_key = 'legacy_payload'
    AND job_id IN (SELECT id FROM running_migration_jobs)
    ORDER BY written DESC
)
SELECT id, status FROM (
    SELECT distinct(id), status, crdb_internal.pb_to_json('cockroach.sql.jobs.jobspb.Payload', payloads.value, false) AS pl
    FROM running_migration_jobs AS j
    INNER JOIN payloads ON j.id = payloads.job_id
) WHERE ((pl->'migration')->'clusterVersion') = $1::JSONB`
)

func (m *Manager) getRunningMigrationJob(
	ctx context.Context, txn isql.Txn, version roachpb.Version,
) (found bool, jobID jobspb.JobID, _ error) {
	// Wrap the version into a ClusterVersion so that the JSON looks like what the
	// Payload proto has inside.
	cv := clusterversion.ClusterVersion{Version: version}
	query := PostJobInfoTableQuery
	jsonMsg, err := protoreflect.MessageToJSON(&cv, protoreflect.FmtFlags{EmitDefaults: false})
	if err != nil {
		return false, 0, errors.Wrap(err, "failed to marshal version to JSON")
	}
	rows, err := txn.QueryBuffered(ctx, "migration-manager-find-jobs", txn.KV(), query, jsonMsg.String())
	if err != nil {
		return false, 0, err
	}
	parseRow := func(row tree.Datums) (id jobspb.JobID, status jobs.State) {
		return jobspb.JobID(*row[0].(*tree.DInt)), jobs.State(*row[1].(*tree.DString))
	}
	switch len(rows) {
	case 0:
		return false, 0, nil
	case 1:
		id, status := parseRow(rows[0])
		log.Infof(ctx, "found existing migration job %d for version %v in status %s, waiting",
			id, &version, status)
		return true, id, nil
	default:
		var buf redact.StringBuilder
		buf.Printf("found multiple non-terminal jobs for version %s: [", redact.Safe(&version))
		for i, row := range rows {
			if i > 0 {
				buf.SafeString(", ")
			}
			id, status := parseRow(row)
			buf.Printf("(%d, %s)", id, redact.Safe(status))
		}
		log.Errorf(ctx, "%s", buf)
		return false, 0, errors.AssertionFailedf("%s", buf)
	}
}

func (m *Manager) listBetween(from roachpb.Version, to roachpb.Version) []roachpb.Version {
	if m.knobs.ListBetweenOverride != nil {
		result := m.knobs.ListBetweenOverride(from, to)
		// Sanity check result to catch invalid overrides.
		for _, v := range result {
			if v.LessEq(from) || to.Less(v) {
				panic(fmt.Sprintf("ListBetweenOverride(%s, %s) returned invalid version %s", from, to, v))
			}
		}
		return result
	}
	return clusterversion.ListBetween(from, to)
}

// checkPreconditions runs the precondition check for each tenant upgrade
// associated with the provided versions.
func (m *Manager) checkPreconditions(ctx context.Context, versions []roachpb.Version) error {
	for _, v := range versions {
		mig, ok := m.GetUpgrade(v)
		if !ok {
			continue
		}
		tm, ok := mig.(*upgrade.TenantUpgrade)
		if !ok {
			continue
		}
		if err := tm.Precondition(ctx, clusterversion.ClusterVersion{Version: v}, upgrade.TenantDeps{
			DB:                 m.deps.DB,
			Codec:              m.codec,
			Settings:           m.settings,
			LeaseManager:       m.lm,
			InternalExecutor:   m.ie,
			JobRegistry:        m.jr,
			TestingKnobs:       &m.knobs,
			ClusterID:          m.clusterID.Get(),
			LicenseEnforcer:    m.le,
			TenantInfoAccessor: m.deps.TenantInfoAccessor,
		}); err != nil {
			return errors.Wrapf(
				err,
				"verifying precondition for version %s",
				redact.SafeString(v.PrettyPrint()),
			)
		}
	}
	return nil
}
