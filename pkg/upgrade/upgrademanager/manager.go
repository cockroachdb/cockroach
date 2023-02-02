// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package upgrademanager provides an implementation of upgrade.Manager
// for use on kv nodes.
package upgrademanager

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
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
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradejob"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgrades"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
	clusterID uuid.UUID
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
	clusterID uuid.UUID,
	testingKnobs *upgradebase.TestingKnobs,
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
	storageClusterVersion := overrides.(*settingswatcher.SettingsWatcher).GetStorageClusterVersion()
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

	user := username.RootUserName()

	if len(permanentUpgrades) == 0 {
		// If we didn't find any permanent migrations, it must be that a test used
		// some the testing knobs to inhibit us from finding the migrations.
		// However, we must run the permanent migrations (at least the one writing
		// the value of the cluster version to the system.settings table); the test
		// did not actually mean to inhibit running these. So we'll run them anyway,
		// without using jobs, so that the side effect of the migrations are
		// minimized and tests continue to be happy as they were before the
		// permanent migrations were introduced.
		return m.runPermanentMigrationsWithoutJobsForTests(ctx, user)
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
	enterpriseEnabled := base.CCLDistributionAndEnterpriseEnabled(m.settings, m.clusterID)
	lastUpgradeCompleted, err := migrationstable.CheckIfMigrationCompleted(
		ctx, lastVer, nil /* txn */, m.ie,
		// We'll do a follower read. This is all best effort anyway, and the
		// follower read should keep the startup time low in the common case where
		// all upgrades have run a long time ago before this node start.
		enterpriseEnabled,
		migrationstable.StaleRead)
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
			"the last last permanent upgrade (v%s) does not appear to have completed; attempting to run all upgrades",
			lastVer)
	}

	for _, u := range permanentUpgrades {
		// Check whether a 22.2 or older node has ran the old startupmigration
		// corresponding to this upgrade. If it has, we don't want to run this
		// upgrade, for two reasons:
		// 1. Creating a job for this upgrade would be dubious. If the respective
		//    job were to be adopted by a 22.2 node, that node would fail to find an
		//    upgrade for the respective version, and would declare the job to be
		//    successful even though the upgrade didn't run. Even though that would
		//    technically be OK, since the existence of a 22.2 node running the job
		//    implies that the startupmigration had been run, it's confusing at the
		//    very least.
		// 2. The upgrade can fail if the corresponding startupmigration had already
		//    run. This is because the upgrade is assuming that the cluster has been
		//    bootstrapped at the current binary version, with the current schema
		//    for system tables. See a discussion in upgrade/doc.go about why that
		//    would be.
		//
		// TODO(andrei): Get rid of this once compatibility with 22.2 is not necessary.
		startupMigrationAlreadyRan, err := checkOldStartupMigrationRan(
			ctx, u.V22_2StartupMigrationName(), m.deps.DB.KV(), m.codec)
		if err != nil {
			return err
		}
		if startupMigrationAlreadyRan {
			log.Infof(ctx,
				"skipping permanent upgrade for v%s because the corresponding startupmigration "+
					"was already run by a v22.2 or older node",
				u.Version())
			// Mark the upgrade as completed so that we can get rid of this logic when
			// compatibility with 22.2 is no longer necessary.
			if err := migrationstable.MarkMigrationCompletedIdempotent(ctx, m.ie, u.Version()); err != nil {
				return err
			}
			continue
		}

		log.Infof(ctx, "running permanent upgrade for version %s", u.Version())
		if err := m.runMigration(ctx, u, user, u.Version(), !m.knobs.DontUseJobs); err != nil {
			return err
		}
	}
	return nil
}

// Check whether this is a cluster upgraded from a pre-23.1 version and the
// old startupmigration with the given name has run. If it did, the
// corresponding upgrade should not run.
func checkOldStartupMigrationRan(
	ctx context.Context, migrationName string, db *kv.DB, codec keys.SQLCodec,
) (bool, error) {
	if migrationName == "" {
		return false, nil
	}
	migrationKey := append(codec.StartupMigrationKeyPrefix(), roachpb.RKey(migrationName)...)
	kv, err := db.Get(ctx, migrationKey)
	if err != nil {
		return false, err
	}
	return kv.Exists(), nil
}

// runPermanentMigrationsWithoutJobsForTests runs all permanent migrations up to
// VPrimordialMax. They are run without jobs, in order to minimize the side
// effects left on cluster.
//
// NOTE: VPrimordialMax was chosen arbitrarily, since we don't have a great way
// to tell which migrations are needed and which aren't on the code path leading
// here.
func (m *Manager) runPermanentMigrationsWithoutJobsForTests(
	ctx context.Context, user username.SQLUsername,
) error {
	log.Infof(ctx, "found test configuration that eliminated all upgrades; running permanent upgrades anyway")
	vers := clusterversion.ListBetween(roachpb.Version{}, clusterversion.ByKey(clusterversion.VPrimordialMax))
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

	// Determine whether it's safe to perform the upgrade for secondary tenants.
	if safe, err := safeToUpgradeTenant(ctx, m.codec, m.settings.OverridesInformer, from); !safe {
		return err
	}

	clusterVersions := m.listBetween(from.Version, to.Version)
	log.Infof(ctx, "migrating cluster from %s to %s (stepping through %s)", from, to, clusterVersions)
	if len(clusterVersions) == 0 {
		return nil
	}

	// Sanity check that we'll actually be able to perform the real
	// cluster version bump, cluster-wide, before potentially creating a job
	// that might be doomed to fail.
	{
		finalVersion := clusterVersions[len(clusterVersions)-1]
		if err := validateTargetClusterVersion(ctx, m.deps.Cluster, clusterversion.ClusterVersion{Version: finalVersion}); err != nil {
			return err
		}
	}

	if err := m.checkPreconditions(ctx, clusterVersions); err != nil {
		return err
	}

	for _, clusterVersion := range clusterVersions {
		log.Infof(ctx, "stepping through %s", clusterVersion)
		cv := clusterversion.ClusterVersion{Version: clusterVersion}
		// First, run the actual upgrade if any.
		mig, exists := m.GetUpgrade(clusterVersion)
		if exists {
			if err := m.runMigration(ctx, mig, user, clusterVersion, !m.knobs.DontUseJobs); err != nil {
				return err
			}
		}

		// Next we'll push out the version gate to every node in the cluster.
		// Each node will persist the version, bump the local version gates, and
		// then return. The upgrade associated with the specific version is
		// executed before any node in the cluster has the corresponding
		// version activated. Migrations that depend on a certain version
		// already being activated will need to registered using a cluster
		// version greater than it.
		//
		// For each intermediate version, we'll need to first bump the fence
		// version before bumping the "real" one. Doing so allows us to provide
		// the invariant that whenever a cluster version is active, all Nodes in
		// the cluster (including ones added concurrently during version
		// upgrades) are running binaries that know about the version.

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

		{
			// The upgrades infrastructure makes use of internal fence
			// versions when stepping through consecutive versions. It's
			// instructive to walk through how we expect a version upgrade
			// from v21.1 to v21.2 to take place, and how we behave in the
			// presence of new v21.1 or v21.2 Nodes being added to the cluster.
			//
			//   - All Nodes are running v21.1
			//   - All Nodes are rolled into v21.2 binaries, but with active
			//     cluster version still as v21.1
			//   - The first version bump will be into v21.2-1(fence), see the
			//     upgrade manager above for where that happens
			//
			// Then concurrently:
			//
			//   - A new node is added to the cluster, but running binary v21.1
			//   - We try bumping the cluster gates to v21.2-1(fence)
			//
			// If the v21.1 Nodes manages to sneak in before the version bump,
			// it's fine as the version bump is a no-op one (all fence versions
			// are). Any subsequent bumps (including the "actual" one bumping to
			// v21.2) will fail during the validation step where we'll first
			// check to see that all Nodes are running v21.2 binaries.
			//
			// If the v21.1 node is only added after v21.2-1(fence) is active,
			// it won't be able to actually join the cluster (it'll be prevented
			// by the join RPC).
			//
			// All of which is to say that once we've seen the node list
			// stabilize (as UntilClusterStable enforces), any new nodes that
			// can join the cluster will run a release that support the fence
			// version, and by design also supports the actual version (which is
			// the direct successor of the fence).
			fenceVersion := upgrade.FenceVersionFor(ctx, cv)
			if err := bumpClusterVersion(ctx, m.deps.Cluster, fenceVersion); err != nil {
				return err
			}
		}

		// Now sanity check that we'll actually be able to perform the real
		// cluster version bump, cluster-wide.
		if err := validateTargetClusterVersion(ctx, m.deps.Cluster, cv); err != nil {
			return err
		}

		// Finally, bump the real version cluster-wide.
		err := bumpClusterVersion(ctx, m.deps.Cluster, cv)
		if err != nil {
			return err
		}
		// Bump up the cluster version for tenants, which
		// will bump over individual version bumps.
		err = updateSystemVersionSetting(ctx, cv)
		if err != nil {
			return err
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
		return err
	})
}

func forEveryNodeUntilClusterStable(
	ctx context.Context,
	op string,
	c upgrade.Cluster,
	f func(ctx context.Context, client serverpb.MigrationClient) error,
) error {
	return c.UntilClusterStable(ctx, func() error {
		return c.ForEveryNode(ctx, op, f)
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

		alreadyCompleted, err := migrationstable.CheckIfMigrationCompleted(
			ctx, version, nil /* txn */, m.ie, false /* enterpriseEnabled */, migrationstable.ConsistentRead,
		)
		if alreadyCompleted || err != nil {
			return err
		}

		switch upg := mig.(type) {
		case *upgrade.SystemUpgrade:
			if err := upg.Run(ctx, mig.Version(), m.SystemDeps()); err != nil {
				return err
			}
		case *upgrade.TenantUpgrade:
			// The TenantDeps used here are incomplete, but enough for the "permanent
			// upgrades" that run under this testing knob.
			if err := upg.Run(ctx, mig.Version(), upgrade.TenantDeps{
				DB:               m.deps.DB,
				Codec:            m.codec,
				Settings:         m.settings,
				LeaseManager:     m.lm,
				InternalExecutor: m.ie,
				JobRegistry:      m.jr,
			}); err != nil {
				return err
			}
		}

		if err := migrationstable.MarkMigrationCompleted(ctx, m.ie, mig.Version()); err != nil {
			return err
		}
		return nil
	} else {
		// Run a job that, in turn, will run the upgrade. By running upgrades inside
		// jobs, we get some observability for them and we avoid multiple nodes
		// attempting to run the same upgrade all at once. Particularly for
		// long-running upgrades, this is useful.
		//
		// If the job already exists, we wait for it to finish.
		alreadyCompleted, alreadyExisting, id, err := m.getOrCreateMigrationJob(ctx, user, version, mig.Name())
		if alreadyCompleted || err != nil {
			return err
		}
		if alreadyExisting {
			log.Infof(ctx, "waiting for %s", mig.Name())
			return m.jr.WaitForJobs(ctx, []jobspb.JobID{id})
		} else {
			log.Infof(ctx, "running %s", mig.Name())
			return m.jr.Run(ctx, []jobspb.JobID{id})
		}
	}
}

func (m *Manager) getOrCreateMigrationJob(
	ctx context.Context, user username.SQLUsername, version roachpb.Version, name string,
) (alreadyCompleted, alreadyExisting bool, jobID jobspb.JobID, _ error) {
	newJobID := m.jr.MakeJobID()
	if err := m.deps.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) (err error) {
		enterpriseEnabled := base.CCLDistributionAndEnterpriseEnabled(m.settings, m.clusterID)
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

func (m *Manager) getRunningMigrationJob(
	ctx context.Context, txn isql.Txn, version roachpb.Version,
) (found bool, jobID jobspb.JobID, _ error) {
	// Wrap the version into a ClusterVersion so that the JSON looks like what the
	// Payload proto has inside.
	cv := clusterversion.ClusterVersion{Version: version}
	const query = `
SELECT id, status
	FROM (
		SELECT id,
		status,
		crdb_internal.pb_to_json(
			'cockroach.sql.jobs.jobspb.Payload',
			payload,
      false -- emit_defaults
		) AS pl
	FROM system.jobs
  WHERE status IN ` + jobs.NonTerminalStatusTupleString + `
	)
	WHERE pl->'migration'->'clusterVersion' = $1::JSON;`
	jsonMsg, err := protoreflect.MessageToJSON(&cv, protoreflect.FmtFlags{EmitDefaults: false})
	if err != nil {
		return false, 0, errors.Wrap(err, "failed to marshal version to JSON")
	}
	rows, err := txn.QueryBuffered(ctx, "migration-manager-find-jobs", txn.KV(), query, jsonMsg.String())
	if err != nil {
		return false, 0, err
	}
	parseRow := func(row tree.Datums) (id jobspb.JobID, status jobs.Status) {
		return jobspb.JobID(*row[0].(*tree.DInt)), jobs.Status(*row[1].(*tree.DString))
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
		return m.knobs.ListBetweenOverride(from, to)
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
			DB:               m.deps.DB,
			Codec:            m.codec,
			Settings:         m.settings,
			LeaseManager:     m.lm,
			InternalExecutor: m.ie,
			JobRegistry:      m.jr,
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
