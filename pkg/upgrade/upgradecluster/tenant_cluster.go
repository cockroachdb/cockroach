// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgradecluster

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlinstance/instancestorage"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/quotapool"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"google.golang.org/grpc"
)

// TenantCluster represents the set of sql nodes running in a secondary tenant.
// It implements the upgrade.Cluster interface. It is used to drive forward
// upgrades in the secondary tenants. In the current iteration, it assumes
// that there is a single pod per tenant.
//
// # Tenants and cluster upgrades
//
// Tenants have their own system tables and settings, which get bootstrapped in
// CreateTenant along with an initial cluster version. The interplay between
// tenant cluster version and KV cluster version is complicated. First, recall
// that there are multiple "versions":
//
//   - the cluster version is the version at which the cluster operates. The
//     cluster version of the system tenant is *the* cluster version of the KV
//     layer. This is a single value (stored in the KV store) but while it
//     changes, some nodes will be using the old value, and some the new one. KV
//     does a lot of work to be able to tell when *all* nodes have adopted the
//     new value. Non-system tenants also have a similar cluster value, which is
//     stored in a KV pair under the tenant's jurisdiction. Explaining how this
//     relates to that of the system tenant is the main aim of this
//     documentation.
//
//   - the binary version is the largest cluster version a binary (i.e. cockroach
//     executable) can in principle support. For most of the time, the binary
//     version equals the cluster version, but during upgrades, nodes will
//     temporarily run with a binary version larger than the cluster version.
//
//   - the binary minimum supported version is the smallest cluster version a
//     binary can in principle support. It is typically set to the previous major
//     release, for binary with a 20.2 binary version has a binary minimum
//     supported version of 20.1, meaning that it can participate in a cluster
//     running at cluster version 20.1 (which is necessary when a 20.1 cluster is
//     upgraded to 20.2).
//
//     MinSupportedVersion                         LatestVersion
//     |                                           |
//     v...........................................v
//     possible range of active
//     cluster versions
//
// Versions are used in many checks to prevent issues due to operator error. The
// main one of interest here is that RPC connections between nodes (including
// tenants) validate the binary server version against the active cluster
// version. That is, when node A connects to node B, node B will verify that A's
// binary version matches or exceeds the active cluster version at B. For
// example, a 20.2 binary will be prevented from connecting to a node running a
// cluster version of 21.1 (and thus at least a 21.1 binary), as this could
// cause undefined behavior.
//
// Upgrading tenants complicates matters. The KV layer internally orchestrates
// the upgrade process with in-depth knowledge of the KV nodes participating in
// the cluster and will ensure that a cluster version is only rolled out when
// all KV nodes support it, and will confirm the upgrade to the operator only
// once it has proven that all current and future KV nodes are picking it up. No
// such tight internal orchestration is possible with tenants. In fact, KV has
// no access to the tenants at all, and very little information about them.
// What's more, the upgrade pattern desired for tenants is fundamentally
// different in that we want to be able to upgrade tenants "piecemeal", at their
// own pace (within some timeframe). We do *not* desire a tight coupling between
// the KV upgrade and that of the tenants.
//
// We relax the invariants around phased-out functionality so that they apply
// only for KV-internal state that is not accessible from tenants. Thus, we
// can legally keep tenants at cluster version N and binary version N or N+1 while
// the KV cluster is already at cluster version N+1. We let tenants chose when
// they upgrade their binary though, so we loosen the connection check rules such
// that tenants are allowed to connect to KV, as long as their active cluster
// version is above KV's *minimum supported version*. This should be benign, as a
// 20.2 binary at cluster version 20.2 should be as good as a 21.1 binary at
// cluster version 20.2 assuming we haven't prematurely removed support for 20.2
// behaviors. Note, however, that this all relies on the fact that the KV nodes
// never dial the SQL pods. So we would get:
//
//  1. KV and tenants run 20.2 (binary and cluster version)
//  2. KV runs 21.1 binary and cluster version
//  3. Tenants can upgrade to 21.1 binary (and then to 21.1 cluster version)
//     at their own pace.
//  4. All tenants have to be at 21.1 cluster version before KV gets upgraded
//     again in the next release.
//
// We could entertain letting tenants trail multiple releases in the future at
// the expense of a smaller `MinSupportedVersion` and more time spent on legacy
// behavior during development.
//
// Or, as an alternative, the binaries can all be upgraded before any versions
// are finalized. This may help catch some incompatibility bugs before crossing
// the rubicon and not being able to downgrade binaries. This has the downside
// that tenant clusters cannot choose when to upgrade their binary; some new
// or changed behaviors are likely to exist in the 21.1 binary. It also means
// that there is a window when tenants will be forced to accept the 21.1 binary
// but will not be allowed to upgrade to 21.1. Another thing to note is that
// there is nothing gating the upgrade to 21.1 for the tenant, which may be
// invalid. This should not be the preferred strategy.
//
//  1. KV and tenants run 20.2 (binary and cluster version)
//  2. KV runs 21.1 binary and 20.2 cluster version.
//  3. Tenants can upgrade to 21.1 binary (but not cluster version).
//  4. KV finalizes 21.1.
//  5. All tenants have to be at 21.1 cluster version before KV gets upgraded
//     again in the next release.
type TenantCluster struct {
	// Dialer allows for the construction of connections to other SQL pods.
	Dialer          NodeDialer
	InstanceReader  *instancestorage.Reader
	instancesAtBump []sqlinstance.InstanceInfo
	DB              *kv.DB
}

// TenantClusterConfig configures a TenantCluster.
type TenantClusterConfig struct {
	// Dialer allows for the construction of connections to other SQL pods.
	Dialer NodeDialer

	// InstanceReader is used to retrieve all SQL pods for a given tenant.
	InstanceReader *instancestorage.Reader

	// DB is used to generate transactions for consistent reads of the set of
	// instances.
	DB *kv.DB
}

// NewTenantCluster returns a new TenantCluster.
func NewTenantCluster(cfg TenantClusterConfig) *TenantCluster {
	return &TenantCluster{
		Dialer:          cfg.Dialer,
		InstanceReader:  cfg.InstanceReader,
		instancesAtBump: make([]sqlinstance.InstanceInfo, 0),
		DB:              cfg.DB,
	}
}

// NumNodesOrTenantPods is part of the upgrade.Cluster interface.
func (t *TenantCluster) NumNodesOrServers(ctx context.Context) (int, error) {
	// Get the list of all SQL instances running.
	instances, err := t.InstanceReader.GetAllInstances(ctx)
	if err != nil {
		return 0, err
	}
	return len(instances), nil
}

// BumpClusterVersionOpName serves as a tag for tenant cluster version
// in-memory bump operations. Every time we bump a tenant's cluster version,
// we send out a gRPC to each of the tenant's SQL servers
// (via ForEveryNodeOrServer), to have them remotely bump their in-memory
// version as well. In the first of these such bumps for a migration (the first
// "fence"), we also cache the list of SQL servers that we contacted, and
// validate after persisting the fence version to the settings table, that
// no new SQL servers have joined. If new SQL servers have joined in the
// interim, we must revalidate that their binary versions are sufficiently
// up-to-date to continue with the upgrade process. Once the bump value is
// persisted to disk, no new SQL servers will be permitted to start with
// binary versions that are less than the tenant's min binary version.
//
// This tag is used in the interlock to identify when we're bumping a cluster
// version and therefore, when we must cache the set of SQL servers contacted.
const BumpClusterVersionOpName = "bump-cluster-version"

// ForEveryNodeOrServer is part of the upgrade.Cluster interface.
// TODO(ajstorm): Make the op here more structured.
func (t *TenantCluster) ForEveryNodeOrServer(
	ctx context.Context, op string, fn func(context.Context, serverpb.MigrationClient) error,
) error {
	// Get the list of all SQL instances running. We must do this using the
	// "NoCache" method, as the upgrade interlock requires a consistent view of
	// the currently running SQL instances to avoid RPC failures when attempting
	// to contact SQL instances which are no longer alive, and to ensure that
	// it's communicating with all currently alive instances.
	instances, err := t.InstanceReader.GetAllInstancesNoCache(ctx)
	if err != nil {
		return err
	}

	// If we're bumping the cluster version, cache the list of instances we
	// contacted at bump time. This list of instances is then consulted at
	// fence write time to ensure that we contacted the same set of instances.
	if strings.Contains(op, BumpClusterVersionOpName) {
		t.instancesAtBump = instances
	}

	// Limiting of outgoing RPCs at the tenant level mirrors what we do for
	// nodes at the storage cluster level.
	const quotaCapacity = 25
	qp := quotapool.NewIntPool("every-sql-server", quotaCapacity)
	log.Infof(ctx, "executing %s on nodes %v", redact.Safe(op), instances)
	grp := ctxgroup.WithContext(ctx)

	for i := range instances {
		instance := instances[i]
		alloc, err := qp.Acquire(ctx, 1)
		if err != nil {
			return err
		}

		grp.GoCtx(func(ctx context.Context) error {
			defer alloc.Release()

			var conn *grpc.ClientConn
			retryOpts := retry.Options{
				InitialBackoff: 1 * time.Millisecond,
				MaxRetries:     20,
				MaxBackoff:     100 * time.Millisecond,
			}
			// This retry was added to benefit our tests (not users) by reducing the chance of
			// test flakes due to network issues.
			if err := retry.WithMaxAttempts(ctx, retryOpts, retryOpts.MaxRetries+1, func() error {
				var err error
				conn, err = t.Dialer.Dial(ctx, roachpb.NodeID(instance.InstanceID), rpc.DefaultClass)
				return err
			}); err != nil {
				return annotateDialError(err)
			}
			client := serverpb.NewMigrationClient(conn)
			return fn(ctx, client)
		})
	}
	return grp.Wait()
}

func annotateDialError(err error) error {
	if !errors.HasType(err, (*netutil.InitialHeartbeatFailedError)(nil)) {
		return err
	}
	if errors.Is(err, rpc.VersionCompatError) {
		err = errors.Wrap(err,
			"upgrade failed due to active SQL servers with incompatible binary version(s)")
		return errors.WithHint(err, "upgrade the binary versions of all SQL servers before re-attempting the tenant upgrade")
	}
	err = errors.Wrapf(err, "upgrade failed due to transient SQL servers")
	return errors.WithHint(err, "retry upgrade when the SQL servers for the given tenant are in a stable state (i.e. not starting/stopping)")
}

// UntilClusterStable is part of the upgrade.Cluster interface.
//
// We don't have the same notion of cluster stability with tenant servers as we
// do with cluster nodes. As a result, this function behaves slightly
// differently with secondary tenants than it does with the system tenant.
// Instead of relying on liveness and waiting until all nodes are active before
// we claim that the cluster has become "stable", we collect the set of active
// SQL servers, and loop until we find two successive iterations where the SQL
// server list is consistent. This does not preclude new SQL servers from
// starting after we've declared "stability", but there are other checks in the
// tenant upgrade interlock which prevent those new SQL servers from starting if
// they're at an incompatible binary version (at the time of writing, in
// SQLServer.preStart).
func (t *TenantCluster) UntilClusterStable(
	ctx context.Context, retryOpts retry.Options, fn func() error,
) error {
	instances, err := t.InstanceReader.GetAllInstancesNoCache(ctx)
	if err != nil {
		return err
	}

	// TODO(ajstorm): this could use a test to validate that the retry behavior
	//  does what we expect. I've tested it manually in the debugger for now.
	for retrier := retry.StartWithCtx(ctx, retryOpts); retrier.Next(); {
		if err := fn(); err != nil {
			return err
		}

		// Check if the set of servers was stable during the function call.
		curInstances, err := t.InstanceReader.GetAllInstancesNoCache(ctx)
		if err != nil {
			return err
		}
		if ok := reflect.DeepEqual(instances, curInstances); ok {
			return nil
		}
		if len(instances) != len(curInstances) {
			log.Infof(ctx,
				"number of SQL servers has changed (pre: %d, post: %d), retrying",
				len(instances), len(curInstances))
		} else {
			log.Infof(ctx, "different set of SQL servers running (pre: %v, post: %v), retrying", instances, curInstances)
		}
		instances = curInstances
	}
	return errors.Newf("unable to observe a stable set of SQL servers after maximum iterations")
}

// IterateRangeDescriptors is part of the upgrade.Cluster interface.
func (t *TenantCluster) IterateRangeDescriptors(
	ctx context.Context, size int, init func(), f func(descriptors ...roachpb.RangeDescriptor) error,
) error {
	return errors.AssertionFailedf("non-system tenants cannot iterate ranges")
}

type inconsistentSQLServersError struct{}

func (inconsistentSQLServersError) Error() string {
	return "new SQL servers added during migration: migration must be retried"
}

var InconsistentSQLServersError = inconsistentSQLServersError{}

func (t *TenantCluster) ValidateAfterUpdateSystemVersion(ctx context.Context, txn *kv.Txn) error {
	if len(t.instancesAtBump) == 0 {
		// We should never get here with an empty slice, since bump must be
		// called before validation.
		return errors.AssertionFailedf("call to validate with empty instances slice")
	}

	instances, err := t.InstanceReader.GetAllInstancesUsingTxn(ctx, txn)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(instances, t.instancesAtBump) {
		return InconsistentSQLServersError
	}
	return nil
}
