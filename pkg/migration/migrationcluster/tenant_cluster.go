// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrationcluster

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/errors"
)

// TenantCluster is an implementation of migration.Cluster that doesn't do
// anything to track the set of nodes in the cluster. In the fullness of time
// a secondary tenant cluster may care about providing a true barrier between
// code versions. As of writing, there is only a single pod running and it
// is assumed to be of the appropriate version. The rest of this comment
// outlines the properties of tenant upgrades.
//
// The upgrade protocol for KV nodes in the system tenant involves tight
// coordination between nodes to gain invariants about what features will be
// used and what migrations can and have been run. SQL pods are inherently much
// more ephemeral and less able to perform this coordination. Nevertheless, we
// need some way to migrate data in tenants and we need some claims about the
// features the tenants may end up using.
//
//
// Problem Overview
//
//  * Tenants have their own system tables, including settings.
//  * System tables and initial data get bootstrapped in CreateTenant.
//     * Ideally write out an initial version into the settings table
//       at that point.
//     * For new tenants what should we initialize this to?
//         * The best that the system tenant can do is its
//           active version.
//     * For pre-existing tenants in the 20.2->21.1 upgrade, cope
//       with the lack of a value gracefully by populating an in-memory value
//       at startup.
//  * The desired protocols allows upgrading the KV servers *before* upgrading
//    the tenant pods. It does not require upgrading the tenants first.
//  * One consideration is the handshake between SQL pods and KV nodes.
//    * Once the KV cluster has its version finalized, new RPC connections
//      from SQL pods with an old cluster version will not be permitted.
//    * This implies that we need to leave the KV cluster in the mixed
//      version state while we upgrade the sql pods to the newest code
//      version.
//      * For 21.1 this is what we'll do.
//    * It's not clear that this is acceptable; clients will want to control
//      when their SQL semantic change, but some clients will want the full
//      functionality of the new version before others. If we can't activate
//      the new version on the KV cluster until all the SQL pods are running
//      the new code, we can't really do this; not all behaviors are preserved
//      in the mixed version state.
//    * What if we allowed old SQL pods to connect to KV nodes? This, on its
//      face, seems like it would totally violate the principles of the
//      version gating as the SQL pods are just using the vanilla KV API.
//      Nothing would prevent an old SQL pod from sending some deprecated
//      and assumed to be fully eliminated command.
//        * Maybe this is too pessimistic. Maybe instead we need to say that
//          KV migrations need occur only with a minimum of a 1 version delay.
//          Today that is the case.
//
// Solution
//
//  * Current phase (20.2->21.1):
//    * Allow tenants to run the migration hook and set the cluster setting
//      up to the active cluster version of the attached KV node.
//    * Populate tenant version setting using the current version of the binary
//      writing the state.
//    * Not supported:
//        * Any reliable claims about the versions of other node.
//    * Do not do anything to permit SQL pods to continue to operate at the old
//      version.
//        * This is the status quo.
//  * Next Phase (21.1->21.2):
//    * Allow the RPC handshake to permit connections from nodes
//      all the way back to the previous version state (the minimum
//      binary supported version) if that connection indicates it is
//      a secondary tenant connection.
//         * In the long term we may want to think harder about this.
//           It relates to ideas about KV protocol versioning.
//         * One oddity here is that a SQL pod won't know its version until
//           after it is able to read it from its keyspace.
//            * It can broadcast its binary version, I guess.
//    * Add some tooling for the system tenant to check to make sure
//      tenants have some version activated.
//
// Current Upgrade Protocol (20.2->21.1)
//
//  * Deploy v21.1 on KV nodes but do not allow finalization.
//  * Deploy v21.1 on SQL pods.
//  * Upgrade all SQL pods to v21.1 (`SET CLUSTER SETTING version = '21.1';`)
//  * Finalize the v21.1 on KV nodes.
//
type TenantCluster struct {
	db *kv.DB
}

// NewTenantCluster returns a new TenantCluster.
func NewTenantCluster(db *kv.DB) *TenantCluster {
	return &TenantCluster{db: db}
}

// DB is part of the migration.Cluster interface.
func (t *TenantCluster) DB() *kv.DB {
	return t.db
}

// ForEveryNode is part of the migration.Cluster interface.
func (t *TenantCluster) ForEveryNode(
	ctx context.Context, op string, fn func(context.Context, serverpb.MigrationClient) error,
) error {
	return errors.AssertionFailedf("non-system tenants cannot iterate nodes")
}

// UntilClusterStable is part of the migration.Cluster interface.
//
// Tenant clusters in the current version assume their cluster is stable
// because they presently assume there is at most one running SQL pod. When
// that changes, this logic will need to change.
func (t TenantCluster) UntilClusterStable(ctx context.Context, fn func() error) error {
	return nil
}

// IterateRangeDescriptors is part of the migration.Cluster interface.
func (t TenantCluster) IterateRangeDescriptors(
	ctx context.Context, size int, init func(), f func(descriptors ...roachpb.RangeDescriptor) error,
) error {
	return errors.AssertionFailedf("non-system tenants cannot iterate ranges")
}
