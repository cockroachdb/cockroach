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

// TenantCluster represents the set of sql nodes running in a secondary tenant.
// It implements the migration.Cluster interface. It is used to drive forward
// migrations in the secondary tenants. In the current iteration, it assumes
// that there is a single pod per tenant.
//
// Tenants and cluster upgrades
//
// Tenants have their own system tables and settings, which get bootstrapped in
// CreateTenant along with an initial cluster version. The interplay between
// tenant cluster version and KV cluster version is complicated. First, recall
// that there are multiple "versions":
//
//  - the cluster version is the version at which the cluster operates. The
//    cluster version of the system tenant is *the* cluster version of the KV
//    layer. This is a single value (stored in the KV store) but while it
//    changes, some nodes will be using the old value, and some the new one. KV
//    does a lot of work to be able to tell when *all* nodes have adopted the
//    new value. Non-system tenants also have a similar cluster value, which is
//    stored in a KV pair under the tenant's jurisdiction. Explaining how this
//    relates to that of the system tenant is the main aim of this
//    documentation.
//  - the binary version is the largest cluster version a binary (i.e. cockroach
//    executable) can in principle support. For most of the time, the binary
//    version equals the cluster version, but during upgrades, nodes will
//    temporarily run with a binary version larger than the cluster version.
//  - the binary minimum supported version is the smallest cluster version a
//    binary can in principle support. It is typically set to the previous major
//    release, for binary with a 20.2 binary version has a binary minimum
//    supported version of 20.1, meaning that it can participate in a cluster
//    running at cluster version 20.1 (which is necessary when a 20.1 cluster is
//    upgraded to 20.2).
//
//  BinaryMinSupportedVersion                        BinaryVersion
//             |                                           |
//             v...........................................v
//                  (possible range of active
//                   cluster versions)
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
