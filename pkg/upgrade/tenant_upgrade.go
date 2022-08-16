// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrade

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/logtags"
)

// TenantDeps are the dependencies of upgrades which perform actions at the
// SQL layer.
type TenantDeps struct {
	DB                *kv.DB
	Codec             keys.SQLCodec
	Settings          *cluster.Settings
	CollectionFactory *descs.CollectionFactory
	LeaseManager      *lease.Manager
	JobRegistry       *jobs.Registry
	InternalExecutor  sqlutil.InternalExecutor
	SessionData       *sessiondata.SessionData

	SpanConfig struct { // deps for span config upgrades; can be removed accordingly
		spanconfig.KVAccessor
		spanconfig.Splitter
		Default roachpb.SpanConfig
	}

	TestingKnobs              *TestingKnobs
	SchemaResolverConstructor func( // A constructor that returns a schema resolver for `descriptors` in `currDb`.
		txn *kv.Txn, descriptors *descs.Collection, currDb string,
	) (resolver.SchemaResolver, func(), error)
}

// TenantUpgradeFunc is used to perform sql-level upgrades. It may be run from
// any tenant.
type TenantUpgradeFunc func(context.Context, clusterversion.ClusterVersion, TenantDeps, *jobs.Job) error

// PreconditionFunc is a function run without isolation before attempting an
// upgrade that includes this upgrade. It is used to verify that the
// required conditions for the upgrade to succeed are met. This can allow
// users to fix any problems before "crossing the rubicon" and no longer
// being able to upgrade.
type PreconditionFunc func(context.Context, clusterversion.ClusterVersion, TenantDeps) error

// TenantUpgrade is an implementation of Upgrade for tenant-level
// upgrades. This is used for all upgrade which might affect the state of
// sql. It includes the system tenant.
type TenantUpgrade struct {
	upgrade
	fn           TenantUpgradeFunc
	precondition PreconditionFunc
}

var _ Upgrade = (*TenantUpgrade)(nil)

// NewTenantUpgrade constructs a TenantUpgrade.
func NewTenantUpgrade(
	description string,
	cv clusterversion.ClusterVersion,
	precondition PreconditionFunc,
	fn TenantUpgradeFunc,
) *TenantUpgrade {
	m := &TenantUpgrade{
		upgrade: upgrade{
			description: description,
			cv:          cv,
		},
		fn:           fn,
		precondition: precondition,
	}
	return m
}

// Run kick-starts the actual upgrade process for tenant-level upgrades.
func (m *TenantUpgrade) Run(
	ctx context.Context, cv clusterversion.ClusterVersion, d TenantDeps, job *jobs.Job,
) error {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("upgrade=%s", cv), nil)
	return m.fn(ctx, cv, d, job)
}

// Precondition runs the precondition check if there is one and reports
// any errors.
func (m *TenantUpgrade) Precondition(
	ctx context.Context, cv clusterversion.ClusterVersion, d TenantDeps,
) error {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("upgrade=%s,precondition", cv), nil)
	return m.precondition(ctx, cv, d)
}
