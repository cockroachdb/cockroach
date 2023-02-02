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
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/logtags"
)

// TenantDeps are the dependencies of upgrades which perform actions at the
// SQL layer.
type TenantDeps struct {
	KVDB         *kv.DB
	Codec        keys.SQLCodec
	Settings     *cluster.Settings
	DB           descs.DB
	LeaseManager *lease.Manager
	JobRegistry  *jobs.Registry
	SessionData  *sessiondata.SessionData

	// TODO(ajwerner): Remove this in favor of the descs.DB above.
	InternalExecutor isql.Executor

	SpanConfig struct { // deps for span config upgrades; can be removed accordingly
		spanconfig.KVAccessor
		spanconfig.Splitter
		Default roachpb.SpanConfig
	}

	TestingKnobs              *upgradebase.TestingKnobs
	SchemaResolverConstructor func( // A constructor that returns a schema resolver for `descriptors` in `currDb`.
		txn *kv.Txn, descriptors *descs.Collection, currDb string,
	) (resolver.SchemaResolver, func(), error)
}

// TenantUpgradeFunc is used to perform sql-level upgrades. It may be run from
// any tenant.
//
// NOTE: The upgrade func runs inside a job, and the job can in principle be
// used to checkpoint the upgrade's progress ergonomically. The tenant func used
// to take a reference to the job running it for this purpose, but it was
// removed because it was no longer used and because of testing complications.
// It can be added back, though, if some upgrade needs it again.
type TenantUpgradeFunc func(context.Context, clusterversion.ClusterVersion, TenantDeps) error

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
	fn TenantUpgradeFunc
	// precondition is executed before fn. Note that permanent upgrades (see
	// upgrade.permanent) cannot have preconditions.
	precondition PreconditionFunc
}

var _ upgradebase.Upgrade = (*TenantUpgrade)(nil)

// NoPrecondition can be used with NewTenantUpgrade to signify that the
// respective upgrade does not need any preconditions checked.
var NoPrecondition PreconditionFunc = nil

// NewTenantUpgrade constructs a TenantUpgrade.
func NewTenantUpgrade(
	description string, v roachpb.Version, precondition PreconditionFunc, fn TenantUpgradeFunc,
) *TenantUpgrade {
	m := &TenantUpgrade{
		upgrade: upgrade{
			description: description,
			v:           v,
			permanent:   false,
		},
		fn:           fn,
		precondition: precondition,
	}
	return m
}

// NewPermanentTenantUpgrade constructs a TenantUpgrade marked as "permanent":
// an upgrade that will run regardless of the cluster's bootstrap version.
func NewPermanentTenantUpgrade(
	description string, v roachpb.Version, fn TenantUpgradeFunc, v22_2StartupMigrationName string,
) *TenantUpgrade {
	m := &TenantUpgrade{
		upgrade: upgrade{
			description:               description,
			v:                         v,
			permanent:                 true,
			v22_2StartupMigrationName: v22_2StartupMigrationName,
		},
		fn:           fn,
		precondition: nil,
	}
	return m
}

// Run kick-starts the actual upgrade process for tenant-level upgrades.
func (m *TenantUpgrade) Run(ctx context.Context, v roachpb.Version, d TenantDeps) error {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("upgrade=%s", v), nil)
	return m.fn(ctx, clusterversion.ClusterVersion{Version: v}, d)
}

// Precondition runs the precondition check if there is one and reports
// any errors.
func (m *TenantUpgrade) Precondition(
	ctx context.Context, cv clusterversion.ClusterVersion, d TenantDeps,
) error {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("upgrade=%s,precondition", cv), nil)
	if m.precondition != nil {
		return m.precondition(ctx, cv, d)
	}
	return nil
}
