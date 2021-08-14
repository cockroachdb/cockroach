// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migration

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/logtags"
)

// TenantDeps are the dependencies of migrations which perform actions at the
// SQL layer.
type TenantDeps struct {
	DB                *kv.DB
	Codec             keys.SQLCodec
	Settings          *cluster.Settings
	CollectionFactory *descs.CollectionFactory
	LeaseManager      *lease.Manager
	InternalExecutor  sqlutil.InternalExecutor
	TestingKnobs      *TestingKnobs
}

// TenantMigrationFunc is used to perform sql-level migrations. It may be run from
// any tenant.
type TenantMigrationFunc func(context.Context, clusterversion.ClusterVersion, TenantDeps) error

// PreconditionFunc is a function run without isolation before attempting an
// upgrade that includes this migration. It is used to verify that the
// required conditions for the migration to succeed are met. This can allow
// users to fix any problems before "crossing the rubicon" and no longer
// being able to upgrade.
type PreconditionFunc TenantMigrationFunc

// TenantMigration is an implementation of Migration for tenant-level
// migrations. This is used for all migration which might affect the state of
// sql. It includes the system tenant.
type TenantMigration struct {
	migration
	fn           TenantMigrationFunc
	precondition PreconditionFunc
}

var _ Migration = (*TenantMigration)(nil)

// NewTenantMigration constructs a TenantMigration.
func NewTenantMigration(
	description string,
	cv clusterversion.ClusterVersion,
	precondition PreconditionFunc,
	fn TenantMigrationFunc,
) *TenantMigration {
	m := &TenantMigration{
		migration: migration{
			description: description,
			cv:          cv,
		},
		fn:           fn,
		precondition: precondition,
	}
	return m
}

// Run kickstarts the actual migration process for tenant-level migrations.
func (m *TenantMigration) Run(
	ctx context.Context, cv clusterversion.ClusterVersion, d TenantDeps,
) error {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("migration=%s", cv), nil)
	return m.fn(ctx, cv, d)
}

// Precondition runs the precondition check if there is one and reports
// any errors.
func (m *TenantMigration) Precondition(
	ctx context.Context, cv clusterversion.ClusterVersion, d TenantDeps,
) error {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("migration=%s,precondition", cv), nil)
	return m.precondition(ctx, cv, d)
}
