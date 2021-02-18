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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/logtags"
)

// SQLDeps are the dependencies of migrations which perform actions at the
// SQL layer.
type SQLDeps struct {
	DB               *kv.DB
	Codec            keys.SQLCodec
	Settings         *cluster.Settings
	LeaseManager     *lease.Manager
	InternalExecutor sqlutil.InternalExecutor
}

// SQLMigrationFn is used to perform sql-level migrations. It may be run from
// any tenant.
type SQLMigrationFn func(context.Context, clusterversion.ClusterVersion, SQLDeps) error

// SQLMigration is an implementation of Migration for SQL-level migrations.
type SQLMigration struct {
	migration
	fn SQLMigrationFn
}

// NewSQLMigration constructs a SQLMigration.
func NewSQLMigration(
	description string, cv clusterversion.ClusterVersion, fn SQLMigrationFn,
) *SQLMigration {
	return &SQLMigration{
		migration: migration{
			description: description,
			cv:          cv,
		},
		fn: fn,
	}
}

// Run kickstarts the actual migration process for SQL-level migrations.
func (m *SQLMigration) Run(
	ctx context.Context, cv clusterversion.ClusterVersion, d SQLDeps,
) (err error) {
	ctx = logtags.AddTag(ctx, fmt.Sprintf("migration=%s", cv), nil)
	return m.fn(ctx, cv, d)
}
