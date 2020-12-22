// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migrations contains the implementation of migrations. It is imported
// by the server library.
//
// This package registers the migrations with the migration package.
package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// registry defines the global mapping between a cluster version and the
// associated migration. The migration is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[clusterversion.ClusterVersion]migration.Migration)

// register is a short hand to register a given migration within the global
// registry.
func register(key clusterversion.Key, fn migration.KVMigrationFn, desc string) {
	cv := clusterversion.ClusterVersion{Version: clusterversion.ByKey(key)}
	if _, ok := registry[cv]; ok {
		log.Fatalf(context.Background(), "doubly registering migration for %s", cv)
	}
	registry[cv] = migration.NewKVMigration(desc, cv, fn)
}

// GetMigration returns the migration corresponding to this version if
// one exists.
func GetMigration(key clusterversion.ClusterVersion) (migration.Migration, bool) {
	m, ok := registry[key]
	return m, ok
}

// TestingRegisterMigrationInterceptor is used in tests to register an
// interceptor for a version migration.
//
// TODO(irfansharif): This is a gross anti-pattern, we're letting tests mutate
// global state. This should instead be a testing knob that the migration
// manager checks when search for attached migrations.
func TestingRegisterMigrationInterceptor(
	cv clusterversion.ClusterVersion, fn migration.KVMigrationFn,
) (unregister func()) {
	registry[cv] = migration.NewKVMigration("", cv, fn)
	return func() { delete(registry, cv) }
}

var kvMigrations = []struct {
	cv          clusterversion.Key
	fn          migration.KVMigrationFn
	description string
}{
	{
		clusterversion.TruncatedAndRangeAppliedStateMigration,
		truncatedStateMigration,
		"use unreplicated TruncatedState and RangeAppliedState for all ranges",
	},
	{
		clusterversion.PostTruncatedAndRangeAppliedStateMigration,
		postTruncatedStateMigration,
		"purge all replicas using the replicated TruncatedState",
	},
}

func init() {
	for _, m := range kvMigrations {
		register(m.cv, m.fn, m.description)
	}
}
