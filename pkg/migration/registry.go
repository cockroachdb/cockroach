// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// registry defines the global mapping between a cluster version and the
// associated migration. The migration is only executed after a cluster-wide
// bump of the corresponding version gate.
var registry = make(map[clusterversion.ClusterVersion]Migration)

// Register is a short hand to Register a given migration within the global
// registry.
func Register(key clusterversion.Key, fn MigrationFn, desc string) {
	cv := clusterversion.ClusterVersion{Version: clusterversion.ByKey(key)}
	if _, ok := registry[cv]; ok {
		log.Fatalf(context.Background(), "doubly registering migration for %s", cv)
	}
	registry[cv] = Migration{cv: cv, fn: fn, desc: desc}
}

// GetMigrations returns the migration corresponding to this version if
// one exists.
func GetMigration(key clusterversion.ClusterVersion) (Migration, bool) {
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
	cv clusterversion.ClusterVersion, fn MigrationFn,
) (unregister func()) {
	registry[cv] = Migration{cv: cv, fn: fn}
	return func() { delete(registry, cv) }
}
