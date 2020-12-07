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
)

// Registry defines the global mapping between a cluster version, and the
// associated migration. The migration is only executed after a cluster-wide
// bump of the version gate.
var Registry = make(map[clusterversion.ClusterVersion]Migration)

func init() {
	// TODO(irfansharif): We'll want to register individual migrations with
	// specific internal cluster versions here.
	//
	//  Registry[clusterversion.ByKey(clusterversion.VersionWhatever)] = WhateverMigration
}

// Migration defines a program to be executed once every node in the cluster is
// (a) running a specific binary version, and (b) has completed all prior
// migrations.
//
// Each migration is associated with a specific internal cluster version and is
// idempotent in nature. When setting the cluster version (via `SET CLUSTER
// SETTING version`), a manager process determines the set of migrations needed
// to bridge the gap between the current active cluster version, and the target
// one.
//
// To introduce a migration, introduce a version key in pkg/clusterversion, and
// introduce a corresponding internal cluster version for it. See [1] for
// details. Following that, define a Migration in this package and add it to the
// Registry. Be sure to key it in with the new cluster version we just added.
// During cluster upgrades, once the operator is able to set a cluster version
// setting that's past the version that was introduced (typically the major
// release version the migration was introduced in), the manager will execute
// the defined migration before letting the upgrade finalize.
//
// [1]: pkg/clusterversion/cockroach_versions.go
type Migration func(context.Context, *Helper) error
