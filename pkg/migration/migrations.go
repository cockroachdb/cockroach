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

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// Registry defines the global mapping between a version, and the associated
// migration. The migration is only executed after a cluster-wide bump of the
// version gate.
var Registry = make(map[roachpb.Version]Migration)

func init() {
	// TODO(irfansharif): We'll want to register individual migrations with
	// specific internal cluster versions here.
	//
	//  Registry[clusterversion.VersionByKey(clusterversion.VersionWhatever)] = WhateverMigration
}
