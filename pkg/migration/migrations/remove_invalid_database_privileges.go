// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/migration"
)

// runRemoveInvalidDatabasePrivileges calls RunPostDeserializationChanges on
// every database descriptor. It also calls RunPostDeserializationChanges on
// all table descriptors to add constraint IDs.
// This migration is done to convert invalid privileges on the
// database to default privileges.
func runRemoveInvalidDatabasePrivileges(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps, _ *jobs.Job,
) error {
	return runPostDeserializationChangesOnAllDescriptors(ctx, d)
}
