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
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
)

func recordsBasedRegistryMigration(
	ctx context.Context, cv clusterversion.ClusterVersion, deps migration.SystemDeps,
) error {
	return deps.Cluster.ForEveryNode(ctx, "deprecate-base-encryption-registry", func(ctx context.Context, client serverpb.MigrationClient) error {
		req := &serverpb.DeprecateBaseEncryptionRegistryRequest{Version: &cv.Version}
		_, err := client.DeprecateBaseEncryptionRegistry(ctx, req)
		return err
	})
}
