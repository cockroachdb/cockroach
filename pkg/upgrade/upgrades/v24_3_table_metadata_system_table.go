// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package upgrades

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

// addTableMetadataTableAndJob creates the system.table_metadata table if it does not exist.
func addTableMetadataTableAndJob(
	ctx context.Context, version clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	if err := createSystemTable(ctx, d.DB, d.Settings, d.Codec, systemschema.TableMetadata, tree.LocalityLevelTable); err != nil {
		return err
	}

	// Add job to backfill the table metadata table.
	if err := createUpdateTableMetadataCacheJob(ctx, version, d); err != nil {
		return err
	}

	return nil
}
