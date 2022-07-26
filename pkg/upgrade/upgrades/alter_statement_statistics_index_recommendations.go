// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
)

const addIndexRecommendationsCol = `
ALTER TABLE system.statement_statistics
ADD COLUMN IF NOT EXISTS "index_recommendations" STRING[] NOT NULL DEFAULT (array[]::STRING[])
FAMILY "primary"
`

func alterSystemStatementStatisticsAddIndexRecommendations(
	ctx context.Context, cs clusterversion.ClusterVersion, d upgrade.TenantDeps, _ *jobs.Job,
) error {
	op := operation{
		name:           "add-statement-statistics-index-recommendations-col",
		schemaList:     []string{"index_rec"},
		query:          addIndexRecommendationsCol,
		schemaExistsFn: hasColumn,
	}
	if err := migrateTable(ctx, cs, d, op, keys.StatementStatisticsTableID, systemschema.StatementStatisticsTable); err != nil {
		return err
	}
	return nil
}
