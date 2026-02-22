// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaobjectlimit

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlclustersettings"
	"github.com/cockroachdb/cockroach/pkg/sql/stats"
	"github.com/cockroachdb/errors"
)

// CheckMaxSchemaObjects validates if the number of schema objects in the cluster plus the new
// objects being created would exceed the configured limit.
// It attempts to use cached table statistics for performance, and will not return
// an error if the cached stats are not available.
func CheckMaxSchemaObjects(
	ctx context.Context,
	txn isql.Txn,
	descsCollection *descs.Collection,
	tableStatsCache *stats.TableStatisticsCache,
	settings *cluster.Settings,
	numNewObjects int,
) error {
	// Skip the check for internal operations (upgrades, system tables, etc.).
	if txn.SessionData().Internal {
		return nil
	}

	limit := sqlclustersettings.ApproxMaxSchemaObjectCount.Get(&settings.SV)
	if limit == 0 {
		// Limit disabled.
		return nil
	}

	var currentCount int64

	// Try to get the row count from cached table statistics.
	desc, err := descsCollection.ByIDWithLeased(txn.KV()).Get().Table(ctx, keys.DescriptorTableID)
	if err != nil {
		return err
	}
	tableStats, err := tableStatsCache.GetFreshTableStats(ctx, desc, nil /* typeResolver */)
	if err != nil {
		return err
	}
	if len(tableStats) > 0 {
		// Use the row count from the most recent statistic.
		currentCount = int64(tableStats[0].RowCount)
	}

	// If we couldn't get stats, allow the creation.
	if currentCount == 0 {
		return nil
	}

	projectedCount := currentCount + int64(numNewObjects)

	if projectedCount > limit {
		return errors.WithHint(
			pgerror.Newf(
				pgcode.ConfigurationLimitExceeded,
				"cannot create new schema object(s): would exceed approximate maximum (%d); "+
					"current count: %d",
				limit, currentCount,
			),
			"You can increase the limit by adjusting the cluster setting "+
				string(sqlclustersettings.ApproxMaxSchemaObjectCount.Name()),
		)
	}
	return nil
}
