package migrations

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/migration"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sqlmigrations"
)

func sqlStatementStatsTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	return sqlmigrations.CreateSystemTable(
		ctx, d.DB, d.Codec, d.Settings, systemschema.SQLStatementStatsTable,
	)
}

func sqlTransactionStatsTableMigration(
	ctx context.Context, _ clusterversion.ClusterVersion, d migration.TenantDeps,
) error {
	return sqlmigrations.CreateSystemTable(
		ctx, d.DB, d.Codec, d.Settings, systemschema.SQLTransactionStatsTable,
	)
}
