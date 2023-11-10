package plpgsqlccl

import (
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	plpgsql.CheckClusterSupportsPLpgSQL = checkClusterSupportsPLpgSQL
}

func CheckClusterSupportsPLpgSQL(settings *cluster.Settings, clusterID uuid.UUID) error {
	return utilccl.CheckEnterpriseEnabled(
		settings,
		clusterID,
		"PL/pgSQL",
	)
}
