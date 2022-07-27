package keyvisstorage

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

func DeleteSamplesBeforeUnixTime(
	ctx context.Context,
	ie *sql.InternalExecutor,
	seconds int64,
) error {

	// delete samples and sample buckets that have expired
	stmt := fmt.Sprintf("WITH deleted_rows AS (DELETE FROM system."+
		"span_stats_samples WHERE sample_time < CAST("+
		"%d AS TIMESTAMP) RETURNING id) DELETE FROM system."+
		"span_stats_buckets WHERE sample_id IN (SELECT id FROM deleted_rows)",
		seconds)

	_, err := ie.ExecEx(ctx, "delete-expired-samples", nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()}, stmt)

	if err != nil {
		return err
	}


	// delete keys that are no longer referenced by any buckets.
	deleteKeysStmt := "DELETE FROM system." +
		"span_stats_unique_keys WHERE NOT EXISTS (" +
		"SELECT * FROM system.span_stats_buckets " +
		"WHERE system.span_stats_buckets.start_key_id = system." +
		"span_stats_unique_keys.id OR system.span_stats_buckets." +
		"end_key_id = system.span_stats_unique_keys.id)"

	_, err = ie.ExecEx(
		ctx,
		"delete-unused-start-keys",
		nil,
		sessiondata.InternalExecutorOverride{User: username.RootUserName()},
		deleteKeysStmt,
	)
	return err
}
