package insights

import (
	"context"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
	"strings"
)

// PersistTxnExecInsight persists transaction execution insights in system.txn_exec_insights table.
func PersistTxnExecInsight(ctx context.Context, txn isql.Txn, insight *Insight) (rowsAffected int, err error) {
	txnExecInsight := insight.Transaction
	stmtExecutionIDs := util.Map(txnExecInsight.StmtExecutionIDs, func(id clusterunique.ID) string {
		return id.String()
	})
	insightProblems := util.Map(txnExecInsight.Problems, func(p Problem) string {
		return p.String()
	})
	insightCauses := util.Map(txnExecInsight.Causes, func(c Cause) string {
		return c.String()
	})
	values := "$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20"
	args := []interface{}{
		txnExecInsight.ID.String(),
		sqlstatsutil.EncodeUint64ToBytes(uint64(txnExecInsight.FingerprintID)),
		txnExecInsight.ImplicitTxn,
		insight.Session.ID.String(),
		txnExecInsight.StartTime,
		txnExecInsight.EndTime,
		txnExecInsight.User,
		txnExecInsight.ApplicationName,
		txnExecInsight.RowsRead,
		txnExecInsight.RowsWritten,
		txnExecInsight.UserPriority,
		txnExecInsight.RetryCount,
		txnExecInsight.AutoRetryReason,
		*txnExecInsight.Contention,
		insightProblems,
		insightCauses,
		stmtExecutionIDs,
		txnExecInsight.CPUSQLNanos,
		txnExecInsight.LastErrorCode,
		txnExecInsight.Status,
	}

	if len(args) != len(strings.Split(values, ",")) {
		return 0, errors.Newf("number of placeholders and values isn't equal: %d and %d", len(values), len(args))
	}

	statement := fmt.Sprintf(`
		INSERT INTO system.txn_exec_insights
		(
		 	txn_id,
			txn_fingerprint_id,
			implicit_txn,
			session_id,
			start_time,
			end_time,
			user_name,
			app_name,
			rows_read,
			rows_written,
			user_priority,
			retries,
			last_retry_reason,
			contention,
			problems,
			causes,
			stmt_execution_ids,
			cpu_sql_nanos,
			last_error_code,
			status
		)
		VALUES (%s)
		ON CONFLICT (txn_id, session_id, start_time, end_time)
		DO NOTHING
	`, values)

	rowsAffected, err = txn.ExecEx(
		ctx,
		"insert-txn-exec-insights",
		txn.KV(), /* txn */
		sessiondata.NodeUserSessionDataOverride,
		statement,
		args...,
	)

	return rowsAffected, err
}
