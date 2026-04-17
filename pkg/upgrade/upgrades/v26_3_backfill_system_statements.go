// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package upgrades

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/errors"
)

const backfillBatchSize = 1000

// backfillSystemStatementsTable populates system.statements with
// deduplicated fingerprints from system.statement_statistics. Each
// distinct fingerprint_id is inserted once; existing entries are left
// untouched via ON CONFLICT DO NOTHING.
func backfillSystemStatementsTable(
	ctx context.Context, _ clusterversion.ClusterVersion, d upgrade.TenantDeps,
) error {
	cursor := []byte{}
	for {
		done, newCursor, err := backfillStatementsBatch(ctx, d, cursor)
		if err != nil {
			return errors.Wrap(err, "backfilling system.statements")
		}
		if done {
			return nil
		}
		cursor = newCursor
	}
}

func backfillStatementsBatch(
	ctx context.Context, d upgrade.TenantDeps, cursor []byte,
) (done bool, newCursor []byte, _ error) {
	err := d.DB.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		rows, err := txn.QueryBuffered(ctx,
			"backfill-statements-select", txn.KV(), `
				SELECT DISTINCT ON (fingerprint_id)
				  fingerprint_id,
				  metadata->>'query',
				  metadata->>'querySummary',
				  COALESCE(metadata->>'db', ''),
				  COALESCE((metadata->>'implicitTxn')::BOOL, false)
				FROM system.statement_statistics
				WHERE fingerprint_id > $1
				  AND metadata->>'query' IS NOT NULL
				  AND metadata->>'query' != ''
				  AND metadata->>'querySummary' IS NOT NULL
				  AND metadata->>'querySummary' != ''
				ORDER BY fingerprint_id
				LIMIT $2
			`, cursor, backfillBatchSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			done = true
			return nil
		}
		newCursor = []byte(tree.MustBeDBytes(rows[len(rows)-1][0]))
		done = len(rows) < backfillBatchSize
		return insertBackfillBatch(ctx, txn, rows)
	})
	return done, newCursor, err
}

func insertBackfillBatch(ctx context.Context, txn isql.Txn, rows []tree.Datums) error {
	const colsPerRow = 5
	var sb strings.Builder
	sb.Grow(120 + len(rows)*25)
	sb.WriteString(
		"INSERT INTO system.statements " +
			"(fingerprint_id, fingerprint, summary, db, metadata) VALUES ")
	args := make([]interface{}, 0, len(rows)*colsPerRow)
	for i, row := range rows {
		if i > 0 {
			sb.WriteByte(',')
		}
		p := i*colsPerRow + 1
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d)",
			p, p+1, p+2, p+3, p+4)

		implicitTxn := bool(tree.MustBeDBool(row[4]))
		metadataStr := `{"implicit_txn": false}`
		if implicitTxn {
			metadataStr = `{"implicit_txn": true}`
		}
		args = append(args,
			[]byte(tree.MustBeDBytes(row[0])),
			string(tree.MustBeDString(row[1])),
			string(tree.MustBeDString(row[2])),
			string(tree.MustBeDString(row[3])),
			metadataStr,
		)
	}
	sb.WriteString(" ON CONFLICT (fingerprint_id) DO NOTHING")

	_, err := txn.Exec(ctx,
		"backfill-statements-insert", txn.KV(), sb.String(), args...)
	return err
}
