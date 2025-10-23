// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/errors"
)

// CheckForStatementHintsInDB queries the system.statement_hints table to
// determine if there are any hints for the given fingerprint hash. The caller
// must be able to retry if an error is returned.
func CheckForStatementHintsInDB(
	ctx context.Context, ex isql.Executor, statementHash int64,
) (hasHints bool, retErr error) {
	const opName = "get-plan-hints"
	const getHintsStmt = `SELECT hash FROM system.statement_hints WHERE "hash" = $1 LIMIT 1`
	it, err := ex.QueryIteratorEx(
		ctx, opName, nil /* txn */, sessiondata.NodeUserSessionDataOverride,
		getHintsStmt, statementHash,
	)
	if err != nil {
		return false, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()
	return it.Next(ctx)
}

// GetStatementHintsFromDB queries the system.statement_hints table for hints
// matching the given fingerprint hash. It is able to handle the case when
// multiple fingerprints match the hash, as well as the case when there are no
// hints for the fingerprint.
//
// The returned slices (hints, fingerprints, and hintIDs) have the same length.
// fingerprints[i] is the statement fingerprint to which hints[i] applies, while
// hintIDs[i] uniquely identifies a hint in the system table. The results are in
// order of hint ID.
func GetStatementHintsFromDB(
	ctx context.Context, ex isql.Executor, statementHash int64,
) (hintIDs []int64, fingerprints []string, hints []hintpb.StatementHintUnion, retErr error) {
	const opName = "get-plan-hints"
	const getHintsStmt = `
    SELECT "row_id", "fingerprint", "hint"
    FROM system.statement_hints
    WHERE "hash" = $1
    ORDER BY "row_id" ASC`
	it, err := ex.QueryIteratorEx(
		ctx, opName, nil /* txn */, sessiondata.NodeUserSessionDataOverride,
		getHintsStmt, statementHash,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, nil, nil, err
		}
		if !ok {
			break
		}
		datums := it.Cur()
		hintIDs = append(hintIDs, int64(tree.MustBeDInt(datums[0])))
		fingerprints = append(fingerprints, string(tree.MustBeDString(datums[1])))
		hint, err := hintpb.FromBytes([]byte(tree.MustBeDBytes(datums[2])))
		if err != nil {
			return nil, nil, nil, err
		}
		hints = append(hints, hint)
	}
	return hintIDs, fingerprints, hints, nil
}

// InsertHintIntoDB inserts a statement hint into the system.statement_hints
// table. It returns the hint ID of the newly inserted hint if successful.
func InsertHintIntoDB(
	ctx context.Context, txn isql.Txn, fingerprint string, hint hintpb.StatementHintUnion,
) (int64, error) {
	const opName = "insert-statement-hint"
	hintBytes, err := hintpb.ToBytes(hint)
	if err != nil {
		return 0, err
	}
	const insertStmt = `INSERT INTO system.statement_hints ("fingerprint", "hint") VALUES ($1, $2) RETURNING "row_id"`
	row, err := txn.QueryRowEx(
		ctx, opName, txn.KV(), sessiondata.NodeUserSessionDataOverride,
		insertStmt, fingerprint, hintBytes,
	)
	if err != nil {
		return 0, err
	}
	// TODO(michae2,drewk): Consider calling
	// StatementHintsCache.handleIncrementalUpdate here to eagerly update the
	// local node's cache.
	return int64(tree.MustBeDInt(row[0])), nil
}
