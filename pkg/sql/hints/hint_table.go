// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints

import (
	"context"
	"fmt"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parserutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// nodeUserLocalOnly is a session data override that forces local (non-DistSQL)
// execution. Statement hint queries are simple point lookups that should not
// incur the overhead of distributed execution.
var nodeUserLocalOnly = sessiondata.InternalExecutorOverride{
	User:          sessiondata.NodeUserSessionDataOverride.User,
	MultiOverride: "distsql=off",
}

// Hint represents an unmarshaled hint that is ready to apply to statements.
type Hint struct {
	hintpb.StatementHintUnion

	// The hint should only be applied to statements if Enabled is true.
	Enabled bool

	// Database is the database to which this hint is scoped. If empty, the hint
	// applies regardless of the current database.
	Database string

	// If Err is not nil it was an error encountered while loading the hint from
	// system.statement_hints, and Enabled will be false.
	Err error

	// HintInjectionDonor is the fully parsed donor statement fingerprint used for
	// hint injection.
	HintInjectionDonor *tree.HintInjectionDonor
}

// CheckForStatementHintsInDB queries the system.statement_hints table to
// determine if there are any hints for the given fingerprint hash. The caller
// must be able to retry if an error is returned.
func CheckForStatementHintsInDB(
	ctx context.Context, ex isql.Executor, statementHash int64,
) (hasHints bool, retErr error) {
	const opName = "get-plan-hints"
	const getHintsStmt = `SELECT hash FROM system.statement_hints WHERE "hash" = $1 LIMIT 1`
	it, err := ex.QueryIteratorEx(
		ctx, opName, nil /* txn */, nodeUserLocalOnly,
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
// order of creation time (descending), meaning the most recent hints are first.
//
// GetStatementHintsFromDB will return an error if the query returns an
// error. If one of the hints cannot be unmarshalled, the hint (and associated
// fingerprint and ID) will be skipped but no error will be returned.
func GetStatementHintsFromDB(
	ctx context.Context,
	settings *cluster.Settings,
	ex isql.Executor,
	statementHash int64,
	fingerprintFlags tree.FmtFlags,
) (hintIDs []int64, fingerprints []string, hints []Hint, retErr error) {
	const opName = "get-plan-hints"
	var getHintsStmt string
	hasEnabledCol := settings.Version.IsActive(
		ctx, clusterversion.V26_2_StatementHintsTypeNameEnabledColumnsAdded,
	)
	if hasEnabledCol {
		getHintsStmt = `
    SELECT "row_id", "fingerprint", "hint", "enabled", "database"
    FROM system.statement_hints
    WHERE "hash" = $1
    ORDER BY "created_at" DESC, "row_id" DESC`
	} else {
		getHintsStmt = `
    SELECT "row_id", "fingerprint", "hint", true AS "enabled", NULL AS "database"
    FROM system.statement_hints
    WHERE "hash" = $1
    ORDER BY "created_at" DESC, "row_id" DESC`
	}
	it, queryErr := ex.QueryIteratorEx(
		ctx, opName, nil /* txn */, nodeUserLocalOnly,
		getHintsStmt, statementHash,
	)
	if queryErr != nil {
		return nil, nil, nil, queryErr
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()

	// To make hint injection more comprehensible, we de-duplicate hint injections
	// and session variable hints for the same variable. When resolving
	// duplicates, we apply the newer of the two hints, which should be the first
	// of the two we encounter. This is true even if we are unable to decode or
	// apply the newer hint for some reason.
	//
	// seenInjections tracks which fingerprints already have a hint injection.
	// seenVarHints tracks which (fingerprint, variable name) pairs already have
	// a session variable hint.
	seenInjections := make(map[string]struct{})
	seenVarHints := make(map[[2]string]struct{})

	for {
		ok, queryErr := it.Next(ctx)
		if queryErr != nil {
			return nil, nil, nil, queryErr
		}
		if !ok {
			break
		}
		hintID, fingerprint, hint := parseHint(it.Cur(), fingerprintFlags)
		if hint.Err != nil {
			log.Dev.Warningf(
				ctx, "could not decode hint ID %v for statement hash %v fingerprint %v: %v",
				hintID, statementHash, fingerprint, hint.Err,
			)
			// Do not return the error. Instead, we'll simply execute the query without
			// this hint (which should already be disabled).
			hint.Enabled = false
		}

		// Resolve duplicate hints by picking the newer one (which will be ordered
		// first by the query).
		switch t := hint.GetValue().(type) {
		case *hintpb.InjectHints:
			if _, ok := seenInjections[fingerprint]; ok {
				hint.Enabled = false
			} else {
				seenInjections[fingerprint] = struct{}{}
			}
		case *hintpb.SessionVariableHint:
			key := [2]string{fingerprint, t.VariableName}
			if _, ok := seenVarHints[key]; ok {
				hint.Enabled = false
			} else {
				seenVarHints[key] = struct{}{}
			}
		}

		hintIDs = append(hintIDs, hintID)
		fingerprints = append(fingerprints, fingerprint)
		hints = append(hints, hint)
	}
	return hintIDs, fingerprints, hints, nil
}

func parseHint(
	datums tree.Datums, fingerprintFlags tree.FmtFlags,
) (hintID int64, fingerprint string, hint Hint) {
	defer errorutil.MaybeCatchPanic(&hint.Err, nil /* errCallback */)
	hintID = int64(tree.MustBeDInt(datums[0]))
	fingerprint = string(tree.MustBeDString(datums[1]))
	hint.StatementHintUnion, hint.Err = hintpb.FromBytes([]byte(tree.MustBeDBytes(datums[2])))
	if hint.Err != nil {
		return hintID, fingerprint, hint
	}
	if hint.InjectHints != nil && hint.InjectHints.DonorSQL != "" {
		donorStmt, err := parserutils.ParseOne(hint.InjectHints.DonorSQL)
		if err != nil {
			hint.Err = err
			return hintID, fingerprint, hint
		}
		hint.HintInjectionDonor, hint.Err = tree.NewHintInjectionDonor(donorStmt.AST, fingerprintFlags)
		if hint.Err != nil {
			return hintID, fingerprint, hint
		}
	}
	hint.Enabled = bool(tree.MustBeDBool(datums[3]))
	if datums[4] != tree.DNull {
		hint.Database = string(tree.MustBeDString(datums[4]))
	}
	return hintID, fingerprint, hint
}

// InsertHintIntoDB inserts a statement hint into the system.statement_hints
// table. It returns the hint ID of the newly inserted hint if successful. If
// optDatabase is non-empty and the cluster version supports it, the hint is
// scoped to the given database.
func InsertHintIntoDB(
	ctx context.Context,
	settings *cluster.Settings,
	txn isql.Txn,
	fingerprint string,
	hint hintpb.StatementHintUnion,
	optDatabase string,
) (int64, error) {
	const opName = "insert-statement-hint"
	hintBytes, err := hintpb.ToBytes(hint)
	if err != nil {
		return 0, err
	}

	// Check whether the hint_type column has been added by the migration. If
	// not, fall back to inserting without it.
	var insertStmt string
	var args []interface{}
	if settings.Version.IsActive(ctx, clusterversion.V26_2_StatementHintsTypeNameEnabledColumnsAdded) {
		if optDatabase != "" {
			insertStmt = `INSERT INTO system.statement_hints ("fingerprint", "hint", "hint_type", "enabled", "database") VALUES ($1, $2, $3, $4, $5) RETURNING "row_id"`
			args = []interface{}{fingerprint, hintBytes, hint.HintType(), true, optDatabase}
		} else {
			insertStmt = `INSERT INTO system.statement_hints ("fingerprint", "hint", "hint_type", "enabled") VALUES ($1, $2, $3, $4) RETURNING "row_id"`
			args = []interface{}{fingerprint, hintBytes, hint.HintType(), true}
		}
	} else {
		insertStmt = `INSERT INTO system.statement_hints ("fingerprint", "hint") VALUES ($1, $2) RETURNING "row_id"`
		args = []interface{}{fingerprint, hintBytes}
	}

	row, err := txn.QueryRowEx(
		ctx, opName, txn.KV(), sessiondata.NodeUserSessionDataOverride,
		insertStmt, args...,
	)
	if err != nil {
		return 0, err
	}
	// TODO(michae2,drewk): Consider calling
	// StatementHintsCache.handleIncrementalUpdate here to eagerly update the
	// local node's cache.
	return int64(tree.MustBeDInt(row[0])), nil
}

// DeleteHintFromDB deletes statement hints from system.statement_hints,
// filtered by row ID or fingerprint. If the provided rowID is zero, we don't
// filter on row ID. If the fingerprint is empty string, we don't filter on
// fingerprint. Returns the row_id, fingerprint, and raw hint protobuf bytes of
// all deleted rows.
func DeleteHintFromDB(
	ctx context.Context, txn isql.Txn, rowID int64, fingerprint string, optDatabase string,
) (rowIDs []int64, fingerprints []string, hintBytes [][]byte, err error) {
	const opName = "delete-statement-hint"
	filterCols := make([]string, 0, 3)
	vals := make([]interface{}, 0, 3)
	if rowID != 0 {
		filterCols = append(filterCols, `"row_id"`)
		vals = append(vals, rowID)
	}
	if fingerprint != "" {
		filterCols = append(filterCols, "fingerprint")
		vals = append(vals, fingerprint)
	}
	if optDatabase != "" {
		filterCols = append(filterCols, `"database"`)
		vals = append(vals, optDatabase)
	}

	if len(filterCols) == 0 {
		return nil, nil, nil, errors.New("delete statement hint must specify at least one of row_id or fingerprint")
	}

	var b strings.Builder
	for i, filterCol := range filterCols {
		_, err := fmt.Fprintf(&b, "%s = $%d", filterCol, i+1)
		if err != nil {
			return nil, nil, nil, err
		}
		if i != len(filterCols)-1 {
			b.WriteString(" AND ")
		}
	}
	rows, err := txn.QueryBufferedEx(
		ctx, opName, txn.KV(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf("DELETE FROM system.statement_hints WHERE %s RETURNING row_id, fingerprint, hint", b.String()), vals...,
	)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, row := range rows {
		if len(row) < 3 {
			return nil, nil, nil, errors.AssertionFailedf("expecting 3 columns from query, got %d", len(row))
		}
		rowIDs = append(rowIDs, int64(tree.MustBeDInt(row[0])))
		fingerprints = append(fingerprints, string(tree.MustBeDString(row[1])))
		hintBytes = append(hintBytes, []byte(tree.MustBeDBytes(row[2])))
	}
	// TODO(michae2,drewk): Consider calling
	// StatementHintsCache.handleIncrementalUpdate here to eagerly update the
	// local node's cache.
	return rowIDs, fingerprints, hintBytes, nil
}

// SetHintEnabledInDB updates the enabled status of statement hints in
// system.statement_hints, filtered by row ID or fingerprint. If the provided
// rowID is zero, we don't filter on row ID. If the fingerprint is empty string,
// we don't filter on fingerprint. Returns the number of affected rows.
func SetHintEnabledInDB(
	ctx context.Context,
	settings *cluster.Settings,
	txn isql.Txn,
	rowID int64,
	fingerprint string,
	enabled bool,
	optDatabase string,
) (int64, error) {
	const opName = "set-statement-hint-enabled"

	// This is implemented as a DELETE followed by an INSERT (rather than an
	// UPDATE) so that secondary index entries are recreated, which triggers
	// rangefeed cache invalidation. A simple UPDATE on the enabled column would
	// not fire a rangefeed event because the watched hash_idx index doesn't
	// include that column.
	if !settings.Version.IsActive(ctx, clusterversion.V26_2_StatementHintsTypeNameEnabledColumnsAdded) {
		return 0, errors.New("cannot set statement hint enabled: enabled column not yet available")
	}
	filterCols := make([]string, 0, 3)
	vals := make([]interface{}, 0, 3)
	if rowID != 0 {
		filterCols = append(filterCols, `"row_id"`)
		vals = append(vals, rowID)
	}
	if fingerprint != "" {
		filterCols = append(filterCols, `"fingerprint"`)
		vals = append(vals, fingerprint)
	}
	if optDatabase != "" {
		filterCols = append(filterCols, `"database"`)
		vals = append(vals, optDatabase)
	}

	if len(filterCols) == 0 {
		return 0, errors.New(
			"set statement hint enabled must specify at least one of row_id or fingerprint",
		)
	}

	// Step 1: DELETE matching rows and capture their data.
	var whereClause strings.Builder
	for i, filterCol := range filterCols {
		if i > 0 {
			whereClause.WriteString(" AND ")
		}
		if _, err := fmt.Fprintf(&whereClause, "%s = $%d", filterCol, i+1); err != nil {
			return 0, err
		}
	}
	deletedRows, err := txn.QueryBufferedEx(
		ctx, opName, txn.KV(), sessiondata.NodeUserSessionDataOverride,
		fmt.Sprintf(
			`DELETE FROM system.statement_hints WHERE %s`+
				` RETURNING row_id, fingerprint, hint, created_at, hint_type, hint_name, database`,
			whereClause.String(),
		),
		vals...,
	)
	if err != nil {
		return 0, err
	}
	if len(deletedRows) == 0 {
		return 0, nil
	}

	// Step 2: INSERT all rows back with the new enabled value. The hash column
	// is computed/stored and will be recomputed automatically.
	var insertBuf strings.Builder
	insertBuf.WriteString(
		`INSERT INTO system.statement_hints` +
			` (row_id, fingerprint, hint, created_at, hint_type, hint_name, enabled, database)` +
			` VALUES `,
	)
	insertArgs := make([]interface{}, 0, len(deletedRows)*8)
	for i, row := range deletedRows {
		if i > 0 {
			insertBuf.WriteString(", ")
		}
		base := i*8 + 1
		fmt.Fprintf(&insertBuf, "($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
			base, base+1, base+2, base+3, base+4, base+5, base+6, base+7,
		)
		// row columns: 0=row_id, 1=fingerprint, 2=hint, 3=created_at,
		// 4=hint_type, 5=hint_name, 6=database
		insertArgs = append(insertArgs,
			row[0], row[1], row[2], row[3], row[4], row[5], enabled, row[6],
		)
	}
	_, err = txn.ExecEx(
		ctx, opName, txn.KV(), sessiondata.NodeUserSessionDataOverride,
		insertBuf.String(),
		insertArgs...,
	)
	if err != nil {
		return 0, err
	}
	return int64(len(deletedRows)), nil
}

// Size returns an estimate of the memory usage of the Hint in bytes.
func (hint *Hint) Size() int64 {
	res := int64(unsafe.Sizeof(*hint))
	res += int64(hint.StatementHintUnion.Size())
	res += int64(len(hint.Database))
	if hint.HintInjectionDonor != nil {
		res += hint.HintInjectionDonor.Size()
	}
	return res
}
