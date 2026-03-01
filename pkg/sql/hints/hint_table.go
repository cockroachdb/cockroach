// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints

import (
	"context"
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

// Hint represents an unmarshaled hint that is ready to apply to statements.
type Hint struct {
	hintpb.StatementHintUnion

	// The hint should only be applied to statements if Enabled is true.
	Enabled bool

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
// order of creation time (descending), meaning the most recent hints are first.
//
// GetStatementHintsFromDB will return an error if the query returns an
// error. If one of the hints cannot be unmarshalled, the hint (and associated
// fingerprint and ID) will be skipped but no error will be returned.
func GetStatementHintsFromDB(
	ctx context.Context, ex isql.Executor, statementHash int64, fingerprintFlags tree.FmtFlags,
) (hintIDs []int64, fingerprints []string, hints []Hint, retErr error) {
	const opName = "get-plan-hints"
	const getHintsStmt = `
    SELECT "row_id", "fingerprint", "hint"
    FROM system.statement_hints
    WHERE "hash" = $1
    ORDER BY "created_at" DESC, "row_id" DESC`
	it, queryErr := ex.QueryIteratorEx(
		ctx, opName, nil /* txn */, sessiondata.NodeUserSessionDataOverride,
		getHintsStmt, statementHash,
	)
	if queryErr != nil {
		return nil, nil, nil, queryErr
	}
	defer func() {
		retErr = errors.CombineErrors(retErr, it.Close())
	}()

	// To make hint injection more comprehensible, we only consider applying the
	// newest hint-injection hint we find, which should be first, and ignore older
	// hint-injection hints. This is true even if we are unable to decode or apply
	// the newest hint-injection hint for some reason. seenHintInjection tracks
	// whether we've seen a hint injection for a given fingerprint.
	seenHintInjection := make(map[string]struct{})

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
			// Do not return the error. Instead we'll simply execute the query without
			// this hint (which should already be disabled).
			hint.Enabled = false
		}

		// Ignore hint injections that are not the newest hint injection.
		if hint.InjectHints != nil {
			if _, ok := seenHintInjection[fingerprint]; ok {
				hint.Enabled = false
			} else {
				seenHintInjection[fingerprint] = struct{}{}
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
	hint.Enabled = true
	return hintID, fingerprint, hint
}

// InsertHintIntoDB inserts a statement hint into the system.statement_hints
// table. It returns the hint ID of the newly inserted hint if successful.
func InsertHintIntoDB(
	ctx context.Context,
	settings *cluster.Settings,
	txn isql.Txn,
	fingerprint string,
	hint hintpb.StatementHintUnion,
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
		insertStmt = `INSERT INTO system.statement_hints ("fingerprint", "hint", "hint_type", "enabled") VALUES ($1, $2, $3, $4) RETURNING "row_id"`
		args = []interface{}{fingerprint, hintBytes, hintpb.HintTypeRewriteInlineHints, true}
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

// Size returns an estimate of the memory usage of the Hint in bytes.
func (hint *Hint) Size() int64 {
	res := int64(unsafe.Sizeof(*hint))
	res += int64(hint.StatementHintUnion.Size())
	if hint.HintInjectionDonor != nil {
		res += hint.HintInjectionDonor.Size()
	}
	return res
}
