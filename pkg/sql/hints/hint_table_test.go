// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package hints_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/hintpb"
	"github.com/cockroachdb/cockroach/pkg/sql/hints"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parserutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestHintTableOperations tests the DB-interfacing functions in hint_table.go:
// CheckForStatementHintsInDB, GetStatementHintsFromDB, and InsertHintIntoDB.
func TestHintTableOperations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	ts := srv.ApplicationLayer()
	db := ts.InternalDB().(descs.DB)
	ex := db.Executor()

	st := cluster.MakeTestingClusterSettings()
	fingerprintFlags := tree.FmtFlags(tree.QueryFormattingForFingerprintsMask.Get(&st.SV))

	// Create test hints.
	fingerprint1 := "SELECT a FROM t WHERE b = $1"
	fingerprint2 := "SELECT c FROM t WHERE d = $2"
	hash1 := computeHash(t, fingerprint1)
	hash2 := computeHash(t, fingerprint2)

	var hint1, hint2 hints.Hint
	hint1.SetValue(&hintpb.InjectHints{DonorSQL: "SELECT a FROM t@t_b_idx WHERE b = $1"})
	hint2.SetValue(&hintpb.InjectHints{DonorSQL: "SELECT c FROM t@{NO_FULL_SCAN} WHERE d = $2"})
	hint1.Enabled = true
	hint2.Enabled = true
	var err error
	donorStmt1, err := parserutils.ParseOne(hint1.InjectHints.DonorSQL)
	require.NoError(t, err)
	donorStmt2, err := parserutils.ParseOne(hint2.InjectHints.DonorSQL)
	require.NoError(t, err)
	hint1.HintInjectionDonor, err = tree.NewHintInjectionDonor(donorStmt1.AST, fingerprintFlags)
	require.NoError(t, err)
	hint2.HintInjectionDonor, err = tree.NewHintInjectionDonor(donorStmt2.AST, fingerprintFlags)
	require.NoError(t, err)

	// Check for nonexistent hints.
	hasHints, err := hints.CheckForStatementHintsInDB(ctx, ex, hash1)
	require.NoError(t, err)
	require.False(t, hasHints)

	// Retrieve nonexistent hints.
	hintIDs, fingerprints, hintsFromDB, err := hints.GetStatementHintsFromDB(
		ctx, ex, hash1, fingerprintFlags,
	)
	require.NoError(t, err)
	require.Empty(t, hintIDs)
	require.Empty(t, fingerprints)
	require.Empty(t, hintsFromDB)

	// Insert a hint.
	var insertedHintID1 int64
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		insertedHintID1, err = hints.InsertHintIntoDB(ctx, ts.ClusterSettings(), txn, fingerprint1, hint1.StatementHintUnion)
		return err
	})
	require.NoError(t, err)
	require.Greater(t, insertedHintID1, int64(0)) // Should return a valid ID.

	// Check for the inserted hint.
	hasHints, err = hints.CheckForStatementHintsInDB(ctx, ex, hash1)
	require.NoError(t, err)
	require.True(t, hasHints)

	// Fetch the inserted hint.
	hintIDs, fingerprints, hintsFromDB, err = hints.GetStatementHintsFromDB(
		ctx, ex, hash1, fingerprintFlags,
	)
	require.NoError(t, err)
	require.Len(t, hintIDs, 1)
	require.Len(t, fingerprints, 1)
	require.Len(t, hintsFromDB, 1)
	require.Equal(t, fingerprint1, fingerprints[0])
	require.Equal(t, hint1, hintsFromDB[0])
	require.Equal(t, insertedHintID1, hintIDs[0])

	// Insert multiple hints for the same fingerprint.
	var insertedHintID2 int64
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		insertedHintID2, err = hints.InsertHintIntoDB(ctx, ts.ClusterSettings(), txn, fingerprint1, hint1.StatementHintUnion)
		return err
	})
	require.NoError(t, err)
	require.Greater(t, insertedHintID2, int64(0))
	require.NotEqual(t, insertedHintID1, insertedHintID2)

	// Fetch all hints for the fingerprint.
	hintIDs, fingerprints, hintsFromDB, err = hints.GetStatementHintsFromDB(
		ctx, ex, hash1, fingerprintFlags,
	)
	require.NoError(t, err)
	require.Len(t, hintIDs, 2)
	require.Len(t, fingerprints, 2)
	require.Len(t, hintsFromDB, 2)
	require.Equal(t, fingerprint1, fingerprints[0])
	require.Equal(t, fingerprint1, fingerprints[1])
	require.Greater(t, hintIDs[0], hintIDs[1])

	// Insert hint for different fingerprint.
	var insertedHintID3 int64
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		insertedHintID3, err = hints.InsertHintIntoDB(ctx, ts.ClusterSettings(), txn, fingerprint2, hint2.StatementHintUnion)
		return err
	})
	require.NoError(t, err)
	require.Greater(t, insertedHintID3, int64(0))

	// Retrieve hint for the new fingerprint.
	hintIDs2, fingerprints2, hintsFromDB2, err := hints.GetStatementHintsFromDB(
		ctx, ex, hash2, fingerprintFlags,
	)
	require.NoError(t, err)
	require.Len(t, hintIDs2, 1)
	require.Len(t, fingerprints2, 1)
	require.Len(t, hintsFromDB2, 1)
	require.Equal(t, fingerprint2, fingerprints2[0])
	require.Equal(t, insertedHintID3, hintIDs2[0])

	// Test InsertHintIntoDB with empty fingerprint and hint.
	var emptyFingerprintHintID int64
	var hintEmpty hintpb.StatementHintUnion
	hintEmpty.SetValue(&hintpb.InjectHints{})
	err = db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		emptyFingerprintHintID, err = hints.InsertHintIntoDB(ctx, ts.ClusterSettings(), txn, "", hintEmpty)
		return err
	})
	require.NoError(t, err)
	require.Greater(t, emptyFingerprintHintID, int64(0))
}
