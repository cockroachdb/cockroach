// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txncorrectnesstest

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// TestTxnDBDirtyWriteAnomaly verifies that none of RC, SI, or SSI
// isolation are subject to the dirty write anomaly.
//
// With dirty writes, two transactions concurrently write to the same
// key(s). If one transaction then rolls back, the final state must
// reflect the write performed by the committing transaction. Crucially,
// the rollback must not interfere with the write from the committing
// transaction.
//
// Two closely related cases are when both transactions roll back and
// when both transactions commit. In the first case, the final state
// must reflect neither write. In the second case, the final state must
// reflect a consistent commit order across keys, such that all written
// keys reflect writes from the second transaction to commit.
//
// Dirty writes would typically fail with a history such as:
//
//	W1(A) W2(A) (C1 or A1) (C2 or A2)

func TestTxnDBDirtyWriteAnomalyOneAbortOneCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txn1 := "W(A,1) W(B,1) A"
	txn2 := "W(A,2) W(B,2) C"
	verify := &verifier{
		history: "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if env["A"] != 2 || env["B"] != 2 {
				return errors.Errorf("expected A=2 and B=2, got A=%d and B=%d", env["A"], env["B"])
			}
			return nil
		},
	}
	checkConcurrency("dirty write", allLevels, []string{txn1, txn2}, verify, t)
}

func TestTxnDBDirtyWriteAnomalyBothAbort(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txn1 := "W(A,1) W(B,1) A"
	txn2 := "W(A,2) W(B,2) A"
	verify := &verifier{
		history: "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if env["A"] != 0 || env["B"] != 0 {
				return errors.Errorf("expected A=0 and B=0, got A=%d and B=%d", env["A"], env["B"])
			}
			return nil
		},
	}
	checkConcurrency("dirty write", allLevels, []string{txn1, txn2}, verify, t)
}

func TestTxnDBDirtyWriteAnomalyBothCommit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txn1 := "W(A,1) W(B,1) C"
	txn2 := "W(A,2) W(B,2) C"
	verify := &verifier{
		history: "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if env["A"] != 1 && env["A"] != 2 {
				return errors.Errorf("expected A=1 or A=2, got %d", env["A"])
			}
			if env["A"] != env["B"] {
				return errors.Errorf("expected A == B (%d != %d)", env["A"], env["B"])
			}
			return nil
		},
	}
	checkConcurrency("dirty write", allLevels, []string{txn1, txn2}, verify, t)
}
