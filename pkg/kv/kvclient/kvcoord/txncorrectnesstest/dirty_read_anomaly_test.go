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

// TestTxnDBDirtyReadAnomaly verifies that none of RC, SI, or SSI
// isolation are subject to the dirty read anomaly.
//
// With dirty reads, a transaction writes to a key while a concurrent
// transaction reads from that key. The writing transaction then rolls
// back. If the reading transaction observes the write, either before or
// after the rollback, it has experienced a dirty read.
//
// Dirty reads would typically fail with a history such as:
//
//	W1(A) R2(A) A1 C2
func TestTxnDBDirtyReadAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txn1 := "W(A,1) A"
	txn2 := "R(A) W(B,A) C"
	verify := &verifier{
		history: "R(B)",
		checkFn: func(env map[string]int64) error {
			if env["B"] != 0 {
				return errors.Errorf("expected B=0, got %d", env["B"])
			}
			return nil
		},
	}
	checkConcurrency("dirty read", allLevels, []string{txn1, txn2}, verify, t)
}
