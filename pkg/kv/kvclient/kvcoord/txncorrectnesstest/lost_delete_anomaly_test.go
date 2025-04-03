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

// TestTxnDBLostDeleteAnomaly verifies that neither SI nor SSI
// isolation are subject to the lost delete anomaly. See #6240.
//
// With lost delete, the two deletions from txn2 are interleaved
// with a read and write from txn1, allowing txn1 to read a pre-
// existing value for A and then write to B, rewriting history
// underneath txn2's deletion of B.
//
// This anomaly is prevented by the use of deletion tombstones,
// even on keys which have no values written.
//
// Lost delete would typically fail with a history such as:
//
//	D2(A) R1(A) D2(B) C2 W1(B,A) C1
func TestTxnDBLostDeleteAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// B must not exceed A.
	txn1 := "R(A) W(B,A) C"
	txn2 := "D(A) D(B) C"
	verify := &verifier{
		preHistory: "W(A,1)",
		history:    "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if env["B"] != 0 && env["A"] == 0 {
				return errors.Errorf("expected B = %d <= %d = A", env["B"], env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update (delete)", serializableAndSnapshot, []string{txn1, txn2}, verify, t)
}
