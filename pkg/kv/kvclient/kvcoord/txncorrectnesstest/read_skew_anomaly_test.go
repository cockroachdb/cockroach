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

// TestTxnDBReadSkewAnomaly verifies that neither SI nor SSI isolation
// are subject to the read skew anomaly, an example of a database
// constraint violation known as inconsistent analysis[^1]. This
// anomaly is prevented by REPEATABLE_READ.
//
// With read skew, there are two concurrent txns. One
// reads keys A & B, the other reads and then writes keys A & B. The
// reader must not see intermediate results from the reader/writer.
//
// Read skew would typically fail with a history such as:
//
//	R1(A) R2(B) I2(B) R2(A) I2(A) R1(B) C1 C2
//
// [^1]: 1995, https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf
func TestTxnDBReadSkewAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txn1 := "R(A) R(B) W(C,A+B) C"
	txn2 := "R(A) R(B) I(A) I(B) C"
	verify := &verifier{
		history: "R(C)",
		checkFn: func(env map[string]int64) error {
			if env["C"] != 2 && env["C"] != 0 {
				return errors.Errorf("expected C to be either 0 or 2, got %d", env["C"])
			}
			return nil
		},
	}
	checkConcurrency("read skew", serializableAndSnapshot, []string{txn1, txn2}, verify, t)
}
