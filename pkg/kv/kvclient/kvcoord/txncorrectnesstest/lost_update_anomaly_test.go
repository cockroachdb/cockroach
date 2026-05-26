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

// TestTxnDBLostUpdateAnomaly verifies that neither SI nor SSI isolation
// are subject to the lost update anomaly. This anomaly is prevented
// in most cases by using the READ_COMMITTED ANSI isolation level.
// However, only REPEATABLE_READ fully protects against it.
//
// With lost update, the write from txn1 is overwritten by the write
// from txn2, and thus txn1's update is lost. Both SI and SSI notice
// this write/write conflict and either txn1 or txn2 is aborted,
// depending on priority.
//
// Lost update would typically fail with a history such as:
//
//	R1(A) R2(A) I1(A) I2(A) C1 C2
//
// However, the following variant will cause a lost update in
// READ_COMMITTED and in practice requires REPEATABLE_READ to avoid.
//
//	R1(A) R2(A) I1(A) C1 I2(A) C2
func TestTxnDBLostUpdateAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txn := "R(A) I(A) C"
	verify := &verifier{
		history: "R(A)",
		checkFn: func(env map[string]int64) error {
			if env["A"] != 2 {
				return errors.Errorf("expected A=2, got %d", env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update", serializableAndSnapshot, []string{txn, txn}, verify, t)
}
