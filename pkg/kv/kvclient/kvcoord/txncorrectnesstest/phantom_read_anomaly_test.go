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

// TestTxnDBPhantomReadAnomaly verifies that neither SI nor SSI isolation
// are subject to the phantom reads anomaly. This anomaly is prevented by
// the SQL ANSI SERIALIZABLE isolation level, though it's also prevented
// by snapshot isolation (i.e. Oracle's traditional "serializable").
//
// Phantom reads occur when a single txn does two identical queries but
// ends up reading different results. This is a variant of non-repeatable
// reads, but is special because it requires the database to be aware of
// ranges when settling concurrency issues.
//
// Phantom reads would typically fail with a history such as:
//
//	R2(B) SC1(A-C) I2(B) C2 SC1(A-C) C1
func TestTxnDBPhantomReadAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txn1 := "SC(A-C) W(D,A+B) SC(A-C) W(E,A+B) C"
	txn2 := "R(B) I(B) C"
	verify := &verifier{
		history: "R(D) R(E)",
		checkFn: func(env map[string]int64) error {
			if env["D"] != env["E"] {
				return errors.Errorf("expected D == E (%d != %d)", env["D"], env["E"])
			}
			return nil
		},
	}
	checkConcurrency("phantom read", serializableAndSnapshot, []string{txn1, txn2}, verify, t)
}
