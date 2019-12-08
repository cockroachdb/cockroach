// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

func TestSavepoints(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, "testdata/savepoints", func(t *testing.T, path string) {
		// New database for each test file.
		s := createTestDB(t)
		defer s.Stop()
		ctx := context.Background()

		// Transient state during the test.
		sp := make(map[string]client.SavepointToken)
		var txn *client.Txn

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			var buf strings.Builder

			ptxn := func() {
				tc := txn.Sender().(*TxnCoordSender)
				fmt.Fprintf(&buf, "%d ", tc.interceptorAlloc.txnSeqNumAllocator.writeSeq)
				if len(tc.mu.txn.IgnoredSeqNums) == 0 {
					buf.WriteString("<noignore>")
				}
				for _, r := range tc.mu.txn.IgnoredSeqNums {
					fmt.Fprintf(&buf, "[%d-%d]", r.Start, r.End)
				}
				fmt.Fprintln(&buf)
			}

			switch td.Cmd {
			case "begin":
				txn = client.NewTxn(ctx, s.DB, 0)
				ptxn()

			case "commit":
				if err := txn.CommitOrCleanup(ctx); err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				}

			case "retry":
				epochBefore := txn.Epoch()
				retryErr := txn.GenerateForcedRetryableError(ctx, "forced retry")
				epochAfter := txn.Epoch()
				fmt.Fprintf(&buf, "synthetic error: %v\n", retryErr)
				fmt.Fprintf(&buf, "epoch: %d -> %d\n", epochBefore, epochAfter)

			case "abort":
				// TODO(knz): add code missing here: simulate an abort error
				// that bumps the txn ID.
				t.Error("unsupported yet")

			case "put":
				if err := txn.Put(ctx,
					roachpb.Key(td.CmdArgs[0].Key),
					[]byte(td.CmdArgs[1].Key)); err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				}

			case "get":
				v, err := txn.Get(ctx, td.CmdArgs[0].Key)
				if err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				} else {
					ba, _ := v.Value.GetBytes()
					fmt.Fprintf(&buf, "%v -> %v\n", v.Key, string(ba))
				}

			case "savepoint":
				spt, err := txn.CreateSavepoint(ctx)
				if err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				} else {
					sp[td.CmdArgs[0].Key] = spt
					ptxn()
				}

			case "release":
				spn := td.CmdArgs[0].Key
				spt := sp[spn]
				if err := txn.ReleaseSavepoint(ctx, spt); err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				} else {
					ptxn()
				}

			case "rollback":
				spn := td.CmdArgs[0].Key
				spt := sp[spn]
				if err := txn.RollbackToSavepoint(ctx, spt); err != nil {
					fmt.Fprintf(&buf, "(%T) %v\n", err, err)
				} else {
					ptxn()
				}

			default:
				td.Fatalf(t, "unknown directive: %s", td.Cmd)
			}
			return buf.String()
		})
	})
}
