// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package concurrency_test

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
)

func nextUUID(counter *uint32) uuid.UUID {
	*counter = *counter + 1
	hi := uint64(*counter) << 32
	return uuid.FromUint128(uint128.Uint128{Hi: hi})
}

func scanTimestamp(t *testing.T, d *datadriven.TestData) hlc.Timestamp {
	return scanTimestampWithName(t, d, "ts")
}

func scanTimestampWithName(t *testing.T, d *datadriven.TestData, name string) hlc.Timestamp {
	var tsS string
	d.ScanArgs(t, name, &tsS)
	ts, err := hlc.ParseTimestamp(tsS)
	if err != nil {
		d.Fatalf(t, "%v", err)
	}
	return ts
}

func scanLockDurability(t *testing.T, d *datadriven.TestData) lock.Durability {
	var durS string
	d.ScanArgs(t, "dur", &durS)
	switch durS {
	case "r":
		return lock.Replicated
	case "u":
		return lock.Unreplicated
	default:
		d.Fatalf(t, "unknown lock durability: %s", durS)
		return 0
	}
}

func scanWaitPolicy(t *testing.T, d *datadriven.TestData, required bool) lock.WaitPolicy {
	const key = "wait-policy"
	if !required && !d.HasArg(key) {
		return lock.WaitPolicy_Block
	}
	var policy string
	d.ScanArgs(t, key, &policy)
	switch policy {
	case "block":
		return lock.WaitPolicy_Block
	case "error":
		return lock.WaitPolicy_Error
	default:
		d.Fatalf(t, "unknown wait policy: %s", policy)
		return 0
	}
}

func scanSingleRequest(
	t *testing.T, d *datadriven.TestData, line string, txns map[string]*roachpb.Transaction,
) roachpb.Request {
	cmd, cmdArgs, err := datadriven.ParseLine(line)
	if err != nil {
		d.Fatalf(t, "error parsing single request: %v", err)
		return nil
	}

	fields := make(map[string]string, len(cmdArgs))
	for _, cmdArg := range cmdArgs {
		if len(cmdArg.Vals) != 1 {
			d.Fatalf(t, "unexpected command values: %+v", cmdArg)
			return nil
		}
		fields[cmdArg.Key] = cmdArg.Vals[0]
	}
	mustGetField := func(f string) string {
		v, ok := fields[f]
		if !ok {
			d.Fatalf(t, "missing required field: %s", f)
		}
		return v
	}
	maybeGetSeq := func() enginepb.TxnSeq {
		s, ok := fields["seq"]
		if !ok {
			return 0
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			d.Fatalf(t, "could not parse seq num: %v", err)
		}
		return enginepb.TxnSeq(n)
	}

	switch cmd {
	case "get":
		var r roachpb.GetRequest
		r.Sequence = maybeGetSeq()
		r.Key = roachpb.Key(mustGetField("key"))
		return &r

	case "scan":
		var r roachpb.ScanRequest
		r.Sequence = maybeGetSeq()
		r.Key = roachpb.Key(mustGetField("key"))
		if v, ok := fields["endkey"]; ok {
			r.EndKey = roachpb.Key(v)
		}
		return &r

	case "put":
		var r roachpb.PutRequest
		r.Sequence = maybeGetSeq()
		r.Key = roachpb.Key(mustGetField("key"))
		r.Value.SetString(mustGetField("value"))
		return &r

	case "resolve-intent":
		var r roachpb.ResolveIntentRequest
		r.IntentTxn = txns[mustGetField("txn")].TxnMeta
		r.Key = roachpb.Key(mustGetField("key"))
		r.Status = parseTxnStatus(t, d, mustGetField("status"))
		return &r

	case "request-lease":
		var r roachpb.RequestLeaseRequest
		return &r

	default:
		d.Fatalf(t, "unknown request type: %s", cmd)
		return nil
	}
}

func scanTxnStatus(t *testing.T, d *datadriven.TestData) (roachpb.TransactionStatus, string) {
	var statusStr string
	d.ScanArgs(t, "status", &statusStr)
	status := parseTxnStatus(t, d, statusStr)
	var verb string
	switch status {
	case roachpb.COMMITTED:
		verb = "committing"
	case roachpb.ABORTED:
		verb = "aborting"
	case roachpb.PENDING:
		verb = "increasing timestamp of"
	default:
		d.Fatalf(t, "unknown txn status: %s", status)
	}
	return status, verb
}

func parseTxnStatus(t *testing.T, d *datadriven.TestData, s string) roachpb.TransactionStatus {
	switch s {
	case "committed":
		return roachpb.COMMITTED
	case "aborted":
		return roachpb.ABORTED
	case "pending":
		return roachpb.PENDING
	default:
		d.Fatalf(t, "unknown txn status: %s", s)
		return 0
	}
}
