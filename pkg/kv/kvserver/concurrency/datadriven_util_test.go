// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package concurrency_test

import (
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/poison"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/dd"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/redact"
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
	ts, err := hlc.ParseTimestamp(dd.ScanArg[string](t, d, name))
	if err != nil {
		d.Fatalf(t, "%v", err)
	}
	return ts
}

func scanTxnPriority(t *testing.T, d *datadriven.TestData) enginepb.TxnPriority {
	priority := scanUserPriority(t, d)
	// NB: don't use roachpb.MakePriority to avoid randomness.
	switch priority {
	case roachpb.MinUserPriority:
		return enginepb.MinTxnPriority
	case roachpb.NormalUserPriority:
		return 1 // not min nor max
	case roachpb.MaxUserPriority:
		return enginepb.MaxTxnPriority
	default:
		d.Fatalf(t, "unknown priority: %s", priority)
		return 0
	}
}

func scanUserPriority(t *testing.T, d *datadriven.TestData) roachpb.UserPriority {
	priS, ok := dd.ScanArgOpt[string](t, d, "priority")
	if !ok {
		return roachpb.NormalUserPriority
	}
	switch priS {
	case "low":
		return roachpb.MinUserPriority
	case "normal":
		return roachpb.NormalUserPriority
	case "high":
		return roachpb.MaxUserPriority
	default:
		d.Fatalf(t, "unknown priority: %s", priS)
		return 0
	}
}

func scanLockDurability(t *testing.T, d *datadriven.TestData) lock.Durability {
	return getLockDurability(t, d, dd.ScanArg[string](t, d, "dur"))
}

func getLockDurability(t *testing.T, d *datadriven.TestData, durS string) lock.Durability {
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

func scanLockStrength(t *testing.T, d *datadriven.TestData) lock.Strength {
	return concurrency.GetStrength(t, d, dd.ScanArg[string](t, d, "str"))
}

func scanWaitPolicy(t *testing.T, d *datadriven.TestData, required bool) lock.WaitPolicy {
	policy, ok := dd.ScanArgOpt[string](t, d, "wait-policy")
	if !required && !ok {
		return lock.WaitPolicy_Block
	}
	switch policy {
	case "block":
		return lock.WaitPolicy_Block
	case "error":
		return lock.WaitPolicy_Error
	case "skip-locked":
		return lock.WaitPolicy_SkipLocked
	default:
		d.Fatalf(t, "unknown wait policy: %s", policy)
		return 0
	}
}

func scanPoisonPolicy(t *testing.T, d *datadriven.TestData) poison.Policy {
	policy, ok := dd.ScanArgOpt[string](t, d, "poison-policy")
	if !ok {
		return poison.Policy_Error
	}
	switch policy {
	case "error":
		return poison.Policy_Error
	case "wait":
		return poison.Policy_Wait
	default:
		d.Fatalf(t, "unknown poison policy: %s", policy)
		return 0
	}
}

func scanSingleRequest(
	t *testing.T, d *datadriven.TestData, line string, txns map[string]*roachpb.Transaction,
) kvpb.Request {
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
	maybeGetStr := func() lock.Strength {
		s, ok := fields["str"]
		if !ok {
			return lock.None
		}
		return concurrency.GetStrength(t, d, s)
	}
	maybeGetDur := func() lock.Durability {
		s, ok := fields["dur"]
		if !ok {
			return lock.Unreplicated
		}
		return getLockDurability(t, d, s)
	}

	switch cmd {
	case "get":
		var r kvpb.GetRequest
		r.Sequence = maybeGetSeq()
		r.Key = roachpb.Key(mustGetField("key"))
		r.KeyLockingStrength = maybeGetStr()
		r.KeyLockingDurability = maybeGetDur()
		return &r

	case "scan":
		var r kvpb.ScanRequest
		r.Sequence = maybeGetSeq()
		r.Key = roachpb.Key(mustGetField("key"))
		if v, ok := fields["endkey"]; ok {
			r.EndKey = roachpb.Key(v)
		}
		r.KeyLockingStrength = maybeGetStr()
		r.KeyLockingDurability = maybeGetDur()
		return &r

	case "put":
		var r kvpb.PutRequest
		r.Sequence = maybeGetSeq()
		r.Key = roachpb.Key(mustGetField("key"))
		r.Value.SetString(mustGetField("value"))
		return &r

	case "resolve-intent":
		var r kvpb.ResolveIntentRequest
		r.IntentTxn = txns[mustGetField("txn")].TxnMeta
		r.Key = roachpb.Key(mustGetField("key"))
		r.Status = parseTxnStatus(t, d, mustGetField("status"))
		return &r

	case "request-lease":
		var r kvpb.RequestLeaseRequest
		return &r

	case "barrier":
		var r kvpb.BarrierRequest
		r.Key = roachpb.Key(mustGetField("key"))
		if v, ok := fields["endkey"]; ok {
			r.EndKey = roachpb.Key(v)
		}
		return &r

	case "refresh":
		var r kvpb.RefreshRequest
		r.Key = roachpb.Key(mustGetField("key"))
		return &r

	default:
		d.Fatalf(t, "unknown request type: %s", cmd)
		return nil
	}
}

func scanTxnStatus(
	t *testing.T, d *datadriven.TestData,
) (roachpb.TransactionStatus, redact.SafeString) {
	status := parseTxnStatus(t, d, dd.ScanArg[string](t, d, "status"))
	var verb redact.SafeString
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
