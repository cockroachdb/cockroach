// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workloadid

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/ctxutil"
)

// This package defines CRDB-wide workload identifiers that we can
// define and use to attribute observability data.
//
// We will prefer using uint64 workload identifiers because those are
// cheaper to move around the system and can help us avoid allocation
// in hot paths.
//
// String-based identifiers are useful when we want to display the
// identifier to a human, or to label go profiles and execution traces,
// which only accept string values for tags.
//
// Currently, this definition is quite simple consisting only of a
// preset list of static identifiers below that can be used numerically
// or using static strings. Statement Fingerprint IDs are the only
// example here of a dynamic ID that can have high cardinality.
//
// It is expected that future work will expand the structure and
// expressivity of the workloadID. No ID stability is guaranteed.
// While we currently do persist statement fingerprint IDs in the SQL
// Activity tables, we advise users of these values to AVOID PERSISTING
// WORKLOAD IDs until they are stable.

const ProfileTag = "workload.id"

type WorkloadID uint64

var workloadIDKey = ctxutil.RegisterFastValueKey()

// ContextWithWorkloadID returns a context annotated with the provided
// workloadID. Use WorkloadIDFromContext(ctx) to retrieve it from the
// ctx later.
func ContextWithWorkloadID(ctx context.Context, workloadID WorkloadID) context.Context {
	return ctxutil.WithFastValue(ctx, workloadIDKey, workloadID)
}

// WorkloadIDFromContext retrieves the server identity put in the
// context by ContextWithWorkloadID.
func WorkloadIDFromContext(ctx context.Context) WorkloadID {
	r := ctxutil.FastValue(ctx, workloadIDKey)
	if r == nil {
		return 0
	}
	return r.(WorkloadID)
}

// The IDs and Names below are currently not persisted anywhere and
// are subject to change.

const (
	WORKLOAD_ID_UNKNOWN WorkloadID = iota
	WORKLOAD_ID_LDR
	WORKLOAD_ID_RAFT
	WORKLOAD_ID_STORELIVENESS
	WORKLOAD_ID_RPC_HEARTBEAT
	WORKLOAD_ID_TXN_PUSH
	WORKLOAD_ID_RANGEFEED
)

const (
	WORKLOAD_NAME_UNKNOWN             = "UNKNOWN"
	WORKLOAD_NAME_LDR                 = "LDR"
	WORKLOAD_NAME_RAFT                = "RAFT"
	WORKLOAD_NAME_STORELIVENESS       = "STORELIVENESS"
	WORKLOAD_NAME_STMT_FINGERPRINT_ID = "STMT_FINGERPRINT_ID"
	WORKLOAD_NAME_RPC_HEARTBEAT       = "RPC_HEARTBEAT"
	WORKLOAD_NAME_TXN_PUSH            = "TXN_PUSH"
	WORKLOAD_NAME_RANGEFEED           = "RANGEFEED"
)

var workloadIDToName = map[WorkloadID]string{
	WORKLOAD_ID_UNKNOWN:       WORKLOAD_NAME_UNKNOWN,
	WORKLOAD_ID_LDR:           WORKLOAD_NAME_LDR,
	WORKLOAD_ID_RAFT:          WORKLOAD_NAME_RAFT,
	WORKLOAD_ID_STORELIVENESS: WORKLOAD_NAME_STORELIVENESS,
	WORKLOAD_ID_RPC_HEARTBEAT: WORKLOAD_NAME_RPC_HEARTBEAT,
	WORKLOAD_ID_TXN_PUSH:      WORKLOAD_NAME_TXN_PUSH,
	WORKLOAD_ID_RANGEFEED:     WORKLOAD_NAME_RANGEFEED,
}

func WorkloadIDName(wID WorkloadID) string {
	name, ok := workloadIDToName[wID]
	if ok {
		return name
	}
	// We assume that any workloadID that's not zero and not in the table
	// above is a Statement fingerprint for now. Future work will encode
	// the type into the ID so we don't have to guess.
	return WORKLOAD_NAME_STMT_FINGERPRINT_ID
}
