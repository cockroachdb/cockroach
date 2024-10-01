// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// AdmissionHeaderForLockUpdateForTxn constructs the admission header for the
// given LockUpdate being done by txn.
//
// TODO(sumeer): We don't have a way of injecting TenantID, since the TenantID
// is decided in rpc.kvAuth. The RPC will be sent from one storage server to
// another, so it will run with the SystemTenantID, which is not desirable for
// multi-tenant CockroachDB. We should be deciding the TenantID based on which
// tenant the range belongs to.
func AdmissionHeaderForLockUpdateForTxn(txn *roachpb.Transaction) kvpb.AdmissionHeader {
	return kvpb.AdmissionHeader{
		Priority: int32(admissionpb.AdjustedPriorityWhenHoldingLocks(admissionpb.WorkPriority(
			txn.AdmissionPriority))),
		CreateTime: txn.MinTimestamp.WallTime,
		Source:     kvpb.AdmissionHeader_ROOT_KV,
	}
}

// AdmissionHeaderForBypass alters the admission header so that admission
// control can be bypassed.
func AdmissionHeaderForBypass(h kvpb.AdmissionHeader) kvpb.AdmissionHeader {
	h.Source = kvpb.AdmissionHeader_OTHER
	return h
}

// MergeAdmissionHeaderForBatch merges admission headers to pick the highest
// priority and oldest CreateTime.
//
// TODO(sumeer): the oldest CreateTime is based on the assumption of FIFO
// ordering for the same priority, and does not play well with epoch-LIFO. But
// no one is using epoch-LIFO in production, so this is ok for now.
func MergeAdmissionHeaderForBatch(
	bh kvpb.AdmissionHeader, h kvpb.AdmissionHeader,
) kvpb.AdmissionHeader {
	// kvpb.AdmissionHeader_OTHER bypasses admission, so prefer that.
	if bh.Source == kvpb.AdmissionHeader_OTHER {
		return bh
	}
	if h.Source == kvpb.AdmissionHeader_OTHER {
		return h
	}
	if h.Priority > bh.Priority {
		return h
	} else if h.Priority < bh.Priority {
		return bh
	}
	// h.Priority == bh.Priority
	if bh.CreateTime > h.CreateTime {
		return h
	}
	return bh
}
