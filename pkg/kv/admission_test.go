// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kv

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestAdmissionHeader(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	txn := roachpb.MakeTransaction("test", []byte("key"), 0, 0, hlc.Timestamp{WallTime: 10},
		0, 0, admissionpb.UserHighPri, false /* omitInRangefeeds */)
	ahHighPri := AdmissionHeaderForLockUpdateForTxn(&txn)
	require.Equal(t, kvpb.AdmissionHeader{
		// Priority bumped from UserHighPri to LockingUserHighPri to give priority to
		// intent resolution.
		Priority:   int32(admissionpb.LockingUserHighPri),
		CreateTime: 10,
		Source:     kvpb.AdmissionHeader_ROOT_KV,
	}, ahHighPri)

	ahBypass := AdmissionHeaderForBypass(ahHighPri)
	require.Equal(t, kvpb.AdmissionHeader_OTHER, ahBypass.Source)

	require.Equal(t, ahBypass, MergeAdmissionHeaderForBatch(ahHighPri, ahBypass))

	txnAt20 := txn
	txnAt20.MinTimestamp = hlc.Timestamp{WallTime: 20}
	ahHighPriAt20 := AdmissionHeaderForLockUpdateForTxn(&txnAt20)
	require.Equal(t, kvpb.AdmissionHeader{
		Priority:   int32(admissionpb.LockingUserHighPri),
		CreateTime: 20,
		Source:     kvpb.AdmissionHeader_ROOT_KV,
	}, ahHighPriAt20)
	require.Equal(t, ahHighPri, MergeAdmissionHeaderForBatch(ahHighPriAt20, ahHighPri))

	txnAt5 := txn
	txnAt5.MinTimestamp = hlc.Timestamp{WallTime: 5}
	txnAt5.AdmissionPriority = int32(admissionpb.LowPri)
	ahLowPriAt5 := AdmissionHeaderForLockUpdateForTxn(&txnAt5)
	require.Equal(t, kvpb.AdmissionHeader{
		// Priority bumped from LowPri to LockingNormalPri to give priority to
		// intent resolution.
		Priority:   int32(admissionpb.LockingNormalPri),
		CreateTime: 5,
		Source:     kvpb.AdmissionHeader_ROOT_KV,
	}, ahLowPriAt5)
	require.Equal(t, ahHighPri, MergeAdmissionHeaderForBatch(ahHighPri, ahLowPriAt5))
}
