// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package lock provides type definitions for locking-related concepts used by
// concurrency control in the key-value layer.
package lock_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestWaiterSafeFormat(t *testing.T) {
	ts := hlc.Timestamp{Logical: 1}
	txnMeta := &enginepb.TxnMeta{
		Key:               roachpb.Key("foo"),
		ID:                uuid.NamespaceDNS,
		Epoch:             2,
		WriteTimestamp:    ts,
		MinTimestamp:      ts,
		Priority:          957356782,
		Sequence:          123,
		CoordinatorNodeID: 3,
	}
	waiter := &lock.Waiter{
		WaitingTxn:   txnMeta,
		ActiveWaiter: true,
		Strength:     lock.Exclusive,
		WaitDuration: 135 * time.Second,
	}

	require.EqualValues(t,
		"waiting_txn:6ba7b810 active_waiter:true strength:Exclusive wait_duration:2m15s",
		redact.Sprint(waiter).StripMarkers())
	require.EqualValues(t,
		"waiting_txn:6ba7b810-9dad-11d1-80b4-00c04fd430c8 active_waiter:true strength:Exclusive wait_duration:2m15s",
		redact.Sprintf("%+v", waiter).StripMarkers())
	require.EqualValues(t,
		"waiting_txn:6ba7b810 active_waiter:true strength:Exclusive wait_duration:2m15s",
		redact.Sprint(waiter).Redact())
	require.EqualValues(t,
		"waiting_txn:6ba7b810-9dad-11d1-80b4-00c04fd430c8 active_waiter:true strength:Exclusive wait_duration:2m15s",
		redact.Sprintf("%+v", waiter).Redact())

	nonTxnWaiter := &lock.Waiter{
		WaitingTxn:   nil,
		ActiveWaiter: false,
		Strength:     lock.None,
		WaitDuration: 17 * time.Millisecond,
	}

	require.EqualValues(t,
		"waiting_txn:<nil> active_waiter:false strength:None wait_duration:17ms",
		redact.Sprint(nonTxnWaiter).StripMarkers())
	require.EqualValues(t,
		"waiting_txn:<nil> active_waiter:false strength:None wait_duration:17ms",
		redact.Sprintf("%+v", nonTxnWaiter).StripMarkers())
	require.EqualValues(t,
		"waiting_txn:<nil> active_waiter:false strength:None wait_duration:17ms",
		redact.Sprint(nonTxnWaiter).Redact())
	require.EqualValues(t,
		"waiting_txn:<nil> active_waiter:false strength:None wait_duration:17ms",
		redact.Sprintf("%+v", nonTxnWaiter).Redact())
}
