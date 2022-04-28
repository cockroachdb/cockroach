// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// TxnFingerprintIDBufferCapacity is the cluster setting that controls the
// capacity of the txn fingerprint ID circular buffer.
var TxnFingerprintIDBufferCapacity = settings.RegisterIntSetting(
	settings.TenantWritable,
	"sql.txn_fingerprint_id_buffer.capacity",
	"the maximum number of txn fingerprint IDs stored",
	100,
).WithPublic()

// TxnFingerprintIDBuffer is a thread-safe circular buffer tracking transaction
// fingerprint IDs at the session level.
type TxnFingerprintIDBuffer struct {
	st *cluster.Settings

	mu struct {
		syncutil.RWMutex
		data          []roachpb.TransactionFingerprintID
		acc           mon.BoundAccount
		size          int
		capacity      int64
		readPosition  int
		writePosition int
	}

	mon *mon.BytesMonitor
}

// NewTxnFingerprintIDBuffer returns a new TxnFingerprintIDBuffer.
func NewTxnFingerprintIDBuffer(
	st *cluster.Settings, parentMon *mon.BytesMonitor,
) *TxnFingerprintIDBuffer {
	b := &TxnFingerprintIDBuffer{st: st}
	b.initializeBufferLocked(TxnFingerprintIDBufferCapacity.Get(&st.SV))

	monitor := mon.NewMonitorInheritWithLimit("txn-fingerprint-id-buffer", 0 /* limit */, parentMon)
	b.mu.acc = monitor.MakeBoundAccount()
	b.mon = monitor
	b.mon.Start(context.Background(), parentMon, mon.BoundAccount{})

	return b
}

// initializeBufferLocked initializes a new buffer with the given capacity.
func (b *TxnFingerprintIDBuffer) initializeBufferLocked(capacity int64) {
	b.mu.capacity = capacity
	b.mu.data = make([]roachpb.TransactionFingerprintID, capacity)
	for i := range b.mu.data {
		b.mu.data[i] = roachpb.InvalidTransactionFingerprintID
	}
}

// Enqueue adds a TxnFingerprintID to the circular buffer.
func (b *TxnFingerprintIDBuffer) Enqueue(value roachpb.TransactionFingerprintID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.checkForCapacityLocked()
	if b.mu.size >= int(b.mu.capacity) {
		b.dequeueLocked()
	}

	if b.mu.writePosition >= int(b.mu.capacity) {
		b.mu.writePosition = 0
	}

	size := value.Size()
	err := b.mu.acc.Grow(context.Background(), size)
	if err != nil {
		return err
	}

	b.mu.data[b.mu.writePosition] = value
	b.mu.writePosition++
	b.mu.size++

	return nil
}

// checkForCapacityLocked checks if the TxnFingerprintIDBufferCapacity cluster
// setting has been updated.
func (b *TxnFingerprintIDBuffer) checkForCapacityLocked() {
	capacityClusterSetting := TxnFingerprintIDBufferCapacity.Get(&b.st.SV)
	if b.mu.capacity != capacityClusterSetting {
		b.updateCapacityLocked(capacityClusterSetting)
	}
}

// updateCapacityLocked updates the capacity of the circular buffer and moves
// the data into a new slice with that capacity.
func (b *TxnFingerprintIDBuffer) updateCapacityLocked(newCapacity int64) {
	newData := make([]roachpb.TransactionFingerprintID, newCapacity)
	oldData := b.mu.data

	ptr := 0
	b.mu.size = 0
	b.initializeBufferLocked(newCapacity)
	for _, txnFingerprintID := range oldData {
		if txnFingerprintID != roachpb.InvalidTransactionFingerprintID {
			if ptr >= int(newCapacity) {
				break
			}
			newData[ptr] = txnFingerprintID
			b.mu.size++
			ptr++
		}
	}

	b.mu.data = newData
	b.mu.readPosition = 0
	b.mu.writePosition = b.mu.size
}

// GetAllTxnFingerprintIDs returns a slice of all TxnFingerprintIDs in the
// circular buffer.
func (b *TxnFingerprintIDBuffer) GetAllTxnFingerprintIDs() []roachpb.TransactionFingerprintID {
	b.mu.Lock()
	defer b.mu.Unlock()

	var txnFingerprintIDs []roachpb.TransactionFingerprintID
	if b.mu.data[b.mu.readPosition] != roachpb.InvalidTransactionFingerprintID {
		txnFingerprintIDs = append(txnFingerprintIDs, b.mu.data[b.mu.readPosition])
	}

	ptr := b.mu.readPosition + 1
	for ptr != b.mu.readPosition {
		if b.mu.data[ptr] != roachpb.InvalidTransactionFingerprintID {
			txnFingerprintIDs = append(txnFingerprintIDs, b.mu.data[ptr])
		}
		ptr = (ptr + 1) % int(b.mu.capacity)
	}

	return txnFingerprintIDs
}

// dequeue returns the oldest transaction fingerprint ID
func (b *TxnFingerprintIDBuffer) dequeue() roachpb.TransactionFingerprintID {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.dequeueLocked()
}

func (b *TxnFingerprintIDBuffer) dequeueLocked() roachpb.TransactionFingerprintID {
	txnFingerprintID := b.mu.data[b.mu.readPosition]
	b.mu.data[b.mu.readPosition] = roachpb.InvalidTransactionFingerprintID

	size := txnFingerprintID.Size()
	b.mu.acc.Shrink(context.Background(), size)
	b.mu.size--
	b.mu.readPosition++

	return txnFingerprintID
}

func (b *TxnFingerprintIDBuffer) size() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.mu.size
}
