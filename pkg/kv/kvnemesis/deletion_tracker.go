// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvnemesis

import (
	"math"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// DeletionTracker helps map tombstones to the operations that wrote them.
// Before operations are sent to KV, they undergo Inject(). Inside of KV,
// requests are intercepted using Extract() to determine the execution timestamp
// for the deletion.
//
// This allows identifying tombstones in MVCC history with their creating
// operation, in effect salvaging kvnemesis' approach of "unique writes".
type DeletionTracker struct {
	syncutil.Mutex
	m map[DelSeq]hlc.Timestamp
}

// DelSeq is the type of sequence numbers used by DeletionTracker.
type DelSeq uint32

func (d *DeletionTracker) Extract(header roachpb.Header) (DelSeq, bool) {
	n := int64(header.LockTimeout)
	// Check whether high 32 bits match math.MaxInt64, which identifies a
	// LockTimeout that is being slightly abused as a carrier for the sequence
	// number.
	if (n>>32)<<32 != (math.MaxInt64>>32)<<32 {
		return 0, false
	}
	i := (n << 32) >> 32
	seq := DelSeq(*(*uint32)(unsafe.Pointer(&i)))
	// Take the lowest 32 bits and interpret them as a uint32.
	if d.m == nil {
		d.m = map[DelSeq]hlc.Timestamp{}
	}
	d.m[seq] = header.Timestamp
	return seq, true
}

func (d *DeletionTracker) Inject(header *roachpb.Header, seq DelSeq) {
	var n int64
	// Use 32 uppermost bits off MaxInt64.
	n += (math.MaxInt64 >> 32) << 32
	// Interpret seq as an int32, and add it (thus
	// making it the lower 32 bits)
	n += int64(*(*int32)(unsafe.Pointer(&seq)))
	header.LockTimeout = time.Duration(n)
}

func (d *DeletionTracker) Lookup(seq DelSeq) (hlc.Timestamp, bool) {
	d.Lock()
	defer d.Unlock()
	ts, ok := d.m[seq]
	return ts, ok
}
