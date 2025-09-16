// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package lease

import "github.com/cockroachdb/cockroach/pkg/util/hlc"

// ReadTimestamp is a wrapper for the transaction timestamp
// that allows us to support locked timestamps for leasing.
type ReadTimestamp interface {
	// GetTimestamp returns the timestamp at which descriptors will
	// be read for this transaction, which will guarantee a transactionally
	// consistent view between descriptors read.
	GetTimestamp() hlc.Timestamp
	// GetBaseTimestamp returns the original read timestamp for
	// the transaction.
	GetBaseTimestamp() hlc.Timestamp
}

// TimestampToReadTimestamp converts a hlc.Timestamp into a ReadTimestamp,
// without any timestamp locking applied.
func TimestampToReadTimestamp(timestamp hlc.Timestamp) ReadTimestamp {
	return LeaseTimestamp{
		ReadTimestamp: timestamp,
	}
}

// LeaseTimestamp implements a locked read timestamp.
type LeaseTimestamp struct {
	ReadTimestamp  hlc.Timestamp
	LeaseTimestamp hlc.Timestamp
}

// GetTimestamp implements ReadTimestamp.
func (ls LeaseTimestamp) GetTimestamp() hlc.Timestamp {
	if !ls.LeaseTimestamp.IsEmpty() {
		return ls.LeaseTimestamp
	}
	return ls.ReadTimestamp
}

// GetBaseTimestamp implements ReadTimestamp.
func (ls LeaseTimestamp) GetBaseTimestamp() hlc.Timestamp {
	return ls.ReadTimestamp
}

var _ ReadTimestamp = LeaseTimestamp{}
