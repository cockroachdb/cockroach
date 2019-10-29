// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package protectedts offers abstractions to prevent spans from having old
// MVCC values live at a given timestamp from being GC'd.
package protectedts

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// ErrNotFound is returned from Get or Release if the record does
// not exist.
var ErrNotFound = errors.New("protected timestamp record not found")

// Provider is the central coordinator for the protectedts subsystem.
// It exists to abstract interaction with subsystem.
type Provider interface {
	Storage
	Tracker
	Start(context.Context, *stop.Stopper) error
}

// Storage provides clients with a mechanism to transactionally protect and
// release protected timestamps for a set of spans.
//
// Clients may provide a txn object which will allow them to write the id
// of this new protection transactionally into their own state. If the caller
// provides a txn object then it is the caller's responsibility to call
// CleanupOnError if an error is returned. If an error is returned and the
// transaction is subsequently committed, the resulting state of the system
// is undefined.
//
// It is the caller's responsibility to ensure that a timestamp is ultimately
// released.
type Storage interface {

	// Protect will durably create a protected timestamp, if no error is returned
	// then no data in the specified spans at which are live at the specified
	// timestamp can be garbage collected until the this ProtectedTimestamp is
	// released.
	//
	// Protect may succeed and yet data may be or already have been garbage
	// collected in the spans specified by the Record. However, the protected
	// timestamp subsystem guarantees that if all possible zone configs which
	// could have applied have GC TTLs which would not have allowed data at
	// the timestamp which the passed Txn commits have been GC'd then that
	// data will not be GC'd until this *Record is released.
	//
	// An error will be returned if the ID of the provided record already exists
	// so callers should be sure to generate new IDs when creating records.
	Protect(context.Context, *client.Txn, *ptpb.Record) error

	// Get retreives the record at with the specified UUID as well as the MVCC
	// timestamp at which it was written.	If no corresponding record exists
	// ErrNotFound is returned.
	//
	// Get exists to work in coordination with the EnsureProtected field of
	// import requests. In order to use EnsureProtected a client must provide
	// both the timestamp which should be protected as well as the timestamp
	// at which the Record providing that protection was created.
	GetRecord(
		context.Context, *client.Txn, uuid.UUID,
	) (_ *ptpb.Record, createdAt hlc.Timestamp, _ error)

	// Release allows spans which were previously protected to now be garbage
	// collected.
	//
	// If the specified UUID does not exist ErrNotFound is returned but the
	// passed txn remains safe for future use.
	Release(context.Context, *client.Txn, uuid.UUID) error

	// GetMetadata retreives the metadata with the provided Txn.
	GetMetadata(context.Context, *client.Txn) (ptpb.Metadata, error)

	// GetState retreives the entire state of storage with the provided Txn.
	GetState(context.Context, *client.Txn) (ptpb.State, error)
}

// Tracker will be used in the storage package to determine a safe
// timestamp for garbage collection.
type Tracker interface {

	// ProtectedBy calls the passed function for each record which overlaps the
	// pass Span. The return value is the MVCC timestamp at which this set of
	// records is known to be valid.
	ProtectedBy(context.Context, roachpb.Span, func(*ptpb.Record)) (asOf hlc.Timestamp)

	// TODO(ajwerner): consider providing a method to force refreshing the
	// state as of the current time. Clients may want to call this when
	// zone configs are changing.
}

// ClockTracker returns a tracker which always returns the current time and no
// records. This is often useful in testing where you want a tracker which
// protects nothing and is always up-to-date.
func ClockTracker(c *hlc.Clock) Tracker {
	return (*clockTracker)(c)
}

type clockTracker hlc.Clock

func (t *clockTracker) ProtectedBy(
	context.Context, roachpb.Span, func(*ptpb.Record),
) (asOf hlc.Timestamp) {
	return (*hlc.Clock)(t).Now()
}
