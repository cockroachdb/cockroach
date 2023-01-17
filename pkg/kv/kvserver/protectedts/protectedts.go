// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package protectedts exports the interface to the protected timestamp
// subsystem which allows clients to prevent GC of expired data.
package protectedts

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ErrNotExists is returned from Get or Release if the record does
// not exist.
var ErrNotExists = errors.New("protected timestamp record does not exist")

// ErrExists returned from Protect when trying to protect a record
// with an ID which already exists.
var ErrExists = errors.New("protected timestamp record already exists")

// Provider is the central coordinator for the protectedts subsystem.
// It exists to abstract interaction with subsystem.
type Provider interface {
	Manager
	Cache
	Reconciler

	Start(context.Context, *stop.Stopper) error
	Metrics() metric.Struct
}

type Manager interface {
	WithTxn(txn isql.Txn) Storage
}

// Storage provides clients with a mechanism to transactionally protect and
// release protected timestamps for a set of spans.
//
// Clients may provide a txn object which will allow them to write the id
// of this new protection transactionally into their own state.
// It is the caller's responsibility to ensure that a timestamp is ultimately
// released.
type Storage interface {

	// Protect will durably create a protected timestamp, if no error is returned
	// then no data in the specified spans which are live at the specified
	// timestamp can be garbage collected until this Record is released.
	//
	// Protect may succeed and yet data may be or already have been garbage
	// collected in the spans specified by the Record. However, the protected
	// timestamp subsystem guarantees that if all possible zone configs which
	// could have applied have GC TTLs which would not have allowed data at
	// the timestamp which the passed Txn commits to be GC'd then that
	// data will not be GC'd until this *Record is released.
	//
	// An error will be returned if the ID of the provided record already exists
	// so callers should be sure to generate new IDs when creating records.
	Protect(context.Context, *ptpb.Record) error

	// GetRecord retreives the record with the specified UUID as well as the MVCC
	// timestamp at which it was written. If no corresponding record exists
	// ErrNotExists is returned.
	//
	// GetRecord exists to work in coordination with verification. In order
	// to use Verifier.Verify a client must provide both the timestamp which
	// should be protected as well as the timestamp at which the Record providing
	// that protection is known to be alive. The ReadTimestamp of the Txn used in
	// this method can be used to provide such a timestamp.
	GetRecord(context.Context, uuid.UUID) (*ptpb.Record, error)

	// MarkVerified will mark a protected timestamp as verified.
	//
	// This method is generally used by an implementation of Verifier.
	MarkVerified(context.Context, uuid.UUID) error

	// Release allows spans which were previously protected to now be garbage
	// collected.
	//
	// If the specified UUID does not exist ErrNotFound is returned but the
	// passed txn remains safe for future use.
	Release(context.Context, uuid.UUID) error

	// GetMetadata retrieves the metadata with the provided Txn.
	GetMetadata(context.Context) (ptpb.Metadata, error)

	// GetState retrieves the entire state of protectedts.Storage with the
	// provided Txn.
	GetState(context.Context) (ptpb.State, error)

	// UpdateTimestamp updates the timestamp protected by the record with the
	// specified UUID.
	UpdateTimestamp(ctx context.Context, id uuid.UUID, timestamp hlc.Timestamp) error
}

// Iterator iterates records in a cache until wantMore is false or all Records
// in the requested range have been seen.
type Iterator func(*ptpb.Record) (wantMore bool)

// Cache is used in the storage package to determine a safe timestamp for
// garbage collection of expired data. A storage.Replica can remove data when
// it has a proof from the Cache that there was no Record providing
// protection. For example, a Replica which determines that it is not protected
// by any Records at a given asOf can move its GC threshold up to that
// timestamp less its GC TTL.
type Cache interface {
	spanconfig.ProtectedTSReader

	// Iterate examines the records with spans which overlap with [from, to).
	// Nil values for from or to are equivalent to Key{}. The order of records
	// between independent calls to Iterate is not defined; the order of records
	// may differ even for the same key range that occurs at the same timestamp.
	// The Records passed to the iterator are safe to be retained but must not be
	// modified.
	Iterate(_ context.Context, from, to roachpb.Key, it Iterator) (asOf hlc.Timestamp)

	// QueryRecord determines whether a Record with the provided ID exists in
	// the Cache state and returns the timestamp corresponding to that state.
	QueryRecord(_ context.Context, id uuid.UUID) (exists bool, asOf hlc.Timestamp)

	// Refresh forces the cache to update to at least asOf.
	Refresh(_ context.Context, asOf hlc.Timestamp) error
}

// Reconciler provides a mechanism to reconcile protected timestamp records with
// external state.
type Reconciler interface {
	// StartReconciler will start the reconciliation where each record's status is
	// determined using the record's meta type and meta in conjunction with the
	// configured StatusFunc.
	//
	// StartReconciler can be called more than once since the work it does is
	// idempotent.
	StartReconciler(ctx context.Context, stopper *stop.Stopper) error
}

// EmptyCache returns a Cache which always returns the current time and no
// records. This is often useful in testing where you want a cache which
// holds nothing and is always up-to-date.
func EmptyCache(c *hlc.Clock) Cache {
	return (*emptyCache)(c)
}

type emptyCache hlc.Clock

func (c *emptyCache) Iterate(
	_ context.Context, from, to roachpb.Key, it Iterator,
) (asOf hlc.Timestamp) {
	return (*hlc.Clock)(c).Now()
}

func (c *emptyCache) QueryRecord(
	_ context.Context, id uuid.UUID,
) (exists bool, asOf hlc.Timestamp) {
	return false, (*hlc.Clock)(c).Now()
}

func (c *emptyCache) Refresh(_ context.Context, asOf hlc.Timestamp) error {
	return nil
}

func (c *emptyCache) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, err error) {
	return protectionTimestamps, (*hlc.Clock)(c).Now(), nil
}
