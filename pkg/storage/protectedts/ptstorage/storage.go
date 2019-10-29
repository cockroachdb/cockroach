// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Provider allows clients to interact with protected timestamps.
type Provider struct {
	settings *cluster.Settings
}

// NewProvider creates a new Provider.
func NewProvider(settings *cluster.Settings) *Provider {
	return &Provider{settings: settings}
}

// Protect will durably create a new protection Record.
//
// Protect may succeed and yet data may later be or already have been garbage
// collected in the spans specified by the Record. However, the protected
// timestamp subsystem guarantees that, if all possible zone configs which
// could have applied have GC TTLs which would not have allowed any data to
// have been garbage collected at the timestamp at which the Txn committed
// then the data is guaranteed to be protected.
//
// An error will be returned if the ID of the provided record already exists
// so callers should be sure to generate new IDs when creating records.
//
// TODO(ajwerner): provide a mechanism to validate that a protected timestamp
// is indeed protected. In general it will use the same mechanism as the
// EnsureProtected will for ImportRequest.
//
// TODO(ajwerner): decide if the spans the record need to be fully merged.
func (p *Provider) Protect(ctx context.Context, txn *client.Txn, r *ptpb.Record) error {
	if err := validateRecord(r); err != nil {
		return err
	}

	// If the caller did not provide us with a transaction perform this operation
	// in its own transaction.
	return protect(ctx, p, txn, r)
}

// GetRecord retreives the record at with the specified UUID as well as the MVCC
// timestamp at which it was written.	If no corresponding record exists
// ErrNotFound is returned.
//
// GetRecord exists to work in coordination with the EnsureProtected field of
// import requests. In order to use EnsureProtected a client must provide
// both the timestamp which should be protected as well as the timestamp
// at which the Record providing that protection was created.
func (p *Provider) GetRecord(
	ctx context.Context, txn *client.Txn, id uuid.UUID,
) (_ *ptpb.Record, createdAt hlc.Timestamp, _ error) {
	return getRecord(ctx, p, txn, id)
}

// Release allows spans which were previously protected to now be garbage
// collected.
//
// If the specified UUID does not exist ErrNotFound is returned but the
// passed txn remains safe for future use.
func (p *Provider) Release(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	return release(ctx, p, txn, id)
}

// GetMetadata retrieves the subsystem metadata.
func (p *Provider) GetMetadata(ctx context.Context, txn *client.Txn) (ptpb.Metadata, error) {
	return getMetadata(ctx, txn)
}

// GetState retrieves the subsystem state.
func (p *Provider) GetState(ctx context.Context, txn *client.Txn) (ptpb.State, error) {
	return getState(ctx, p, txn)
}

func protect(ctx context.Context, p *Provider, txn *client.Txn, r *ptpb.Record) error {
	// TODO(ajwerner): This would be an ideal use case for SELECT FOR UPDATE-style
	// read locking. In the absense of read locks, concurrent attempts to protect
	// timestamps are likely to encounter serializable restarts.
	md, mdVal, err := getMetadataVal(ctx, txn)
	if err != nil {
		return err
	}
	newMD, err := addRecordToMetadata(md, r, &p.settings.SV)
	if err != nil {
		return err
	}
	if err := txn.CPut(ctx, keys.ProtectedTimestampMetadata, &newMD, mdVal); err != nil {
		return errors.Wrap(err, "failed to update protectedts metadata")
	}
	if err := txn.CPut(ctx, makeRecordKey(r.ID), r, nil); err != nil {
		return errors.Wrapf(err, "failed to create protectedts record %v", r)
	}
	return nil
}

func getRecord(
	ctx context.Context, p *Provider, txn *client.Txn, id uuid.UUID,
) (r *ptpb.Record, asOf hlc.Timestamp, err error) {
	kv, err := txn.Get(ctx, makeRecordKey(id))
	if err != nil {
		return nil, hlc.Timestamp{}, err
	}
	if kv.Value == nil {
		return nil, hlc.Timestamp{}, protectedts.ErrNotFound
	}
	r = new(ptpb.Record)
	if err := kv.Value.GetProto(r); err != nil {
		return nil, hlc.Timestamp{},
			errors.NewAssertionErrorWithWrappedErrf(err,
				"malformed protected timestamp record for id %v", id)
	}
	return r, kv.Value.Timestamp, nil
}

func release(ctx context.Context, p *Provider, txn *client.Txn, id uuid.UUID) error {
	r, _, err := getRecord(ctx, p, txn, id)
	if err != nil {
		return err
	}
	md, mdVal, err := getMetadataVal(ctx, txn)
	if err != nil {
		return err
	}
	md.Version++
	md.NumRecords--
	md.NumSpans -= uint64(len(r.Spans))
	if err := txn.CPut(ctx, keys.ProtectedTimestampMetadata, &md, mdVal); err != nil {
		return errors.Wrap(err, "failed to update protectedts metadata")
	}
	return txn.Del(ctx, makeRecordKey(id))
}

func getMetadata(ctx context.Context, txn *client.Txn) (ptpb.Metadata, error) {
	md, _, err := getMetadataVal(ctx, txn)
	return md, err
}

func getMetadataVal(
	ctx context.Context, txn *client.Txn,
) (md ptpb.Metadata, val *roachpb.Value, err error) {
	// TODO(ajwerner): do we put this into a migration and ensure it exists?
	// probably.
	kv, err := txn.Get(ctx, keys.ProtectedTimestampMetadata)
	if err != nil {
		return md, nil, errors.NewAssertionErrorWithWrappedErrf(err, "protectedts metadata key did not exist")
	}
	if kv.Value == nil {
		return md, nil, nil
	}
	if err := kv.Value.GetProto(&md); err != nil {
		return md, nil, errors.NewAssertionErrorWithWrappedErrf(err, "protectedts metadata value was malformed")
	}
	return md, kv.Value, nil
}

var protectedTimestampTablePrefix = keys.MakeTablePrefix(keys.ProtectedTimestampRecordsTableID)
var protectedTimestampTableEnd = keys.MakeTablePrefix(keys.ProtectedTimestampRecordsTableID + 1)

func addRecordToMetadata(
	md ptpb.Metadata, r *ptpb.Record, sv *settings.Values,
) (ptpb.Metadata, error) {
	updated := md
	updated.Version++
	updated.NumSpans += uint64(len(r.Spans))
	updated.NumRecords++
	if maxSpans := uint64(protectedts.MaxSpans.Get(sv)); maxSpans > 0 && updated.NumSpans > maxSpans {
		return ptpb.Metadata{},
			errors.Errorf("protectedts: maximum span limit of %d exceeded: before %d, after %d",
				maxSpans, md.NumSpans, updated.NumSpans)
	}
	if maxRecords := uint64(protectedts.MaxRecords.Get(sv)); maxRecords > 0 && updated.NumRecords > maxRecords {
		return ptpb.Metadata{},
			errors.Errorf("protectedts: maximum span limit of %d exceeded: before %d, after %d",
				maxRecords, md.NumRecords, updated.NumRecords)
	}
	return updated, nil
}

func makeRecordKey(id uuid.UUID) roachpb.Key {
	k := keys.MakeTablePrefix(uint32(keys.ProtectedTimestampRecordsTableID))
	return encoding.EncodeBytesAscending(k, id[:])
}

func validateRecord(r *ptpb.Record) error {
	if r.Timestamp == (hlc.Timestamp{}) {
		return errors.Errorf("cannot protect an empty timestamp")
	}
	if len(r.Spans) == 0 {
		return errors.Errorf("cannot protect empty set of spans")
	}
	return nil
}

func getState(ctx context.Context, p *Provider, txn *client.Txn) (ptpb.State, error) {
	md, err := getMetadata(ctx, txn)
	if err != nil {
		return ptpb.State{}, err
	}
	kvs, err := txn.Scan(ctx, protectedTimestampTablePrefix, protectedTimestampTableEnd, 0)
	if err != nil {
		return ptpb.State{}, errors.Wrap(err, "failed to retrieve protected timestamps")
	}
	var records []ptpb.Record
	for i := range kvs {
		if records == nil { // lazily initialize records
			records = make([]ptpb.Record, len(kvs))
		}
		if err := kvs[i].ValueProto(&records[i]); err != nil {
			// TODO(ajwerner): decode the key and include it in this error.
			// Ideally we could just move on but all of our correctness would be thrown out of the window.
			return ptpb.State{}, errors.NewAssertionErrorWithWrappedErrf(err, "failed to decode protected timestamp record at key %s",
				kvs[i].Key)
		}
	}
	return ptpb.State{
		Metadata: md,
		Records:  records,
	}, nil
}
