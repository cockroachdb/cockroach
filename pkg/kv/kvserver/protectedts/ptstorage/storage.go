// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ptstorage implements protectedts.Storage.
package ptstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Consider memory accounting.
// TODO(ajwerner): Add metrics.

// TODO(ajwerner): Provide some sort of reconciliation of metadata in the face
// of corruption. Not clear how or why such corruption might happen but if it
// does it might be nice to have an escape hatch. Perhaps another interface
// method which scans the records and updates the counts in the meta row
// accordingly.

// TODO(ajwerner): Hook into the alerts infrastructure and metrics to provide
// visibility into corruption when it is detected.

// storage interacts with the durable state of the protectedts subsystem.
type storage struct {
	settings *cluster.Settings
	ex       sqlutil.InternalExecutor
}

var _ protectedts.Storage = (*storage)(nil)

// New creates a new Storage.
func New(settings *cluster.Settings, ex sqlutil.InternalExecutor) protectedts.Storage {
	return &storage{settings: settings, ex: ex}
}

var errNoTxn = errors.New("must provide a non-nil transaction")

func (p *storage) Protect(ctx context.Context, txn *kv.Txn, r *ptpb.Record) error {
	if err := validateRecordForProtect(r); err != nil {
		return err
	}
	if txn == nil {
		return errNoTxn
	}
	encodedSpans, err := protoutil.Marshal(&Spans{Spans: r.Spans})
	if err != nil { // how can this possibly fail?
		return errors.Wrap(err, "failed to marshal spans")
	}
	meta := r.Meta
	if meta == nil {
		// v20.1 crashes in rowToRecord and storage.Release if it finds a NULL
		// value in system.protected_ts_records.meta. v20.2 and above handle
		// this correctly, but we need to maintain mixed version compatibility
		// for at least one release.
		// TODO(nvanbenschoten): remove this for v21.1.
		meta = []byte{}
	}
	s := makeSettings(p.settings)
	it, err := p.ex.QueryIteratorEx(ctx, "protectedts-protect", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		protectQuery,
		s.maxSpans, s.maxBytes, len(r.Spans),
		r.ID.GetBytesMut(), r.Timestamp.AsOfSystemTime(),
		r.MetaType, meta,
		len(r.Spans), encodedSpans)
	if err != nil {
		return errors.Wrapf(err, "failed to write record %v", r.ID)
	}
	ok, err := it.Next(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed to write record %v", r.ID)
	}
	if !ok {
		return errors.Newf("failed to write record %v", r.ID)
	}
	row := it.Cur()
	if err := it.Close(); err != nil {
		log.Infof(ctx, "encountered %v when writing record %v", err, r.ID)
	}
	if failed := *row[0].(*tree.DBool); failed {
		curNumSpans := int64(*row[1].(*tree.DInt))
		if s.maxSpans > 0 && curNumSpans+int64(len(r.Spans)) > s.maxSpans {
			return errors.WithHint(
				errors.Errorf("protectedts: limit exceeded: %d+%d > %d spans", curNumSpans,
					len(r.Spans), s.maxSpans),
				"SET CLUSTER SETTING kv.protectedts.max_spans to a higher value")
		}
		curBytes := int64(*row[2].(*tree.DInt))
		recordBytes := int64(len(encodedSpans) + len(r.Meta) + len(r.MetaType))
		if s.maxBytes > 0 && curBytes+recordBytes > s.maxBytes {
			return errors.WithHint(
				errors.Errorf("protectedts: limit exceeded: %d+%d > %d bytes", curBytes, recordBytes,
					s.maxBytes),
				"SET CLUSTER SETTING kv.protectedts.max_bytes to a higher value")
		}
		return protectedts.ErrExists
	}
	return nil
}

func (p *storage) GetRecord(ctx context.Context, txn *kv.Txn, id uuid.UUID) (*ptpb.Record, error) {
	if txn == nil {
		return nil, errNoTxn
	}
	row, err := p.ex.QueryRowEx(ctx, "protectedts-GetRecord", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		getRecordQuery, id.GetBytesMut())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read record %v", id)
	}
	if len(row) == 0 {
		return nil, protectedts.ErrNotExists
	}
	var r ptpb.Record
	if err := rowToRecord(ctx, row, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (p *storage) MarkVerified(ctx context.Context, txn *kv.Txn, id uuid.UUID) error {
	if txn == nil {
		return errNoTxn
	}
	numRows, err := p.ex.ExecEx(ctx, "protectedts-MarkVerified", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		markVerifiedQuery, id.GetBytesMut())
	if err != nil {
		return errors.Wrapf(err, "failed to mark record %v as verified", id)
	}
	if numRows == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p *storage) Release(ctx context.Context, txn *kv.Txn, id uuid.UUID) error {
	if txn == nil {
		return errNoTxn
	}
	numRows, err := p.ex.ExecEx(ctx, "protectedts-Release", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		releaseQuery, id.GetBytesMut())
	if err != nil {
		return errors.Wrapf(err, "failed to release record %v", id)
	}
	if numRows == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p *storage) GetMetadata(ctx context.Context, txn *kv.Txn) (ptpb.Metadata, error) {
	if txn == nil {
		return ptpb.Metadata{}, errNoTxn
	}
	row, err := p.ex.QueryRowEx(ctx, "protectedts-GetMetadata", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		getMetadataQuery)
	if err != nil {
		return ptpb.Metadata{}, errors.Wrap(err, "failed to read metadata")
	}
	if row == nil {
		return ptpb.Metadata{}, errors.New("failed to read metadata")
	}
	return ptpb.Metadata{
		Version:    uint64(*row[0].(*tree.DInt)),
		NumRecords: uint64(*row[1].(*tree.DInt)),
		NumSpans:   uint64(*row[2].(*tree.DInt)),
		TotalBytes: uint64(*row[3].(*tree.DInt)),
	}, nil
}

func (p *storage) GetState(ctx context.Context, txn *kv.Txn) (ptpb.State, error) {
	if txn == nil {
		return ptpb.State{}, errNoTxn
	}
	md, err := p.GetMetadata(ctx, txn)
	if err != nil {
		return ptpb.State{}, err
	}
	records, err := p.getRecords(ctx, txn)
	if err != nil {
		return ptpb.State{}, err
	}
	return ptpb.State{
		Metadata: md,
		Records:  records,
	}, nil
}

func (p *storage) getRecords(ctx context.Context, txn *kv.Txn) ([]ptpb.Record, error) {
	it, err := p.ex.QueryIteratorEx(ctx, "protectedts-GetRecords", txn,
		sessiondata.InternalExecutorOverride{User: security.NodeUserName()},
		getRecordsQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read records")
	}
	var ok bool
	var records []ptpb.Record
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		var record ptpb.Record
		if err := rowToRecord(ctx, it.Cur(), &record); err != nil {
			log.Errorf(ctx, "failed to parse row as record: %v", err)
		}
		records = append(records, record)
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to read records")
	}
	return records, nil
}

// rowToRecord parses a row as returned from the variants of getRecords and
// populates the passed *Record. If any errors are encountered during parsing,
// they are logged but not returned. Returning an error due to malformed data
// in the protected timestamp subsystem would create more problems than it would
// solve. Malformed records can still be removed (and hopefully will be).
func rowToRecord(ctx context.Context, row tree.Datums, r *ptpb.Record) error {
	r.ID = row[0].(*tree.DUuid).UUID
	tsDecimal := row[1].(*tree.DDecimal)
	ts, err := tree.DecimalToHLC(&tsDecimal.Decimal)
	if err != nil {
		return errors.Wrapf(err, "failed to parse timestamp for %v", r.ID)
	}
	r.Timestamp = ts

	r.MetaType = string(*row[2].(*tree.DString))
	if row[3] != tree.DNull {
		if meta := row[3].(*tree.DBytes); len(*meta) > 0 {
			r.Meta = []byte(*meta)
		}
	}
	var spans Spans
	if err := protoutil.Unmarshal([]byte(*row[4].(*tree.DBytes)), &spans); err != nil {
		return errors.Wrapf(err, "failed to unmarshal spans for %v", r.ID)
	}
	r.Spans = spans.Spans
	r.Verified = bool(*row[5].(*tree.DBool))
	return nil
}

type settings struct {
	maxSpans int64
	maxBytes int64
}

func makeSettings(s *cluster.Settings) settings {
	return settings{
		maxSpans: protectedts.MaxSpans.Get(&s.SV),
		maxBytes: protectedts.MaxBytes.Get(&s.SV),
	}
}

var (
	errZeroTimestamp        = errors.New("invalid zero value timestamp")
	errZeroID               = errors.New("invalid zero value ID")
	errEmptySpans           = errors.Errorf("invalid empty set of spans")
	errInvalidMeta          = errors.Errorf("invalid Meta with empty MetaType")
	errCreateVerifiedRecord = errors.Errorf("cannot create a verified record")
)

func validateRecordForProtect(r *ptpb.Record) error {
	if r.Timestamp.IsEmpty() {
		return errZeroTimestamp
	}
	if r.ID == uuid.Nil {
		return errZeroID
	}
	if len(r.Spans) == 0 {
		return errEmptySpans
	}
	if len(r.Meta) > 0 && len(r.MetaType) == 0 {
		return errInvalidMeta
	}
	if r.Verified {
		return errCreateVerifiedRecord
	}
	return nil
}
