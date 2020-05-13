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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	rows, err := p.ex.QueryEx(ctx, "protectedts-protect", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.NodeUser},
		protectQuery,
		s.maxSpans, s.maxBytes, len(r.Spans),
		r.ID.GetBytesMut(), r.Timestamp.AsOfSystemTime(),
		r.MetaType, meta,
		len(r.Spans), encodedSpans)
	if err != nil {
		return errors.Wrapf(err, "failed to write record %v", r.ID)
	}
	row := rows[0]
	if failed := *row[0].(*tree.DBool); failed {
		curNumSpans := int64(*row[1].(*tree.DInt))
		if curNumSpans+int64(len(r.Spans)) > s.maxSpans {
			return errors.Errorf("protectedts: limit exceeded: %d+%d > %d spans",
				curNumSpans, len(r.Spans), s.maxSpans)
		}
		curBytes := int64(*row[2].(*tree.DInt))
		recordBytes := int64(len(encodedSpans) + len(r.Meta) + len(r.MetaType))
		if curBytes+recordBytes > s.maxBytes {
			return errors.Errorf("protectedts: limit exceeded: %d+%d > %d bytes",
				curBytes, recordBytes, s.maxBytes)
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
		sqlbase.InternalExecutorSessionDataOverride{User: security.NodeUser},
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
	rows, err := p.ex.QueryEx(ctx, "protectedts-MarkVerified", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.NodeUser},
		markVerifiedQuery, id.GetBytesMut())
	if err != nil {
		return errors.Wrapf(err, "failed to mark record %v as verified", id)
	}
	if len(rows) == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p *storage) Release(ctx context.Context, txn *kv.Txn, id uuid.UUID) error {
	if txn == nil {
		return errNoTxn
	}
	rows, err := p.ex.QueryEx(ctx, "protectedts-Release", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.NodeUser},
		releaseQuery, id.GetBytesMut())
	if err != nil {
		return errors.Wrapf(err, "failed to release record %v", id)
	}
	if len(rows) == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p *storage) GetMetadata(ctx context.Context, txn *kv.Txn) (ptpb.Metadata, error) {
	if txn == nil {
		return ptpb.Metadata{}, errNoTxn
	}
	row, err := p.ex.QueryRowEx(ctx, "protectedts-GetMetadata", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.NodeUser},
		getMetadataQuery)
	if err != nil {
		return ptpb.Metadata{}, errors.Wrap(err, "failed to read metadata")
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
	rows, err := p.ex.QueryEx(ctx, "protectedts-GetRecords", txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.NodeUser},
		getRecordsQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read records")
	}
	if len(rows) == 0 {
		return nil, nil
	}
	records := make([]ptpb.Record, len(rows))
	for i, row := range rows {
		if err := rowToRecord(ctx, row, &records[i]); err != nil {
			log.Errorf(ctx, "failed to parse row as record: %v", err)
		}
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
	if r.Timestamp == (hlc.Timestamp{}) {
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
