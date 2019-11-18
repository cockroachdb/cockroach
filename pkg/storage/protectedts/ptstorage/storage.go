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

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
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

func (p *storage) Protect(ctx context.Context, txn *client.Txn, r *ptpb.Record) error {
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
	s := makeSettings(p.settings)
	row, err := p.ex.QueryRow(ctx, "protectedts-protect", txn, protectQuery,
		s.maxSpans, s.maxBytes, len(r.Spans),
		[]byte(r.ID[:]), r.Timestamp.AsOfSystemTime(),
		r.MetaType, r.Meta,
		len(r.Spans), encodedSpans)
	if err != nil {
		return errors.Wrapf(err, "failed to write record %v", r.ID)
	}
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

func validateRecordForProtect(r *ptpb.Record) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Verified {
		return errors.Errorf("cannot create a verified record")
	}
	return nil
}

func (p *storage) GetRecord(
	ctx context.Context, txn *client.Txn, id uuid.UUID,
) (*ptpb.Record, error) {
	if txn == nil {
		return nil, errNoTxn
	}
	row, err := p.ex.QueryRow(ctx, "protectedts-GetRecord", txn, getRecordQuery, id[:])
	if err != nil {
		return nil, errors.Wrapf(err, "failed to read record %v", id)
	}
	if len(row) == 0 {
		return nil, protectedts.ErrNotExists
	}
	var r ptpb.Record
	rowToRecord(ctx, row, &r)
	return &r, nil
}

func (p *storage) MarkVerified(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	if txn == nil {
		return errNoTxn
	}
	row, err := p.ex.QueryRow(ctx, "protectedts-MarkVerified", txn, markVerifiedQuery, id[:])
	if err != nil {
		return errors.Wrapf(err, "failed to mark record %v as verified", id)
	}
	if len(row) == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p *storage) Release(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	if txn == nil {
		return errNoTxn
	}
	row, err := p.ex.QueryRow(ctx, "protectedts-Release", txn, releaseQuery, id[:])
	if err != nil {
		return errors.Wrapf(err, "failed to release record %v", id)
	}
	if len(row) == 0 {
		return protectedts.ErrNotExists
	}
	return nil
}

func (p *storage) GetMetadata(ctx context.Context, txn *client.Txn) (ptpb.Metadata, error) {
	if txn == nil {
		return ptpb.Metadata{}, errNoTxn
	}
	row, err := p.ex.QueryRow(ctx, "protectedts-GetMetadata", txn, getMetadataQuery)
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

func (p *storage) GetState(ctx context.Context, txn *client.Txn) (ptpb.State, error) {
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

func (p *storage) getRecords(ctx context.Context, txn *client.Txn) ([]ptpb.Record, error) {
	rows, err := p.ex.Query(ctx, "protectedts-GetRecords", txn, getRecordsQuery)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read records")
	}
	if len(rows) == 0 {
		return nil, nil
	}
	records := make([]ptpb.Record, len(rows))
	for i, row := range rows {
		rowToRecord(ctx, row, &records[i])
	}
	return records, nil
}

// rowToRecord parses a row as returned from the variants of getRecords and
// populates the passed *Record. If any errors are encountered during parsing,
// they are logged but not returned. Returning an error due to malformed data
// in the protected timestamp subsystem would create more problems than it would
// solve. Malformed records can still be removed (and hopefully will be).
func rowToRecord(ctx context.Context, row tree.Datums, r *ptpb.Record) {
	r.ID = row[0].(*tree.DUuid).UUID
	tsDecimal := row[1].(*tree.DDecimal)
	if ts, err := tree.DecimalToHLC(&tsDecimal.Decimal); err != nil {
		log.Errorf(ctx, "failed to parse decimal as HLC for %v: %v", r.ID, err)
	} else {
		r.Timestamp = ts
	}
	r.MetaType = string(*row[2].(*tree.DString))
	if meta := row[3].(*tree.DBytes); meta != nil && len(*meta) > 0 {
		r.Meta = []byte(*meta)
	}
	var spans Spans
	if err := protoutil.Unmarshal([]byte(*row[4].(*tree.DBytes)), &spans); err != nil {
		log.Errorf(ctx, "failed to unmarshal spans for %v: %v", r.ID, err)
	}
	r.Spans = spans.Spans
	r.Verified = bool(*row[5].(*tree.DBool))
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

const (

	// currentMetaCTEs are used by all queries which access the meta row.
	currentMetaCTEs = `
read_meta AS (
    SELECT version, num_records, num_spans, total_bytes FROM system.protected_ts_meta
),` +
		// TODO(ajwerner): consider installing a migration to ensure that this row is initialized
		`
current_meta AS (
     SELECT version, num_records, num_spans, total_bytes FROM read_meta
     UNION ALL
     SELECT
         0 AS version, 0 AS num_records, 0 AS num_spans, 0 AS total_bytes
     WHERE
         NOT EXISTS (SELECT * FROM read_meta)
)`

	protectQuery = `WITH ` +
		currentMetaCTEs + `, ` +
		`old_record AS ( ` + protectSelectRecordCTE + ` ), ` +
		`checks AS ( ` + protectChecksCTE + ` ), ` +
		`updated_meta AS ( ` + protectUpsertMetaCTE + ` ), ` +
		`new_record AS ( ` + protectInsertRecordCTE + ` )
SELECT
    failed, 
    num_spans AS prev_spans, 
    total_bytes AS prev_total_bytes,
    version AS prev_version
FROM
    checks, current_meta;`

	protectSelectRecordCTE = `
SELECT NULL
FROM system.protected_ts_records
WHERE id = $4::UUID`

	protectChecksCTE = `
SELECT
    new_version, 
    new_num_records,
    new_num_spans, 
    new_total_bytes,
    (
       new_num_spans > $1
       OR new_total_bytes > $2
       OR EXISTS(SELECT * FROM old_record)
    ) AS failed
FROM (
    SELECT
        version + 1 AS new_version,
        num_records + 1 AS new_num_records, 
        num_spans + $3 AS new_num_spans, 
        total_bytes + length($9) + length($6) + length($7) AS new_total_bytes
    FROM
        current_meta
)`

	protectUpsertMetaCTE = `
UPSERT
INTO
    system.protected_ts_meta
(version, num_records, num_spans, total_bytes)
(
    SELECT
        new_version, new_num_records, new_num_spans, new_total_bytes
    FROM
        checks
    WHERE
        NOT failed
)
RETURNING
    version, num_records, num_spans, total_bytes`

	protectInsertRecordCTE = `
INSERT
INTO
    system.protected_ts_records (id, ts, meta_type, meta, num_spans, spans)
(
    SELECT
        id, ts, meta_type, meta, num_spans, spans
    FROM
        (VALUES ($4::UUID, $5::DECIMAL, $6, $7, $8, $9))
            AS _ (id, ts, meta_type, meta, num_spans, spans),
        checks
    WHERE
        NOT failed
)
RETURNING id`

	getRecordsQuery = `` + `SELECT id, ts, meta_type, meta, spans, verified ` +
		`FROM system.protected_ts_records`

	getRecordQuery = getRecordsQuery + " WHERE id = $1::UUID"

	markVerifiedQuery = `
	UPDATE 
		system.protected_ts_records
	SET 
		verified = true
	WHERE 
		id = $1::UUID
	RETURNING 
		true`

	releaseQuery = `WITH ` +
		currentMetaCTEs + `, ` +
		`record AS (` + releaseSelectRecordCTE + `), ` +
		`updated_meta AS ( ` + releaseUpsertMetaCTE + ` ) ` +
		`DELETE FROM system.protected_ts_records AS r ` +
		`WHERE EXISTS(SELECT NULL FROM record WHERE r.id = record.id) ` +
		`RETURNING NULL`

	// Collect the number of spans for the record identified by $1.
	releaseSelectRecordCTE = `SELECT ` +
		`id, ` +
		`num_spans AS record_spans, ` +
		`(length(spans)+length(meta_type)+length(meta)) AS record_bytes ` +
		`FROM system.protected_ts_records ` +
		`WHERE id = $1::UUID`

	// Updates the meta row if there was a record.
	releaseUpsertMetaCTE = `UPSERT INTO
    system.protected_ts_meta (version, num_records, num_spans, total_bytes)
(
    SELECT
        version, num_records, num_spans, total_bytes
    FROM
        (
            SELECT
                version + 1 AS version,
                num_records - 1 AS num_records,
                num_spans - record_spans AS num_spans,
                total_bytes - record_bytes AS total_bytes
             FROM
                current_meta, record
        ), record
    WHERE
        EXISTS(SELECT * FROM record)
)
RETURNING 1`

	getMetadataQuery = `WITH ` + currentMetaCTEs + ` ` +
		`SELECT version, num_records, num_spans, total_bytes FROM current_meta`
)
