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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Consider memory accounting.
// TODO(ajwerner): Add metrics.

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

func (p *storage) Protect(ctx context.Context, txn *client.Txn, r *ptpb.Record) error {
	if err := validateRecord(r); err != nil {
		return err
	}
	encodedSpans, err := protoutil.Marshal(&ptpb.Spans{Spans: r.Spans})
	if err != nil {
		return errors.Wrap(err, "failed to marshal spans")
	}
	s := makeSettings(p.settings)
	row, err := p.ex.QueryRow(ctx, "protectedts-protect", txn, protectQuery,
		s.maxRecords, s.maxSpans, s.maxBytes, len(r.Spans),
		[]byte(r.ID[:]), r.Timestamp.AsOfSystemTime(),
		r.MetaType, r.Meta,
		len(r.Spans), encodedSpans)
	if err != nil {
		return err
	}
	if failed := *row[0].(*tree.DBool); failed {
		records := int64(*row[1].(*tree.DInt))
		if records >= s.maxRecords {
			return errors.Errorf("limits exceeded: %d+1 > %d rows",
				records, s.maxRecords)
		}
		curNumSpans := int64(*row[2].(*tree.DInt))
		if curNumSpans >= s.maxSpans {
			return errors.Errorf("limits exceeded: %d+%d > %d spans",
				curNumSpans, len(r.Spans), s.maxSpans)
		}
		curSpanBytes := int64(*row[3].(*tree.DInt))
		if curSpanBytes >= s.maxBytes {
			return errors.Errorf("limits exceeded: %d+%d > %d bytes",
				curSpanBytes, len(encodedSpans), s.maxBytes)
		}
		return protectedts.ErrExists
	}
	return nil
}

func validateRecord(r *ptpb.Record) error {
	if r.Timestamp == (hlc.Timestamp{}) {
		return errors.Errorf("cannot protect zero value timestamp")
	}
	if r.Verified {
		return errors.Errorf("Cannot create a verified record")
	}
	if len(r.Spans) == 0 {
		return errors.Errorf("cannot protect empty set of spans")
	}
	return nil
}

func (p *storage) GetRecord(
	ctx context.Context, txn *client.Txn, id uuid.UUID,
) (*ptpb.Record, error) {
	row, err := p.ex.QueryRow(ctx, "protectedts-GetRecord", txn, getRecordQuery, id.String())
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, protectedts.ErrNotFound
	}
	var r ptpb.Record
	if err := rowToRecord(row, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

func (p *storage) MarkVerified(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	row, err := p.ex.QueryRow(ctx, "protectedts-MarkVerified", txn, markVerifiedQuery, id.String())
	if err != nil {
		return err
	}
	if len(row) == 0 {
		return protectedts.ErrNotFound
	}
	return nil
}

func (p *storage) Release(ctx context.Context, txn *client.Txn, id uuid.UUID) error {
	// Check if the record exists
	// If the record exists then delete it and update the meta row
	row, err := p.ex.QueryRow(ctx, "protectedts-Release", txn, releaseQuery, id[:])
	if err != nil {
		return err
	}
	if len(row) == 0 {
		return protectedts.ErrNotFound
	}
	return nil
}

func (p *storage) GetMetadata(ctx context.Context, txn *client.Txn) (ptpb.Metadata, error) {
	row, err := p.ex.QueryRow(ctx, "protectedts-GetMetadata", txn, getMetadataQuery)
	if err != nil {
		return ptpb.Metadata{}, errors.Wrap(err, "failed to fetch metadata")
	}
	return ptpb.Metadata{
		Version:    uint64(*row[0].(*tree.DInt)),
		NumRecords: uint64(*row[1].(*tree.DInt)),
		NumSpans:   uint64(*row[2].(*tree.DInt)),
		TotalBytes: uint64(*row[3].(*tree.DInt)),
	}, nil
}

func (p *storage) GetState(ctx context.Context, txn *client.Txn) (ptpb.State, error) {
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
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	records := make([]ptpb.Record, len(rows))
	for i, row := range rows {
		if err := rowToRecord(row, &records[i]); err != nil {
			return nil, err
		}
	}
	return records, nil
}

func rowToRecord(row tree.Datums, r *ptpb.Record) (err error) {
	r.ID = row[0].(*tree.DUuid).UUID
	tsDecimal := row[1].(*tree.DDecimal)
	if r.Timestamp, err = tree.DecimalToHLC(&tsDecimal.Decimal); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "failed to parse decimal as HLC")
	}
	r.MetaType = string(*row[2].(*tree.DString))
	if meta := row[3].(*tree.DBytes); meta != nil && len(*meta) > 0 {
		r.Meta = []byte(*meta)
	}
	var spans ptpb.Spans
	if err := protoutil.Unmarshal([]byte(*row[4].(*tree.DBytes)), &spans); err != nil {
		return errors.NewAssertionErrorWithWrappedErrf(err, "failed to parse spans")
	}
	r.Spans = spans.Spans
	r.Verified = bool(*row[5].(*tree.DBool))
	return nil
}

type settings struct {
	maxRecords int64
	maxSpans   int64
	maxBytes   int64
}

func makeSettings(s *cluster.Settings) settings {
	return settings{
		maxRecords: protectedts.MaxRecords.Get(&s.SV),
		maxSpans:   protectedts.MaxSpans.Get(&s.SV),
		maxBytes:   protectedts.MaxBytes.Get(&s.SV),
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
num_records AS prev_records,
num_spans AS prev_spans, 
total_bytes AS prev_total_bytes,
version AS prev_version
FROM
checks, current_meta;`

	protectSelectRecordCTE = `
SELECT NULL
FROM system.protected_ts_records
WHERE id = $5::UUID`

	protectChecksCTE = `
SELECT
    new_version, 
    new_num_records,
    new_num_spans, 
    new_total_bytes,
    (
       new_num_records > $1
       OR new_num_spans > $2
       OR new_total_bytes > $3
       OR EXISTS(SELECT * FROM old_record)
    ) AS failed
FROM (
    SELECT
        version + 1 AS new_version,
        num_records + 1 AS new_num_records, 
        num_spans + $4 AS new_num_spans, 
        total_bytes + length($10) + length($7) + length($8) AS new_total_bytes
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
        (VALUES ($5::UUID, $6::DECIMAL, $7, $8, $9, $10))
            AS _ (id, ts, meta_type, meta, num_spans, spans),
        checks
    WHERE
        NOT failed
)
RETURNING id`

	getRecordsQuery = `` + `SELECT id, ts, meta_type, meta, spans, verified ` +
		`FROM system.protected_ts_records`

	getRecordQuery = getRecordsQuery + " WHERE id = $1"

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
		`WHERE id = $1`

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
