// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// encodeRow holds all the pieces necessary to encode a row change into a key or
// value.
type encodeRow struct {
	// datums is the new value of a changed table row.
	datums rowenc.EncDatumRow
	// updated is the mvcc timestamp corresponding to the latest
	// update in `datums` or, if the row is part of a backfill,
	// the time at which the backfill was started.
	updated hlc.Timestamp
	// mvccTimestamp is the mvcc timestamp corresponding to the
	// latest update in `datums`.
	mvccTimestamp hlc.Timestamp
	// deleted is true if row is a deletion. In this case, only the primary
	// key columns are guaranteed to be set in `datums`.
	deleted bool
	// tableDesc is a TableDescriptor for the table containing `datums`.
	// It's valid for interpreting the row at `updated`.
	tableDesc catalog.TableDescriptor
	// familyID indicates which column family is populated on this row.
	// It's valid for interpreting the row at `updated`.
	familyID descpb.FamilyID
	// prevDatums is the old value of a changed table row. The field is set
	// to nil if the before value for changes was not requested (OptDiff).
	prevDatums rowenc.EncDatumRow
	// prevDeleted is true if prevDatums is missing or is a deletion.
	prevDeleted bool
	// prevTableDesc is a TableDescriptor for the table containing `prevDatums`.
	// It's valid for interpreting the row at `updated.Prev()`.
	prevTableDesc catalog.TableDescriptor
	// prevFamilyID indicates which column family is populated in prevDatums.
	prevFamilyID descpb.FamilyID
	// topic is set to the string to be included if TopicInValue is true
	topic string
}

// Encoder turns a row into a serialized changefeed key, value, or resolved
// timestamp. It represents one of the `format=` changefeed options.
type Encoder interface {
	// EncodeKey encodes the primary key of the given row. The columns of the
	// datums are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`, but only the primary key fields will be used. The
	// returned bytes are only valid until the next call to Encode*.
	EncodeKey(context.Context, encodeRow) ([]byte, error)
	// EncodeValue encodes the primary key of the given row. The columns of the
	// datums are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeValue(context.Context, encodeRow) ([]byte, error)
	// EncodeResolvedTimestamp encodes a resolved timestamp payload for the
	// given topic name. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeResolvedTimestamp(context.Context, string, hlc.Timestamp) ([]byte, error)
}

func getEncoder(
	opts map[string]string, targets []jobspb.ChangefeedTargetSpecification,
) (Encoder, error) {
	switch changefeedbase.FormatType(opts[changefeedbase.OptFormat]) {
	case ``, changefeedbase.OptFormatJSON:
		return makeJSONEncoder(opts, targets)
	case changefeedbase.OptFormatAvro, changefeedbase.DeprecatedOptFormatAvro:
		return newConfluentAvroEncoder(opts, targets)
	case changefeedbase.OptFormatCSV:
		return newCSVEncoder(opts), nil
	default:
		return nil, errors.Errorf(`unknown %s: %s`, changefeedbase.OptFormat, opts[changefeedbase.OptFormat])
	}
}
