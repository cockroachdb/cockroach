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

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Encoder turns a row into a serialized changefeed key, value, or resolved
// timestamp. It represents one of the `format=` changefeed options.
type Encoder interface {
	// EncodeKey encodes the primary key of the given row. The columns of the
	// datums are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`, but only the primary key fields will be used. The
	// returned bytes are only valid until the next call to Encode*.
	EncodeKey(context.Context, cdcevent.Row) ([]byte, error)
	// EncodeValue encodes the primary key of the given row. The columns of the
	// datums are expected to match 1:1 with the `Columns` field of the
	// `TableDescriptor`. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeValue(
		ctx context.Context,
		evCtx eventContext,
		updatedRow cdcevent.Row,
		prevRow cdcevent.Row,
	) ([]byte, error)
	// EncodeResolvedTimestamp encodes a resolved timestamp payload for the
	// given topic name. The returned bytes are only valid until the next call
	// to Encode*.
	EncodeResolvedTimestamp(context.Context, string, hlc.Timestamp) ([]byte, error)
}

func getEncoder(
	opts changefeedbase.EncodingOptions, targets changefeedbase.Targets,
) (Encoder, error) {
	switch opts.Format {
	case changefeedbase.OptFormatJSON:
		return makeJSONEncoder(opts)
	case changefeedbase.OptFormatAvro, changefeedbase.DeprecatedOptFormatAvro:
		return newConfluentAvroEncoder(opts, targets)
	case changefeedbase.OptFormatCSV:
		return newCSVEncoder(opts), nil
	case changefeedbase.OptFormatParquet:
		//We will return no encoder for parquet format because there is a separate
		//sink implemented for parquet format for cloud storage, which does the job
		//of both encoder and sink. See parquet_sink_cloudstorage.go file for more
		//information on why this was needed.
		return nil, nil
	default:
		return nil, errors.AssertionFailedf(`unknown format: %s`, opts.Format)
	}
}

// timestampToString converts an internal timestamp to the string form used in
// all encoders. This could be made more efficient. And/or it could be configurable
// to include the Synthetic flag when present, but that's unlikely to be needed.
func timestampToString(t hlc.Timestamp) string {
	return t.WithSynthetic(false).AsOfSystemTime()
}
