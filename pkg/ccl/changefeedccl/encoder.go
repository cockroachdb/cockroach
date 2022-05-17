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
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
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
