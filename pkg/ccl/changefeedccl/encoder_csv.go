// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type csvEncoder struct {
	buf       *bytes.Buffer
	formatter *tree.FmtCtx
	writer    *csv.Writer
}

var _ Encoder = &csvEncoder{}

func newCSVEncoder(opts changefeedbase.EncodingOptions) *csvEncoder {
	newBuf := bytes.NewBuffer([]byte{})
	newEncoder := &csvEncoder{
		buf:       newBuf,
		formatter: tree.NewFmtCtx(tree.FmtExport),
		writer:    csv.NewWriter(newBuf),
	}
	newEncoder.writer.SkipNewline = true
	return newEncoder
}

// EncodeKey implements the Encoder interface.
func (e *csvEncoder) EncodeKey(_ context.Context, row cdcevent.Row) ([]byte, error) {
	return nil, nil
}

// EncodeValue implements the Encoder interface.
func (e *csvEncoder) EncodeValue(
	ctx context.Context, evCtx eventContext, updatedRow cdcevent.Row, prevRow cdcevent.Row,
) ([]byte, error) {
	if updatedRow.IsDeleted() {
		return nil, errors.AssertionFailedf(`cannot encode deleted rows into CSV format`)
	}
	e.buf.Reset()
	if err := updatedRow.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		e.formatter.Reset()
		e.formatter.FormatNode(d)
		return e.writer.WriteField(&e.formatter.Buffer, mayNeedCSVEscaping(d.ResolvedType().Family()))
	}); err != nil {
		return nil, err
	}

	if err := e.writer.FinishRecord(); err != nil {
		return nil, err
	}
	e.writer.Flush()
	return e.buf.Bytes(), nil
}

// mayNeedCSVEscaping checks whether the given type is known to encode to a charset that can't contain
// special characters. For this encoder, special characters are double quotes, commas, leading spaces,
// and the special 2-byte string "\.".
func mayNeedCSVEscaping(f types.Family) bool {
	switch f {
	case types.BoolFamily, types.IntFamily, types.DecimalFamily, types.FloatFamily, types.BitFamily:
		// Literals with a limited charset.
		return false
	case types.BytesFamily:
		// Bytes are hex-encoded.
		return false
	}
	// There are definitely more types that could return false here, but probability of error goes up
	// with each one added so let's add them as needed.
	return true
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *csvEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, resolved hlc.Timestamp,
) ([]byte, error) {
	return nil, errors.New("EncodeResolvedTimestamp is not supported with the CSV encoder")
}
