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
	"github.com/cockroachdb/cockroach/pkg/util/encoding/csv"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

type csvEncoder struct {
	alloc                   tree.DatumAlloc
	virtualColumnVisibility string

	csvRow []string
	buf    *bytes.Buffer
	writer *csv.Writer
}

var _ Encoder = &csvEncoder{}

func newCSVEncoder(opts map[string]string) *csvEncoder {
	newBuf := bytes.NewBuffer([]byte{})
	newEncoder := &csvEncoder{
		virtualColumnVisibility: opts[changefeedbase.OptVirtualColumns],
		buf:                     newBuf,
		writer:                  csv.NewWriter(newBuf),
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
		return nil, errors.Errorf(`cannot encode deleted rows into CSV format`)
	}
	e.csvRow = e.csvRow[:0]
	if err := updatedRow.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		e.csvRow = append(e.csvRow, tree.AsString(d))
		return nil
	}); err != nil {
		return nil, err
	}

	e.buf.Reset()
	if err := e.writer.Write(e.csvRow); err != nil {
		return nil, err
	}
	e.writer.Flush()
	return e.buf.Bytes(), nil
}

// EncodeResolvedTimestamp implements the Encoder interface.
func (e *csvEncoder) EncodeResolvedTimestamp(
	_ context.Context, _ string, resolved hlc.Timestamp,
) ([]byte, error) {
	return nil, errors.New("EncodeResolvedTimestamp is not supported with the CSV encoder")
}
