// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

		switch di := d.(type) {
		case *tree.DCollatedString:
			e.formatter.WriteString(di.Contents)
		default:
			e.formatter.FormatNode(d)
		}

		return e.writer.WriteField(&e.formatter.Buffer)
	}); err != nil {
		return nil, err
	}

	if err := e.writer.FinishRecord(); err != nil {
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
