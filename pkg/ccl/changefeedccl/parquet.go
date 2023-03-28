// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"io"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/roachparquet"
)

// NewCDCParquetWriterFromRow constructs a new parquet writer which outputs to
// the given sink. This function interprets the schema from the supplied row.
// Rows passed to Writer.AddData should have the same schema as the row
// passed to this function.
//
// maxRowGroupSize specifies the maximum number of rows to include
// in a row group when writing to the sink.
func NewCDCParquetWriterFromRow(
	row cdcevent.Row, sink io.Writer, maxRowGroupSize int64,
) (*roachparquet.Writer, error) {

	schemaRowIter := func(f func(colName string, typ *types.T) error) error {
		if err := row.ForEachColumn().Col(func(col cdcevent.ResultColumn) error {
			if err := f(col.Name, col.Typ); err != nil {
				return err
			}

			return nil
		}); err != nil {
			return err
		}

		if err := f(parquetCrdbEventTypeColName, types.String); err != nil {
			return err
		}

		return nil
	}

	schemaDef, err := roachparquet.NewSchema(schemaRowIter)
	if err != nil {
		return nil, err
	}

	return roachparquet.NewWriter(schemaDef, sink, maxRowGroupSize)
}

func AddData(
	writer *roachparquet.Writer, updatedRow cdcevent.Row, prevRow cdcevent.Row, returnDatums bool,
) ([]tree.Datum, error) {
	eventType := getEventTypeDatum(updatedRow, prevRow)

	datumRow := make([]tree.Datum, 0)
	idx := 0

	addCol := func(d tree.Datum) {
		if !returnDatums {
			return
		}

		datumRow = append(datumRow, d)
	}
	if returnDatums {
		datumRow = make([]tree.Datum, 0)
	}
	datumIter := func(f func(d tree.Datum) error) error {
		if err := updatedRow.ForEachColumn().Datum(func(d tree.Datum, _ cdcevent.ResultColumn) error {
			if err := f(d); err != nil {
				return err
			}

			addCol(d)
			idx += 1
			return nil
		}); err != nil {
			return err
		}

		if err := f(eventType.DString()); err != nil {
			return err
		}
		addCol(eventType.DString())
		return nil
	}

	return datumRow, writer.AddData(datumIter)
}
