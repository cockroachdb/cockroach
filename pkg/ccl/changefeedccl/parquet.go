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
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
)

// NewCDCParquetWriterFromRow constructs a new parquet writer which outputs to
// the given sink. This function interprets the schema from the supplied row.
func NewCDCParquetWriterFromRow(
	row cdcevent.Row, sink io.Writer, maxRowGroupSize int64,
) (*parquet.Writer, error) {
	columnNames := make([]string, len(row.ResultColumns())+1)
	columnTypes := make([]*types.T, len(row.ResultColumns())+1)

	idx := 0
	if err := row.ForEachColumn().Col(func(col cdcevent.ResultColumn) error {
		columnNames[idx] = col.Name
		columnTypes[idx] = col.Typ
		idx += 1
		return nil
	}); err != nil {
		return nil, err
	}

	columnNames[idx] = parquetCrdbEventTypeColName
	columnTypes[idx] = types.String

	schemaDef, err := parquet.NewSchema(columnNames, columnTypes)
	if err != nil {
		return nil, err
	}

	return parquet.NewWriter(schemaDef, sink, parquet.WithMaxRowGroupLength(maxRowGroupSize))
}

// AddData writes the updatedRow to the writer, adding the row's event type.
// If returnDatums is true, this method will return the array of datums
// which were written. Otherwise, the returned array will be nil.
func AddData(
	writer *parquet.Writer, updatedRow cdcevent.Row, prevRow cdcevent.Row, returnDatums bool,
) ([]tree.Datum, error) {
	eventTypeDatum := getEventTypeDatum(updatedRow, prevRow).DString()
	var datumRow []tree.Datum

	addDataToReturn := func(d tree.Datum) {
		if returnDatums {
			if datumRow == nil {
				datumRow = []tree.Datum{}
			}
			datumRow = append(datumRow, d)
		}
	}

	rowWriter, err := writer.AddData()
	if err != nil {
		return nil, err
	}

	if err := updatedRow.ForEachColumn().Datum(func(d tree.Datum, _ cdcevent.ResultColumn) error {
		if err = rowWriter.WriteColumn(d); err != nil {
			return err
		}
		addDataToReturn(d)
		return nil
	}); err != nil {
		return nil, err
	}
	if err = rowWriter.WriteColumn(eventTypeDatum); err != nil {
		return nil, err
	}
	addDataToReturn(eventTypeDatum)
	return datumRow, err
}
