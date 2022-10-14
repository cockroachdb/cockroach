// Copyright 2019 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	pqexporter "github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/pkg/errors"
)

// We need a separate sink for parquet format because the parquet encoder has to
// write metadata to the parquet file (buffer) after each flush. This means that the
// parquet encoder should have access to the buffer object inside
// cloudStorageSinkFile file. This means that the parquet writer has to be
// embedded in the cloudStorageSinkFile file. If we wanted to maintain the
// existing separation between encoder and the sync, then we would need to
// figure out a way to get the embedded parquet writer in the
// cloudStorageSinkFile and pass it to the encode function in the encoder.
// Instead of this it logically made sense to have a single sink for parquet
// format which did the job of both encoding and emitting to the cloud storage.
// This sink currently embeds the cloudStorageSinkFile and it has a function
// EncodeAndEmitRow which links the buffer in the cloudStorageSinkFile and the
// parquetWriter every time we need to create a file for each unique combination
// of topic and schema version (We need to have a unique parquetWriter for each
// unique combination of topic and schema version because each parquet file can
// have a single schema written inside it and each parquetWriter can only be
// associated with a single schema.)
type parquetCloudStorageSink struct {
	wrapped *cloudStorageSink
}

type parquetWriterWrapper struct {
	parquetWriter  *goparquet.FileWriter
	schema         *parquetschema.SchemaDefinition
	parquetColumns []pqexporter.ParquetColumn
	numCols        int
}

func makeParquetCloudStorageSink(baseCloudStorageSink *cloudStorageSink) *parquetCloudStorageSink {
	pcs := &parquetCloudStorageSink{}

	pcs.wrapped = baseCloudStorageSink

	return pcs
}

func (pqcs *parquetCloudStorageSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {

	return errors.Errorf("Emit Row should not be called for parquet format")

}

func (s *parquetCloudStorageSink) Close() error {
	return s.wrapped.Close()
}

func (s *parquetCloudStorageSink) Dial() error {
	return s.wrapped.Dial()
}

func (s *parquetCloudStorageSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	return errors.Errorf("parquet format does not support emitting resolved timestamp")
}

func (pqcs *parquetCloudStorageSink) Flush(ctx context.Context) error {
	return pqcs.wrapped.Flush(ctx)
}

// EncodeAndEmitRow links the buffer in the cloud storage sync file and the
// parquet writer (see parquetCloudStorageSink). It also takes care of encoding
// and emitting row event to cloud storage.
func (pqcs *parquetCloudStorageSink) EncodeAndEmitRow(
	ctx context.Context,
	updatedRow cdcevent.Row,
	topic TopicDescriptor,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {

	s := pqcs.wrapped
	var pqww *parquetWriterWrapper
	file, err := s.getOrCreateFile(topic, mvcc)
	if err != nil {
		return err
	}

	if file.pw == nil {
		var err error
		pqww, err = makeparquetWriterWrapper(ctx, updatedRow, &file.buf)
		if err != nil {
			return err
		}
		file.pw = pqww
	} else {
		pqww = file.pw
	}

	i := -1
	parquetRow := make(map[string]interface{}, pqww.numCols)
	// Revisit: I am assuming that this iterates through the columns in
	// the same order as when iterating through the columns when
	// creating the schema. this is important because each encode
	// function is dependent on the column position.
	updatedRow.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		i++
		// Omit NULL columns from parquet row (cannot think of any other way of
		// encoding NULL values)
		if d == tree.DNull {
			parquetRow[col.Name] = nil
			return nil
		}
		// Revisit: should probably wrap these errors with something
		// more meaningful for upstream
		encodeFn, err := pqww.parquetColumns[i].GetEncoder()
		if err != nil {
			return err
		}
		// Revisit: no clue what unwrap datum does, using it cuz export also did
		edNative, err := encodeFn(eval.UnwrapDatum(nil, d))
		if err != nil {
			return err
		}
		colName, err := pqww.parquetColumns[i].Name()
		if err != nil {
			return err
		}

		parquetRow[colName] = edNative

		return nil

	})

	pqww.parquetWriter.AddData(parquetRow)

	file.alloc.Merge(&alloc)
	if pqww.parquetWriter.CurrentRowGroupSize() > s.targetMaxFileSize {
		s.metrics.recordSizeBasedFlush()

		pqww.parquetWriter.Close()
		if err := s.flushTopicVersions(ctx, file.topic, file.schemaID); err != nil {
			return err
		}
	}

	return nil

}

func makeparquetWriterWrapper(
	ctx context.Context, row cdcevent.Row, buf *bytes.Buffer,
) (*parquetWriterWrapper, error) {

	parquetColumns, err := getParquetColumnTypes(ctx, row)
	if err != nil {
		return nil, err
	}

	schema := pqexporter.NewParquetSchema(parquetColumns)

	keyCols := make(map[string]string)
	colNames := ""
	row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		colNames += col.Name + ","
		return nil
	})
	keyCols["pkeys"] = colNames

	// Revisit: not using parquets inbuilt compressor, relying on sinks
	// compression
	pqw := goparquet.NewFileWriter(buf,
		goparquet.WithSchemaDefinition(schema),
		goparquet.WithMetaData(keyCols),
	)

	pqww := &parquetWriterWrapper{
		pqw,
		schema,
		parquetColumns,
		len(parquetColumns),
	}
	return pqww, nil
}

func getParquetColumnTypes(
	ctx context.Context, row cdcevent.Row,
) ([]pqexporter.ParquetColumn, error) {
	typs := make([]*types.T, 0)
	names := make([]string, 0)

	row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		typs = append(typs, col.Typ)
		names = append(names, col.Name)
		return nil
	})

	parquetColumns := make([]pqexporter.ParquetColumn, len(typs))

	for i := 0; i < len(typs); i++ {
		// Revisit: im passing "true" for all columns, meaning that all
		// value columns are nullable. make sure this is correct.
		parquetCol, err := pqexporter.NewParquetColumn(typs[i], names[i], true)
		if err != nil {
			return nil, err
		}
		parquetColumns[i] = parquetCol
	}

	return parquetColumns, nil
}
