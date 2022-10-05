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
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	pqexporter "github.com/cockroachdb/cockroach/pkg/sql/importer"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/pkg/errors"
)

type parquetCloudStorageSink struct {
	baseCloudStorageSink *cloudStorageSink
}

type parquetWriterWrapper struct {
	parquetWriter  *goparquet.FileWriter
	schema         *parquetschema.SchemaDefinition
	parquetColumns []pqexporter.ParquetColumn
	numCols        int
}

func makeParquetCloudStorageSink(baseCloudStorageSink *cloudStorageSink) *parquetCloudStorageSink {
	pcs := &parquetCloudStorageSink{}

	pcs.baseCloudStorageSink = baseCloudStorageSink

	return pcs
}

func (pqcs *parquetCloudStorageSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc) error {

	return errors.Errorf("Emit Row should not be called for parquet format")

}

func (s *parquetCloudStorageSink) Close() error {
	return s.baseCloudStorageSink.Close()
}

func (s *parquetCloudStorageSink) Dial() error {
	return s.baseCloudStorageSink.Dial()
}

func (s *parquetCloudStorageSink) EmitResolvedTimestamp(
	ctx context.Context,
	encoder Encoder,
	resolved hlc.Timestamp) error {
	return errors.Errorf("parquet format does not support emitting resolved timestamp")
}

func (pqcs *parquetCloudStorageSink) Flush(ctx context.Context) error {
	return nil
}

func (pqcs *parquetCloudStorageSink) flushFile(ctx context.Context, file *cloudStorageSinkFile) error {
	s := pqcs.baseCloudStorageSink
	// filename := fmt.Sprintf(`%s-%s-%d-%d-%08x-%s-%x%s`, s.dataFileTs,
	// 	s.jobSessionID, s.srcID, s.sinkID, 1, "ganesh", 1234, ".parquet")

	filename_output := "/Users/ganeshb/go/src/github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/parquet_encoding_test_files/output.parquet"
	payload, err := os.ReadFile(filename_output)
	if err != nil {
		fmt.Printf("error whene opening file: %s\n", err)
	}
	n2, err := file.Write(payload)
	if err != nil {
		fmt.Printf("error whene writing: %s\n", err)
	}
	fmt.Printf("xkcd: wrote %d bytes\n", n2)

	return s.flushFile(ctx, file)
}

func (pqcs *parquetCloudStorageSink) EncodeAndEmitRow(
	ctx context.Context,
	updatedRow cdcevent.Row,
	topic TopicDescriptor,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	// cacheKey := parquetWriterKey{
	// 	updatedRow.FamilyID,
	// }

	s := pqcs.baseCloudStorageSink
	// var pqww *parquetWriterWrapper
	file, err := s.getOrCreateFile(topic, mvcc)
	if err != nil {
		return err
	}
	pqcs.flushFile(ctx, file)

	// if file.pqww == nil {
	// 	var err error
	// 	pqww, err = makeparquetWriterWrapper(ctx, updatedRow, &file.buf)
	// 	if err != nil {
	// 		return err
	// 	}

	// 	file.pqww = pqww
	// } else {
	// 	pqww = file.pqww
	// }

	// i := 0
	// parquetRow := make(map[string]interface{}, pqww.numCols)
	// // Revisit: I am assuming that this iterates through the columns in
	// // the same order as when iterating through the columns when
	// // creating the schema. this is important because each encode
	// // function is dependent on the column position.
	// updatedRow.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
	// 	// Revisit: should probably wrap these errors with something
	// 	// more meaningful for upstream
	// 	encodeFn, err := pqww.parquetColumns[i].GetEncoder()
	// 	if err != nil {
	// 		return err
	// 	}
	// 	i++
	// 	edNative, err := encodeFn(eval.UnwrapDatum(nil, d))
	// 	if err != nil {
	// 		return err
	// 	}
	// 	colName, err := pqww.parquetColumns[i].GetColName()
	// 	if err != nil {
	// 		return err
	// 	}

	// 	parquetRow[colName] = edNative

	// 	return nil

	// })

	// pqww.parquetWriter.AddData(parquetRow)

	// file.alloc.Merge(&alloc)
	// if pqww.parquetWriter.CurrentRowGroupSize() > s.targetMaxFileSize {
	// 	s.metrics.recordSizeBasedFlush()

	// 	pqww.parquetWriter.Close()
	// 	if err := s.flushTopicVersions(ctx, file.topic, file.schemaID); err != nil {
	// 		return err
	// 	}
	// }

	return nil

}

func makeparquetWriterWrapper(ctx context.Context, row cdcevent.Row, buf *bytes.Buffer) (*parquetWriterWrapper, error) {

	parquetColumns, err := getParquetColumnTypes(ctx, row)
	if err != nil {
		return nil, err
	}

	schema := pqexporter.NewParquetSchema(parquetColumns)

	// Revisit: not using parquets inbuilt compressor, relying on sinks
	// compression
	pqw := goparquet.NewFileWriter(buf,
		goparquet.WithSchemaDefinition(schema),
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
