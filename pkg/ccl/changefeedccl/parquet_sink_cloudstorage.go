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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// This variable controls whether we add primary keys of the table to the
// metadata of the parquet file. Currently, this will be true only under
// testing.
// TODO(cdc): We should consider including this metadata during production also
var includeParquetTestMetadata = false

// This is an extra column that will be added to every parquet file which tells
// us about the type of event that generated a particular row. The types are
// defined below.
const parquetCrdbEventTypeColName string = "__crdb_event_type__"

const (
	parquetEventInsert string = "c"
	parquetEventUpdate string = "u"
	parquetEventDelete string = "d"
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
	wrapped     *cloudStorageSink
	compression parquet.CompressionCodec
}

type parquetFileWriter struct {
	parquetWriter  *goparquet.FileWriter
	schema         *parquetschema.SchemaDefinition
	parquetColumns []pqexporter.ParquetColumn
	numCols        int
}

func makeParquetCloudStorageSink(
	baseCloudStorageSink *cloudStorageSink,
) (*parquetCloudStorageSink, error) {
	parquetSink := &parquetCloudStorageSink{wrapped: baseCloudStorageSink}
	if !baseCloudStorageSink.compression.enabled() {
		parquetSink.compression = parquet.CompressionCodec_UNCOMPRESSED
	} else if baseCloudStorageSink.compression == sinkCompressionGzip {
		parquetSink.compression = parquet.CompressionCodec_GZIP
	} else {
		return nil, errors.AssertionFailedf("Specified compression not supported with parquet")
	}
	return parquetSink, nil
}

// getConcreteType implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) getConcreteType() sinkType {
	return parquetSink.wrapped.getConcreteType()
}

// EmitRow does not do anything. It must not be called. It is present so that
// parquetCloudStorageSink implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) EmitRow(
	ctx context.Context,
	topic TopicDescriptor,
	key, value []byte,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	return errors.AssertionFailedf("Emit Row should not be called for parquet format")
}

// Close implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) Close() error {
	return parquetSink.wrapped.Close()
}

// Dial implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) Dial() error {
	return parquetSink.wrapped.Dial()
}

// EmitResolvedTimestamp does not do anything as of now. It is there to
// implement Sink interface.
func (parquetSink *parquetCloudStorageSink) EmitResolvedTimestamp(
	ctx context.Context, encoder Encoder, resolved hlc.Timestamp,
) error {
	return errors.AssertionFailedf("Parquet format does not support emitting resolved timestamp")
}

// Flush implements the Sink interface.
func (parquetSink *parquetCloudStorageSink) Flush(ctx context.Context) error {
	return parquetSink.wrapped.Flush(ctx)
}

// EncodeAndEmitRow links the buffer in the cloud storage sync file and the
// parquet writer (see parquetCloudStorageSink). It also takes care of encoding
// and emitting row event to cloud storage. Implements the SinkWithEncoder
// interface.
func (parquetSink *parquetCloudStorageSink) EncodeAndEmitRow(
	ctx context.Context,
	updatedRow cdcevent.Row,
	prevRow cdcevent.Row,
	topic TopicDescriptor,
	updated, mvcc hlc.Timestamp,
	alloc kvevent.Alloc,
) error {
	s := parquetSink.wrapped
	file, err := s.getOrCreateFile(topic, mvcc)
	if err != nil {
		return err
	}
	file.alloc.Merge(&alloc)

	if file.parquetCodec == nil {
		var err error
		file.parquetCodec, err = makeParquetWriterWrapper(ctx, updatedRow, &file.buf, parquetSink.compression)
		if err != nil {
			return err
		}
	}

	colOrd := -1
	// TODO (ganeshb): Avoid map allocation on every call to emit row
	parquetRow := make(map[string]interface{}, file.parquetCodec.numCols)
	if err := updatedRow.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		colOrd++
		// Omit NULL columns from parquet row
		if d == tree.DNull {
			parquetRow[col.Name] = nil
			return nil
		}
		encodeFn, err := file.parquetCodec.parquetColumns[colOrd].GetEncoder()
		if err != nil {
			return err
		}
		edNative, err := encodeFn(d)
		if err != nil {
			return err
		}

		parquetRow[col.Name] = edNative

		return nil

	}); err != nil {
		return err
	}

	if updatedRow.IsDeleted() {
		parquetRow[parquetCrdbEventTypeColName] = []byte(parquetEventDelete)
	} else if prevRow.IsInitialized() && !prevRow.IsDeleted() {
		parquetRow[parquetCrdbEventTypeColName] = []byte(parquetEventUpdate)
	} else {
		parquetRow[parquetCrdbEventTypeColName] = []byte(parquetEventInsert)
	}

	if err = file.parquetCodec.parquetWriter.AddData(parquetRow); err != nil {
		return err
	}

	if file.parquetCodec.parquetWriter.CurrentRowGroupSize() > s.targetMaxFileSize {
		s.metrics.recordSizeBasedFlush()

		if err = file.parquetCodec.parquetWriter.Close(); err != nil {
			return err
		}
		if err := s.flushTopicVersions(ctx, file.topic, file.schemaID); err != nil {
			return err
		}
	}

	return nil
}

func makeParquetWriterWrapper(
	ctx context.Context, row cdcevent.Row, buf *bytes.Buffer, compression parquet.CompressionCodec,
) (*parquetFileWriter, error) {
	parquetColumns, err := getParquetColumnTypes(ctx, row)
	if err != nil {
		return nil, err
	}

	schema := pqexporter.NewParquetSchema(parquetColumns)

	parquetWriterOptions := make([]goparquet.FileWriterOption, 0)

	// TODO(cdc): We really should revisit if we should include any metadata in
	// parquet files. There are plenty things we can include there, including crdb
	// native column types, OIDs for those column types, etc
	if includeParquetTestMetadata {
		metadata, err := getMetadataForParquetFile(ctx, row)
		if err != nil {
			return nil, err
		}
		parquetWriterOptions = append(parquetWriterOptions, goparquet.WithMetaData(metadata))
	}

	// TODO(cdc): Determine if we should parquet's builtin compressor or rely on
	// sinks compressing. Currently using not parquets builtin compressor, relying
	// on sinks compression
	parquetWriterOptions = append(parquetWriterOptions, goparquet.WithSchemaDefinition(schema))
	parquetWriterOptions = append(parquetWriterOptions, goparquet.WithCompressionCodec(compression))
	pqw := goparquet.NewFileWriter(buf,
		parquetWriterOptions...,
	)

	pqww := &parquetFileWriter{
		pqw,
		schema,
		parquetColumns,
		len(parquetColumns),
	}
	return pqww, nil
}

func getMetadataForParquetFile(ctx context.Context, row cdcevent.Row) (map[string]string, error) {
	metadata := make(map[string]string)
	primaryKeyColNames := ""
	columnNames := ""
	if err := row.ForEachKeyColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		primaryKeyColNames += col.Name + ","
		return nil
	}); err != nil {
		return nil, err
	}
	metadata["primaryKeyNames"] = primaryKeyColNames
	if err := row.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		columnNames += col.Name + ","
		return nil
	}); err != nil {
		return nil, err
	}
	metadata["columnNames"] = columnNames
	return metadata, nil
}

func getParquetColumnTypes(
	ctx context.Context, row cdcevent.Row,
) ([]pqexporter.ParquetColumn, error) {
	typs := make([]*types.T, 0)
	names := make([]string, 0)

	if err := row.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		typs = append(typs, col.Typ)
		names = append(names, col.Name)
		return nil
	}); err != nil {
		return nil, err
	}

	parquetColumns := make([]pqexporter.ParquetColumn, len(typs)+1)
	const nullable = true

	for i := 0; i < len(typs); i++ {
		// Make every field optional, so that all schema evolutions for a table are
		// considered "backward compatible" by parquet. This means that the parquet
		// type doesn't mirror the column's nullability, but it makes it much easier
		// to work with long histories of table data afterward, especially for
		// things like loading into analytics databases.
		parquetCol, err := pqexporter.NewParquetColumn(typs[i], names[i], nullable)
		if err != nil {
			return nil, err
		}
		parquetColumns[i] = parquetCol
	}

	// Add the extra column which will store the type of event that generated that
	// particular row.
	var err error
	parquetColumns[len(typs)], err = pqexporter.NewParquetColumn(types.String, parquetCrdbEventTypeColName, false)
	if err != nil {
		return nil, err
	}

	return parquetColumns, nil
}
