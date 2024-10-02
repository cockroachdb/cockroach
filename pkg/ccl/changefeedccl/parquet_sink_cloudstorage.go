// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/kvevent"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/parquet"
	"github.com/cockroachdb/errors"
)

// This is an extra column that will be added to every parquet file which tells
// us about the type of event that generated a particular row. The types are
// defined below.
const parquetCrdbEventTypeColName string = metaSentinel + "event_type"

type parquetEventType int

const (
	parquetEventInsert parquetEventType = iota
	parquetEventUpdate
	parquetEventDelete
)

var parquetEventTypeDatumStringMap = map[parquetEventType]*tree.DString{
	parquetEventInsert: tree.NewDString("c"),
	parquetEventUpdate: tree.NewDString("u"),
	parquetEventDelete: tree.NewDString("d"),
}

func (e parquetEventType) DString() *tree.DString {
	return parquetEventTypeDatumStringMap[e]
}

// We need a separate sink for parquet format because the parquet encoder has to
// write metadata to the parquet file (buffer) after each flush. This means that
// the parquet encoder should have access to the buffer object inside
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
	everyN      log.EveryN
}

func makeParquetCloudStorageSink(
	baseCloudStorageSink *cloudStorageSink,
) (*parquetCloudStorageSink, error) {
	parquetSink := &parquetCloudStorageSink{
		wrapped:     baseCloudStorageSink,
		compression: parquet.CompressionNone,
		everyN:      log.Every(5 * time.Second),
	}
	if baseCloudStorageSink.compression.enabled() {
		switch baseCloudStorageSink.compression {
		case sinkCompressionGzip:
			parquetSink.compression = parquet.CompressionGZIP
		case sinkCompressionZstd:
			parquetSink.compression = parquet.CompressionZSTD
		default:
			return nil, errors.AssertionFailedf(
				"unexpected compression codec %s", baseCloudStorageSink.compression,
			)
		}
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
	return errors.AssertionFailedf("EmitRow unimplemented by the parquet cloud storage sink")
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
	ctx context.Context, _ Encoder, resolved hlc.Timestamp,
) (err error) {
	// TODO: There should be a better way to check if the sink is closed.
	// This is copied from the wrapped sink's EmitResolvedTimestamp()
	// method.
	if parquetSink.wrapped.files == nil {
		return errors.New(`cannot EmitRow on a closed sink`)
	}

	defer parquetSink.wrapped.metrics.recordResolvedCallback()()

	if err := parquetSink.wrapped.waitAsyncFlush(ctx); err != nil {
		return errors.Wrapf(err, "while emitting resolved timestamp")
	}

	var buf bytes.Buffer
	sch, err := parquet.NewSchema([]string{metaSentinel + "resolved"}, []*types.T{types.Decimal})
	if err != nil {
		return err
	}

	// TODO: Ideally, we do not create a new schema and writer every time
	// we emit a resolved timestamp. Currently, util/parquet does not support it.
	writer, err := parquet.NewWriter(sch, &buf)
	if err != nil {
		return err
	}

	if err := writer.AddRow([]tree.Datum{eval.TimestampToDecimalDatum(resolved)}); err != nil {
		return err
	}

	if err := writer.Close(); err != nil {
		return err
	}

	part := resolved.GoTime().Format(parquetSink.wrapped.partitionFormat)
	filename := fmt.Sprintf(`%s.RESOLVED`, cloudStorageFormatTime(resolved))
	if log.V(1) {
		log.Infof(ctx, "writing file %s %s", filename, resolved.AsOfSystemTime())
	}
	return cloud.WriteFile(ctx, parquetSink.wrapped.es, filepath.Join(part, filename), &buf)
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
	encodingOpts changefeedbase.EncodingOptions,
	alloc kvevent.Alloc,
) error {
	s := parquetSink.wrapped
	file, err := s.getOrCreateFile(topic, mvcc)
	if err != nil {
		return err
	}
	file.mergeAlloc(&alloc)

	if file.parquetCodec == nil {
		var err error
		file.parquetCodec, err = newParquetWriterFromRow(
			updatedRow, &file.buf, encodingOpts,
			parquet.WithCompressionCodec(parquetSink.compression))
		if err != nil {
			return err
		}
	}

	if err := file.parquetCodec.addData(updatedRow, prevRow, updated, mvcc); err != nil {
		return err
	}
	file.numMessages += 1

	// The parquet codec itself buffers data in an uncompressed form. When we
	// call flush(), it compresses the data and writes it to the file buffer.
	// We want to continuously flush data into file buffer we compare
	// compressed data with s.targetMaxFileSize instead of uncompressed data.
	//
	// Flushing every 1MB below is done because 1MB is big enough that the
	// parquet writer can compress more bits into a small size. 1MB is small
	// enough that we won't overshoot s.targetMaxFileSize by an excessive amount.
	if file.parquetCodec.estimatedBufferedBytes() > 1<<20 {
		if err := file.parquetCodec.flush(); err != nil {
			return err
		}
	}
	bufferedBytesEstimate := file.parquetCodec.estimatedBufferedBytes()

	// The size of the alloc associated with all the rows in the file should be
	// the number of bytes in the file and the number of buffered bytes.
	prevAllocSize := file.alloc.Bytes()
	newAllocSize := int64(file.buf.Len()) + bufferedBytesEstimate
	file.adjustBytesToTarget(ctx, newAllocSize)

	if log.V(1) && parquetSink.everyN.ShouldLog() {
		log.Infof(ctx, "topic: %d/%d, written: %d, buffered %d, new alloc: %d, old alloc: %d",
			topic.GetTopicIdentifier().TableID, topic.GetTopicIdentifier().FamilyID, int64(file.buf.Len()),
			bufferedBytesEstimate, prevAllocSize, newAllocSize)
	}

	if int64(file.buf.Len())+bufferedBytesEstimate > s.targetMaxFileSize {
		s.metrics.recordSizeBasedFlush()

		if err = file.parquetCodec.close(); err != nil {
			return err
		}
		if err := s.flushTopicVersions(ctx, file.topic, file.schemaID); err != nil {
			return err
		}
	}

	return nil
}

func getEventTypeDatum(updatedRow cdcevent.Row, prevRow cdcevent.Row) parquetEventType {
	if updatedRow.IsDeleted() {
		return parquetEventDelete
	} else if prevRow.IsInitialized() && !prevRow.IsDeleted() {
		return parquetEventUpdate
	}
	return parquetEventInsert
}
