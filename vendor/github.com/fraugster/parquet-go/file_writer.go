package goparquet

import (
	"context"
	"encoding/binary"
	"errors"
	"io"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
)

// FileWriter is used to write data to a parquet file. Always use NewFileWriter
// to create such an object.
type FileWriter struct {
	w writePos

	version int32
	//SchemaWriter

	schemaWriter *schema

	totalNumRecords int64
	kvStore         map[string]string
	createdBy       string

	rowGroupFlushSize int64

	rowGroups []*parquet.RowGroup

	codec parquet.CompressionCodec

	newPageFunc newDataPageFunc

	ctx context.Context

	schemaDef *parquetschema.SchemaDefinition
}

// FileWriterOption describes an option function that is applied to a FileWriter when it is created.
type FileWriterOption func(fw *FileWriter)

// NewFileWriter creates a new FileWriter. You can provide FileWriterOptions to influence the
// file writer's behaviour.
func NewFileWriter(w io.Writer, options ...FileWriterOption) *FileWriter {
	fw := &FileWriter{
		w: &writePosStruct{
			w:   w,
			pos: 0,
		},
		version:      1,
		schemaWriter: &schema{},
		kvStore:      make(map[string]string),
		rowGroups:    []*parquet.RowGroup{},
		createdBy:    "parquet-go",
		newPageFunc:  newDataPageV1Writer,
		ctx:          context.Background(),
	}

	for _, opt := range options {
		opt(fw)
	}

	// if a WithSchemaDefinition option was provided, the schema needs to be set after everything else
	// as other options can change settings on the schemaWriter (such as the maximum page size).
	if fw.schemaDef != nil {
		if err := fw.schemaWriter.SetSchemaDefinition(fw.schemaDef); err != nil {
			panic(err) // TODO: this shouldn't happen, but still isn't great. We messed up the API design for options and NewFileWriter.
		}
	}

	return fw
}

// FileVersion sets the version of the file itself.
func FileVersion(version int32) FileWriterOption {
	return func(fw *FileWriter) {
		fw.version = version
	}
}

// WithCreator sets the creator in the meta data of the file.
func WithCreator(createdBy string) FileWriterOption {
	return func(fw *FileWriter) {
		fw.createdBy = createdBy
	}
}

// WithCompressionCodec sets the compression codec used when writing the file.
func WithCompressionCodec(codec parquet.CompressionCodec) FileWriterOption {
	return func(fw *FileWriter) {
		fw.codec = codec
	}
}

// WithMetaData sets the key-value meta data on the file.
func WithMetaData(data map[string]string) FileWriterOption {
	return func(fw *FileWriter) {
		if data != nil {
			fw.kvStore = data
			return
		}
		fw.kvStore = make(map[string]string)
	}
}

// WithMaxRowGroupSize sets the rough maximum size of a row group before it shall
// be flushed automatically. Please note that enabling auto-flush will not allow
// you to set per-column-chunk meta-data upon calling FlushRowGroup. If you
// require this feature, you need to flush your rowgroups manually.
func WithMaxRowGroupSize(size int64) FileWriterOption {
	return func(fw *FileWriter) {
		fw.rowGroupFlushSize = size
	}
}

func WithMaxPageSize(size int64) FileWriterOption {
	return func(fw *FileWriter) {
		fw.schemaWriter.maxPageSize = size
	}
}

// WithSchemaDefinition sets the schema definition to use for this parquet file.
func WithSchemaDefinition(sd *parquetschema.SchemaDefinition) FileWriterOption {
	return func(fw *FileWriter) {
		fw.schemaDef = sd
	}
}

// WithDataPageV2 enables the writer to write pages in the new V2 format. By default,
// the library is using the V1 format. Please be aware that this may cause compatibility
// issues with older implementations of parquet.
func WithDataPageV2() FileWriterOption {
	return func(fw *FileWriter) {
		fw.newPageFunc = newDataPageV2Writer
	}
}

func WithCRC(enableCRC bool) FileWriterOption {
	return func(fw *FileWriter) {
		fw.schemaWriter.enableCRC = enableCRC
	}
}

// WithWriterContext overrides the default context (which is a context.Background())
// in the FileWriter with the provided context.Context object.
func WithWriterContext(ctx context.Context) FileWriterOption {
	return func(fw *FileWriter) {
		fw.ctx = ctx
	}
}

type flushRowGroupOptionHandle struct {
	cols   map[string]map[string]string
	global map[string]string
}

func newFlushRowGroupOptionHandle() *flushRowGroupOptionHandle {
	return &flushRowGroupOptionHandle{
		cols:   make(map[string]map[string]string),
		global: make(map[string]string),
	}
}

func (h *flushRowGroupOptionHandle) getMetaData(col string) map[string]string {
	data := make(map[string]string)

	for k, v := range h.global {
		data[k] = v
	}

	for k, v := range h.cols[col] {
		data[k] = v
	}

	if len(data) > 0 {
		return data
	}
	return nil
}

// FlushRowGroupOption is an option to pass additiona configuration to FlushRowGroup.
type FlushRowGroupOption func(h *flushRowGroupOptionHandle)

// WithRowGroupMetaDataForColumn adds key-value metadata to a particular column that is identified
// by its full dotted-notation name.
func WithRowGroupMetaDataForColumn(col string, kv map[string]string) FlushRowGroupOption {
	return func(h *flushRowGroupOptionHandle) {
		colKV := h.cols[col]
		if colKV == nil {
			colKV = make(map[string]string)
		}
		for k, v := range kv {
			colKV[k] = v
		}
		h.cols[col] = colKV
	}
}

// WithRowGroupMetaData adds key-value metadata to all columns. Please note that if you use the same
// key both in the meta data for all columns as well as in column-specific meta data
// (using MetaDataForColumn), the column-specific meta data has preference.
func WithRowGroupMetaData(kv map[string]string) FlushRowGroupOption {
	return func(h *flushRowGroupOptionHandle) {
		for k, v := range kv {
			h.global[k] = v
		}
	}
}

// FlushRowGroup writes the current row group to the parquet file.
func (fw *FileWriter) FlushRowGroup(opts ...FlushRowGroupOption) error {
	return fw.FlushRowGroupWithContext(fw.ctx, opts...)
}

// FlushRowGroupWithContext writes the current row group to the parquet file.
func (fw *FileWriter) FlushRowGroupWithContext(ctx context.Context, opts ...FlushRowGroupOption) error {
	// Write the entire row group
	if fw.schemaWriter.rowGroupNumRecords() == 0 {
		return errors.New("nothing to write")
	}

	if fw.w.Pos() == 0 {
		if err := writeFull(fw.w, magic); err != nil {
			return err
		}
	}

	h := newFlushRowGroupOptionHandle()

	for _, o := range opts {
		o(h)
	}

	cc, err := writeRowGroup(ctx, fw.w, fw.schemaWriter, fw.codec, fw.newPageFunc, h)
	if err != nil {
		return err
	}

	var totalCompressedSize, totalUncompressedSize int64

	for _, c := range cc {
		totalCompressedSize += c.MetaData.TotalCompressedSize
		totalUncompressedSize += c.MetaData.TotalUncompressedSize
	}

	fw.rowGroups = append(fw.rowGroups, &parquet.RowGroup{
		Columns:             cc,
		TotalByteSize:       totalUncompressedSize,
		TotalCompressedSize: &totalCompressedSize,
		NumRows:             fw.schemaWriter.rowGroupNumRecords(),
		SortingColumns:      nil,
	})
	fw.totalNumRecords += fw.schemaWriter.rowGroupNumRecords()
	// flush the schema
	fw.schemaWriter.resetData()

	return nil
}

// AddData adds a new record to the current row group and flushes it if auto-flush is enabled and the size
// is equal to or greater than the configured maximum row group size.
func (fw *FileWriter) AddData(m map[string]interface{}) error {
	if err := fw.schemaWriter.AddData(m); err != nil {
		return err
	}

	if fw.rowGroupFlushSize > 0 && fw.schemaWriter.DataSize() >= fw.rowGroupFlushSize {
		return fw.FlushRowGroup()
	}

	return nil
}

// Close flushes the current row group if necessary, taking the provided
// options into account, and writes the meta data footer to the file.
// Please be aware that this only finalizes the writing process. If you
// provided a file as io.Writer when creating the FileWriter, you still need
// to Close that file handle separately.
func (fw *FileWriter) Close(opts ...FlushRowGroupOption) error {
	return fw.CloseWithContext(fw.ctx, opts...)
}

// CloseWithContext flushes the current row group if necessary, taking the provided
// options into account, and writes the meta data footer to the file.
// Please be aware that this only finalizes the writing process. If you
// provided a file as io.Writer when creating the FileWriter, you still need
// to Close that file handle separately.
func (fw *FileWriter) CloseWithContext(ctx context.Context, opts ...FlushRowGroupOption) error {
	if len(fw.rowGroups) == 0 || fw.schemaWriter.rowGroupNumRecords() > 0 {
		if err := fw.FlushRowGroup(opts...); err != nil {
			return err
		}
	}

	kv := make([]*parquet.KeyValue, 0, len(fw.kvStore))
	for i := range fw.kvStore {
		v := fw.kvStore[i]
		addr := &v
		if v == "" {
			addr = nil
		}
		kv = append(kv, &parquet.KeyValue{
			Key:   i,
			Value: addr,
		})
	}
	meta := &parquet.FileMetaData{
		Version:          fw.version,
		Schema:           fw.schemaWriter.getSchemaArray(),
		NumRows:          fw.totalNumRecords,
		RowGroups:        fw.rowGroups,
		KeyValueMetadata: kv,
		CreatedBy:        &fw.createdBy,
		ColumnOrders:     nil,
	}

	pos := fw.w.Pos()
	if err := writeThrift(ctx, meta, fw.w); err != nil {
		return err
	}

	ln := int32(fw.w.Pos() - pos)
	if err := binary.Write(fw.w, binary.LittleEndian, &ln); err != nil {
		return err
	}

	return writeFull(fw.w, magic)
}

// CurrentRowGroupSize returns a rough estimation of the uncompressed size of the current row group data. If you selected
// a compression format other than UNCOMPRESSED, the final size will most likely be smaller and will dpeend on how well
// your data can be compressed.
func (fw *FileWriter) CurrentRowGroupSize() int64 {
	return fw.schemaWriter.DataSize()
}

// CurrentFileSize returns the amount of data written to the file so far. This does not include data that is in the
// current row group and has not been flushed yet. After closing the file, the size will be even larger since the
// footer is appended to the file upon closing.
func (fw *FileWriter) CurrentFileSize() int64 {
	return fw.w.Pos()
}

// AddColumn adds a new data column to the schema of this file writer.
func (fw *FileWriter) AddColumn(path string, col *Column) error {
	return fw.schemaWriter.AddColumn(path, col)
}

// AddGroup adds a new group to the schema of this file writer.
func (fw *FileWriter) AddGroup(path string, rep parquet.FieldRepetitionType) error {
	return fw.schemaWriter.AddGroup(path, rep)
}

// GetSchemaDefinition returns the schema definition that has been set in this file writer.
func (fw *FileWriter) GetSchemaDefinition() *parquetschema.SchemaDefinition {
	return fw.schemaWriter.GetSchemaDefinition()
}
