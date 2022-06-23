package goparquet

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"github.com/pkg/errors"
)

// FileReader is used to read data from a parquet file. Always use NewFileReader or a related
// function  to create such an object.
type FileReader struct {
	meta         *parquet.FileMetaData
	schemaReader *schema
	reader       io.ReadSeeker

	rowGroupPosition int
	currentRecord    int64
	skipRowGroup     bool

	ctx context.Context
}

// NewFileReaderWithOptions creates a new FileReader. You can provide a list of FileReaderOptions to configure
// aspects of its behaviour, such as limiting the columns to read, the file metadata to use, or the
// context to use. For a full list of options, please see the type FileReaderOption.
func NewFileReaderWithOptions(r io.ReadSeeker, readerOptions ...FileReaderOption) (*FileReader, error) {
	opts := newFileReaderOptions()
	if err := opts.apply(readerOptions); err != nil {
		return nil, err
	}

	var err error
	if opts.metaData == nil {
		opts.metaData, err = ReadFileMetaData(r, true)
		if err != nil {
			return nil, errors.Wrap(err, "reading file meta data failed")
		}
	}

	schema, err := makeSchema(opts.metaData, opts.validateCRC)
	if err != nil {
		return nil, errors.Wrap(err, "creating schema failed")
	}

	schema.SetSelectedColumns(opts.columns...)
	// Reset the reader to the beginning of the file
	if _, err := r.Seek(4, io.SeekStart); err != nil {
		return nil, err
	}
	return &FileReader{
		meta:         opts.metaData,
		schemaReader: schema,
		reader:       r,
		ctx:          opts.ctx,
	}, nil
}

// FileReaderOption is an option that can be passed on to NewFileReaderWithOptions when
// creating a new parquet file reader.
type FileReaderOption func(*fileReaderOptions) error
type fileReaderOptions struct {
	metaData    *parquet.FileMetaData
	ctx         context.Context
	columns     []string
	validateCRC bool
}

func newFileReaderOptions() *fileReaderOptions {
	return &fileReaderOptions{ctx: context.Background()}
}

func (o *fileReaderOptions) apply(opts []FileReaderOption) error {
	for _, f := range opts {
		if err := f(o); err != nil {
			return err
		}
	}
	return nil
}

// WithReaderContext configures a custom context for the file reader. If none
// is provided, context.Background() is used as a default.
func WithReaderContext(ctx context.Context) FileReaderOption {
	return func(opts *fileReaderOptions) error {
		opts.ctx = ctx
		return nil
	}
}

// WithFileMetaData allows you to provide your own file metadata. If none
// is set with this option, the file reader will read it from the parquet
// file.
func WithFileMetaData(metaData *parquet.FileMetaData) FileReaderOption {
	return func(opts *fileReaderOptions) error {
		opts.metaData = metaData
		return nil
	}
}

// WithColumns limits the columns which are read. If none are set, then
// all columns will be read by the parquet file reader.
func WithColumns(columns ...string) FileReaderOption {
	return func(opts *fileReaderOptions) error {
		opts.columns = columns
		return nil
	}
}

// WithCRC32Validation allows you to configure whether CRC32 page checksums will
// be validated when they're read. By default, checksum validation is disabled.
func WithCRC32Validation(enable bool) FileReaderOption {
	return func(opts *fileReaderOptions) error {
		opts.validateCRC = enable
		return nil
	}
}

// NewFileReader creates a new FileReader. You can limit the columns that are read by providing
// the names of the specific columns to read using dotted notation. If no columns are provided,
// then all columns are read.
func NewFileReader(r io.ReadSeeker, columns ...string) (*FileReader, error) {
	return NewFileReaderWithOptions(r, WithColumns(columns...))
}

// NewFileReaderWithContext creates a new FileReader. You can limit the columns that are read by providing
// the names of the specific columns to read using dotted notation. If no columns are provided,
// then all columns are read. The provided context.Context overrides the default context (which is a context.Background())
// for use in other methods of the *FileReader type.
//
// Deprecated: use the function NewFileReaderWithOptions and the option WithContext instead.
func NewFileReaderWithContext(ctx context.Context, r io.ReadSeeker, columns ...string) (*FileReader, error) {
	return NewFileReaderWithOptions(r, WithReaderContext(ctx), WithColumns(columns...))
}

// NewFileReaderWithMetaData creates a new FileReader with custom file meta data. You can limit the columns that
// are read by providing the names of the specific columns to read using dotted notation. If no columns are provided,
// then all columns are read.
//
// Deprecated: use the function NewFileReaderWithOptions and the option WithFileMetaData instead.
func NewFileReaderWithMetaData(r io.ReadSeeker, meta *parquet.FileMetaData, columns ...string) (*FileReader, error) {
	return NewFileReaderWithOptions(r, WithFileMetaData(meta), WithColumns(columns...))
}

// SeekToRowGroup seeks to a particular row group, identified by its index.
func (f *FileReader) SeekToRowGroup(rowGroupPosition int) error {
	return f.SeekToRowGroupWithContext(f.ctx, rowGroupPosition)
}

// SeekToRowGroupWithContext seeks to a particular row group, identified by its index.
func (f *FileReader) SeekToRowGroupWithContext(ctx context.Context, rowGroupPosition int) error {
	f.rowGroupPosition = rowGroupPosition - 1
	f.currentRecord = 0
	return f.readRowGroup(ctx)
}

// readRowGroup read the next row group into memory
func (f *FileReader) readRowGroup(ctx context.Context) error {
	if len(f.meta.RowGroups) <= f.rowGroupPosition {
		return io.EOF
	}
	f.rowGroupPosition++
	return readRowGroup(ctx, f.reader, f.schemaReader, f.meta.RowGroups[f.rowGroupPosition-1])
}

// CurrentRowGroup returns information about the current row group.
func (f *FileReader) CurrentRowGroup() *parquet.RowGroup {
	if f == nil || f.meta == nil || f.meta.RowGroups == nil || f.rowGroupPosition-1 >= len(f.meta.RowGroups) {
		return nil
	}
	return f.meta.RowGroups[f.rowGroupPosition-1]
}

// RowGroupCount returns the number of row groups in the parquet file.
func (f *FileReader) RowGroupCount() int {
	return len(f.meta.RowGroups)
}

// NumRows returns the number of rows in the parquet file. This information is directly taken from
// the file's meta data.
func (f *FileReader) NumRows() int64 {
	return f.meta.NumRows
}

func (f *FileReader) advanceIfNeeded(ctx context.Context) error {
	if f.rowGroupPosition == 0 || f.currentRecord >= f.schemaReader.rowGroupNumRecords() || f.skipRowGroup {
		if err := f.readRowGroup(ctx); err != nil {
			f.skipRowGroup = true
			return err
		}
		f.currentRecord = 0
		f.skipRowGroup = false
	}

	return nil
}

// RowGroupNumRows returns the number of rows in the current RowGroup.
func (f *FileReader) RowGroupNumRows() (int64, error) {
	return f.RowGroupNumRowsWithContext(f.ctx)
}

// RowGroupNumRowsWithContext returns the number of rows in the current RowGroup.
func (f *FileReader) RowGroupNumRowsWithContext(ctx context.Context) (int64, error) {
	if err := f.advanceIfNeeded(ctx); err != nil {
		return 0, err
	}

	return f.schemaReader.rowGroupNumRecords(), nil
}

// NextRow reads the next row from the parquet file. If required, it will load the next row group.
func (f *FileReader) NextRow() (map[string]interface{}, error) {
	return f.NextRowWithContext(f.ctx)
}

// NextRowWithContext reads the next row from the parquet file. If required, it will load the next row group.
func (f *FileReader) NextRowWithContext(ctx context.Context) (map[string]interface{}, error) {
	if err := f.advanceIfNeeded(ctx); err != nil {
		return nil, err
	}

	f.currentRecord++
	return f.schemaReader.getData()
}

// SkipRowGroup skips the currently loaded row group and advances to the next row group.
func (f *FileReader) SkipRowGroup() {
	f.skipRowGroup = true
}

// PreLoad is used to load the row group if required. It does nothing if the row group is already loaded.
func (f *FileReader) PreLoad() error {
	return f.PreLoadWithContext(f.ctx)
}

// PreLoadWithContext is used to load the row group if required. It does nothing if the row group is already loaded.
func (f *FileReader) PreLoadWithContext(ctx context.Context) error {
	return f.advanceIfNeeded(ctx)
}

// MetaData returns a map of metadata key-value pairs stored in the parquet file.
func (f *FileReader) MetaData() map[string]string {
	return keyValueMetaDataToMap(f.meta.KeyValueMetadata)
}

// ColumnMetaData returns a map of metadata key-value pairs for the provided column in the current
// row group. The column name has to be provided in its dotted notation.
func (f *FileReader) ColumnMetaData(colName string) (map[string]string, error) {
	for _, col := range f.CurrentRowGroup().Columns {
		if colName == strings.Join(col.MetaData.PathInSchema, ".") {
			return keyValueMetaDataToMap(col.MetaData.KeyValueMetadata), nil
		}
	}
	return nil, fmt.Errorf("column %q not found", colName)
}

// Columns returns the list of columns.
func (f *FileReader) Columns() []*Column {
	return f.schemaReader.Columns()
}

// GetSchemaDefinition returns the current schema definition.
func (f *FileReader) GetSchemaDefinition() *parquetschema.SchemaDefinition {
	return f.schemaReader.GetSchemaDefinition()
}

func keyValueMetaDataToMap(kvMetaData []*parquet.KeyValue) map[string]string {
	data := make(map[string]string)
	for _, kv := range kvMetaData {
		if kv.Value != nil {
			data[kv.Key] = *kv.Value
		}
	}
	return data
}
