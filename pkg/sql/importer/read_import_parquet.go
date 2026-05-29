// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type parquetInputReader struct {
	importCtx *parallelImportContext
	opts      roachpb.ParquetOptions
}

// newParquetInputReader creates a new Parquet input converter.
func newParquetInputReader(
	semaCtx *tree.SemaContext,
	kvCh chan row.KVBatch,
	walltime int64,
	parallelism int,
	tableDesc catalog.TableDescriptor,
	targetCols tree.NameList,
	evalCtx *eval.Context,
	seqChunkProvider *row.SeqChunkProvider,
	db *kv.DB,
	parquetOpts roachpb.ParquetOptions,
) (*parquetInputReader, error) {
	// Setup parallel import context (same as other importers)
	importCtx := &parallelImportContext{
		walltime:         walltime,
		numWorkers:       parallelism,
		batchSize:        parallelImporterReaderBatchSize,
		evalCtx:          evalCtx,
		semaCtx:          semaCtx,
		tableDesc:        tableDesc,
		targetCols:       targetCols,
		kvCh:             kvCh,
		seqChunkProvider: seqChunkProvider,
		db:               db,
	}

	return &parquetInputReader{
		importCtx: importCtx,
		opts:      parquetOpts,
	}, nil
}

var _ inputConverter = &parquetInputReader{}

// readFiles implements the inputConverter interface.
func (p *parquetInputReader) readFiles(
	ctx context.Context,
	dataFiles map[int32]string,
	resumePos map[int32]int64,
	format roachpb.IOFileFormat,
	makeExternalStorage cloud.ExternalStorageFactory,
	user username.SQLUsername,
) error {
	return readInputFiles(ctx, dataFiles, resumePos, format, p.readFile,
		makeExternalStorage, user)
}

// readFile processes a single Parquet file.
func (p *parquetInputReader) readFile(
	ctx context.Context, input *fileReader, inputIdx int32, resumePos int64, rejected chan string,
) error {
	// Create file context
	fileCtx := &importFileContext{
		source:   inputIdx,
		skip:     resumePos,
		rejected: rejected,
	}

	// Create row producer - it will open the file and determine which columns to read
	producer, err := newParquetRowProducer(input, p.importCtx)
	if err != nil {
		return err
	}

	// Create row consumer with schema mapping
	consumer, err := newParquetRowConsumer(p.importCtx, producer, fileCtx, p.opts.StrictMode)
	if err != nil {
		return err
	}

	// Process rows using the standard parallel import pipeline
	return runParallelImport(ctx, p.importCtx, fileCtx, producer, consumer)
}

// Default batch size for reading Parquet rows.
// TODO (silvano): tune this based on experiment, possibly exposing as an option if needed.
const defaultParquetBatchSize = 100

// parquetRowView is a lightweight view into a row of columnar data.
// Instead of allocating a new []any for each row, we return this view
// which allows the consumer to read typed values directly from column batches.
//
// The view captures references to the current batches to remain valid even if
// the producer refills its buffer.
//
// Example usage:
//
//	view := producer.Row()
//	for colIdx, batch := range view.batches {
//	  if batch.isNull[view.rowIndex] {
//	    // Handle NULL
//	  } else {
//	    // Access typed value, e.g., batch.int32Values[view.rowIndex]
//	  }
//	}
type parquetRowView struct {
	batches    []*parquetColumnBatch // Snapshot of column batches
	numColumns int                   // Total number of columns
	rowIndex   int64                 // Index within the batches
}

// parquetRowProducer implements importRowProducer for Parquet files.
// It reads columnar data in batches and provides row-oriented access via views.
type parquetRowProducer struct {
	reader *file.Reader // Apache Arrow Parquet file reader

	// Row group tracking
	currentRowGroup   int   // Which row group we're currently reading
	totalRowGroups    int   // Total number of row groups in file
	rowsInGroup       int64 // Number of rows in current row group
	currentRowInGroup int64 // Current position within row group

	// Column selection - which Parquet columns to actually read
	columnsToRead     []int                            // Parquet column indices to read
	numColumns        int                              // Total number of columns in Parquet file
	columnReaders     map[int]file.ColumnChunkReader   // Maps Parquet column index -> reader
	listColumnReaders map[int]*parquetListColumnReader // Stateful readers for LIST columns (per row group)

	// Cached schema metadata (populated once per file, used for all batches)
	columnMetadata map[int]*parquetColumnMetadata // Maps Parquet column index -> metadata

	// Batching configuration
	batchSize int64 // Number of rows to read in a single batch

	// Reusable buffers for reading Parquet values
	// Allocated once per row group to avoid repeated allocations
	buffers *parquetBatchBuffers

	// Column batches - stores typed data for each column (sparse array)
	// Only columns in columnsToRead will have non-nil batches
	columnBatches      []*parquetColumnBatch // [column index]
	bufferedRowCount   int64                 // Number of rows currently buffered
	currentBufferedRow int64                 // Which buffered row to return next

	// Progress tracking
	totalRows     int64 // Total rows across all row groups
	rowsProcessed int64 // Rows processed so far

	err error
}

// deriveLogicalType returns the LogicalType for a Parquet column.
// For legacy files with ConvertedType but no LogicalType, it converts the
// ConvertedType to LogicalType using Apache Arrow's ToLogicalType() method.
// This allows a single conversion code path for both modern and legacy files.
func deriveLogicalType(col *schema.Column) schema.LogicalType {
	logicalType := col.LogicalType()
	convertedType := col.ConvertedType()

	// If no LogicalType (legacy file), derive it from ConvertedType
	if logicalType == nil && convertedType != schema.ConvertedTypes.None {
		// Get decimal metadata for proper decimal conversion
		decimalMeta := schema.DecimalMetadata{}
		if node, ok := col.SchemaNode().(*schema.PrimitiveNode); ok {
			decimalMeta = node.DecimalMetadata()
		}
		logicalType = convertedType.ToLogicalType(decimalMeta)
	}

	return logicalType
}

// newParquetRowProducer creates a new Parquet row producer from a fileReader.
// It opens the Parquet file and determines which columns to read based on importCtx.
// If importCtx is nil, all columns are read (useful for tests).
//
// Thread Safety Note:
// Apache Arrow's Parquet reader uses ReadAt to read column chunks, potentially from
// multiple goroutines. This is safe because fileReader's underlying ResumingReader
// supports concurrent ReadAt calls. See fileReader's documentation for details.
func newParquetRowProducer(
	input *fileReader, importCtx *parallelImportContext,
) (*parquetRowProducer, error) {

	// Open Parquet file
	reader, err := file.NewParquetReader(input)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open Parquet file")
	}

	// Count total rows across all row groups for progress tracking
	totalRows := int64(0)
	totalRowGroups := reader.NumRowGroups()
	for i := range totalRowGroups {
		totalRows += reader.RowGroup(i).NumRows()
	}

	numColumns := reader.MetaData().Schema.NumColumns()
	batchSize := int64(defaultParquetBatchSize)
	parquetSchema := reader.MetaData().Schema

	// Build columnMetadata for every leaf before deciding which leaves to
	// read. A Parquet LIST<T> column is encoded as a 3-level nested group:
	//
	//   tags        (group, LIST-annotated)   <- the name the user wrote
	//     list      (repeated group)
	//       element (leaf, the actual T data) <- what Schema.Column(i) returns
	//
	// The Apache Arrow Parquet API only exposes leaves through
	// Schema.Column(i) and Schema.NumColumns(), so the LIST column at leaf
	// index i shows up as "element", not "tags". determineColumnsToRead
	// matches Parquet column names against the IMPORT target list (e.g.
	// "tags"), so it needs the group-node name, not the leaf name, or it
	// would silently skip every LIST column.
	//
	// parquetSchema.ColumnRoot(i) returns the top-level group/leaf node
	// (the user-visible name) for any leaf — flat, LIST, MAP, or otherwise —
	// so we use it to populate columnMetadata[i].columnName unconditionally.
	//
	// detectListColumn errors (for unsupported nested structures like
	// LIST<LIST<T>> or MAP) are stored in columnMetadata[i].detectionErr
	// and only surfaced later if the user actually imports that column.
	// Deferring lets a file with unsupported columns still open and be
	// partially imported, as long as the user does not target those
	// columns.
	columnMetadata := make(map[int]*parquetColumnMetadata)
	for colIdx := range numColumns {
		col := parquetSchema.Column(colIdx)
		name := parquetSchema.ColumnRoot(colIdx).Name()

		listInfo, err := detectListColumn(parquetSchema, colIdx)
		if err != nil {
			columnMetadata[colIdx] = &parquetColumnMetadata{
				columnName:   name,
				detectionErr: err,
			}
			continue
		}

		if listInfo != nil {
			columnMetadata[colIdx] = &parquetColumnMetadata{
				columnName:    name,
				logicalType:   listInfo.elementLogicalType,
				convertedType: listInfo.elementConvertedType,
				isList:        true,
				listInfo:      listInfo,
			}
		} else {
			columnMetadata[colIdx] = &parquetColumnMetadata{
				columnName:    name,
				logicalType:   deriveLogicalType(col),
				convertedType: col.ConvertedType(),
			}
		}
	}

	// Determine which columns to read.
	var columnsToRead []int
	if importCtx == nil {
		// No import context (test mode) - read all columns.
		columnsToRead = make([]int, numColumns)
		for i := range numColumns {
			columnsToRead[i] = i
		}
	} else {
		columnsToRead, err = determineColumnsToRead(importCtx, parquetSchema, columnMetadata)
		if err != nil {
			return nil, err
		}
	}

	return &parquetRowProducer{
		reader:          reader,
		currentRowGroup: -1, // Start at -1 so first advance goes to 0
		totalRowGroups:  totalRowGroups,
		columnsToRead:   columnsToRead,
		numColumns:      numColumns,
		columnMetadata:  columnMetadata,
		totalRows:       totalRows,
		rowsProcessed:   0,
		batchSize:       batchSize,
		buffers:         newParquetBatchBuffers(batchSize),

		// Initialize column batches (sparse array - only read columns will have batches)
		columnBatches:      make([]*parquetColumnBatch, numColumns),
		bufferedRowCount:   0,
		currentBufferedRow: 0,
	}, nil
}

// Scan checks if the next row is available, loading the next batch if necessary.
// Returns false when no more rows exist.
// This method transparently skips empty row groups (row groups with 0 rows).
func (p *parquetRowProducer) Scan() bool {
	if p.err != nil {
		return false
	}

	// Check if we have buffered rows available
	if p.currentBufferedRow < p.bufferedRowCount {
		return true
	}

	// Buffer is empty - need to refill from row groups
	// Loop to skip any empty row groups transparently
	for {
		// Check if we need to advance to next row group
		if p.columnReaders == nil || p.currentRowInGroup >= p.rowsInGroup {
			if err := p.advanceToNextRowGroup(); err != nil {
				p.err = err
				return false
			}
		}

		// Check if we've exhausted all row groups
		if p.currentRowGroup >= p.totalRowGroups {
			return false
		}

		// Check if current row group has rows (skip empty row groups)
		if p.rowsInGroup > 0 {
			// Fill buffer from current non-empty row group
			if err := p.fillBuffer(); err != nil {
				p.err = err
				return false
			}
			return true
		}

		// Current row group is empty - mark as exhausted and loop to advance
		// This ensures we skip empty row groups transparently
		p.currentRowInGroup = p.rowsInGroup
	}
}

// advanceToNextRowGroup sets up column readers for the next row group.
// Returns nil if we've moved past the last row group.
func (p *parquetRowProducer) advanceToNextRowGroup() error {
	// Move to next row group
	p.currentRowGroup++

	if p.currentRowGroup >= p.totalRowGroups {
		// Mark state as exhausted
		p.rowsInGroup = 0
		p.currentRowInGroup = 0
		p.columnReaders = nil
		p.listColumnReaders = nil
		return nil
	}

	// Get the row group reader
	rowGroup := p.reader.RowGroup(p.currentRowGroup)
	p.rowsInGroup = rowGroup.NumRows()
	p.currentRowInGroup = 0

	// Set up column chunk readers only for columns we need to read.
	// listColumnReaders is allocated lazily; most files have no LIST columns.
	p.columnReaders = make(map[int]file.ColumnChunkReader, len(p.columnsToRead))
	p.listColumnReaders = nil
	for _, colIdx := range p.columnsToRead {
		colReader, err := rowGroup.Column(colIdx)
		if err != nil {
			return errors.Wrapf(err, "column reader for column %d", colIdx)
		}
		p.columnReaders[colIdx] = colReader

		// Create stateful readers for LIST columns so that overflow
		// entries are preserved across fillBuffer calls.
		metadata := p.columnMetadata[colIdx]
		if metadata.isList {
			listReader, err := newParquetListColumnReader(colReader, metadata.listInfo)
			if err != nil {
				return errors.Wrapf(err, "creating LIST reader for column %d", colIdx)
			}
			if p.listColumnReaders == nil {
				p.listColumnReaders = make(map[int]*parquetListColumnReader)
			}
			p.listColumnReaders[colIdx] = listReader
		}
	}

	return nil
}

// fillBuffer reads up to batchSize rows from needed columns and stores them in columnBatches.
// This method reads N values from each column at once in typed form, avoiding boxing until needed.
// Only columns in columnsToRead are actually read; other columns remain nil.
// Precondition: currentRowInGroup < rowsInGroup (i.e., rows are available in current row group)
func (p *parquetRowProducer) fillBuffer() error {
	// Determine how many rows to read in this batch
	// (might be less than batchSize at end of row group)
	rowsRemaining := p.rowsInGroup - p.currentRowInGroup
	rowsToRead := p.batchSize
	if rowsToRead > rowsRemaining {
		rowsToRead = rowsRemaining
	}

	// Defensive check - should be impossible due to Scan() precondition
	if rowsToRead <= 0 {
		return errors.Newf("fillBuffer: no rows remaining (rowsInGroup=%d, currentRowInGroup=%d)",
			p.rowsInGroup, p.currentRowInGroup)
	}

	// Read rowsToRead values only from columns we need.
	for _, colIdx := range p.columnsToRead {
		metadata := p.columnMetadata[colIdx]

		var batch *parquetColumnBatch
		var err error
		if metadata.isList {
			// LIST columns use a stateful reader that handles repetition levels,
			// reassembles per-row element arrays, and buffers overflow rows
			// across fillBuffer calls.
			batch, err = p.listColumnReaders[colIdx].ReadBatch(rowsToRead)
		} else {
			batch, err = p.readBatchFromColumn(p.columnReaders[colIdx], colIdx, rowsToRead)
		}
		if err != nil {
			return errors.Wrapf(err, "reading batch from column %q (index %d)",
				p.columnMetadata[colIdx].columnName, colIdx)
		}

		// Store the batch for this column (sparse array - only needed columns populated).
		p.columnBatches[colIdx] = batch
	}

	// Update buffer state
	p.bufferedRowCount = rowsToRead
	p.currentBufferedRow = 0

	// Increment currentRowInGroup by the number of rows we just read
	p.currentRowInGroup += rowsToRead

	return nil
}

// Row returns a lightweight view of the current row.
// Instead of boxing values into `[]any`, we return a view that allows
// the consumer to read typed values directly from column batches.
// Scan() must be called before Row() to ensure the buffer is filled.
func (p *parquetRowProducer) Row() (any, error) {
	if p.err != nil {
		return nil, p.err
	}

	// Scan() should have already filled the buffer, but check just in case
	if p.currentBufferedRow >= p.bufferedRowCount {
		return nil, errors.New("Row() called without successful Scan()")
	}

	// Capture current batch references to make the view independent of producer state
	// This ensures the view remains valid even if the producer refills its buffers
	batchesCopy := make([]*parquetColumnBatch, len(p.columnBatches))
	copy(batchesCopy, p.columnBatches)

	// Return a view into the current row
	view := &parquetRowView{
		batches:    batchesCopy,
		numColumns: p.numColumns,
		rowIndex:   p.currentBufferedRow,
	}

	// Move to next buffered row and update progress
	p.currentBufferedRow++
	p.rowsProcessed++
	return view, nil
}

// readBatchFromColumn reads a batch of values from a column chunk reader.
// Returns a parquetColumnBatch with typed values, avoiding boxing into `any`
func (p *parquetRowProducer) readBatchFromColumn(
	colReader file.ColumnChunkReader, colIdx int, rowsToRead int64,
) (*parquetColumnBatch, error) {
	// Create batch with reusable buffers
	batch, err := newParquetColumnBatch(colReader, p.buffers, rowsToRead)
	if err != nil {
		return nil, err
	}

	// Read and populate the batch
	if err := batch.Read(); err != nil {
		return nil, err
	}

	return batch, nil
}

// Err returns any error encountered during scanning.
func (p *parquetRowProducer) Err() error {
	// First check our cached error
	if p.err != nil {
		return p.err
	}

	// Check all active column readers for errors that may have occurred
	// during streaming reads (e.g., storage errors from S3).
	// This prevents silent data loss similar to the issue fixed in PR #161318
	// for Avro imports, where storage errors could be missed if only cached
	// errors were checked.
	//
	// LIST columns are covered transparently: the file.ColumnChunkReader that
	// backs each parquetListColumnReader is also registered in p.columnReaders
	// when the row group is opened (see advanceToNextRowGroup), so the loop
	// below observes streaming errors for both flat and LIST columns.
	for _, colReader := range p.columnReaders {
		if err := colReader.Err(); err != nil {
			return err
		}
	}

	return nil
}

// Skip skips the current row.
func (p *parquetRowProducer) Skip() error {
	if p.err != nil {
		return p.err
	}
	// Advance the buffered row position to skip this row
	if p.currentBufferedRow < p.bufferedRowCount {
		p.currentBufferedRow++
		p.rowsProcessed++
	}
	return nil
}

// Progress returns the fraction of the file that has been processed.
func (p *parquetRowProducer) Progress() float32 {
	if p.totalRows == 0 {
		return 0
	}
	return float32(p.rowsProcessed) / float32(p.totalRows)
}

// validateAndBuildColumnMapping validates the Parquet schema against the table schema
// and builds a mapping from Parquet column names to table column indices.
func (p *parquetRowProducer) validateAndBuildColumnMapping(
	importCtx *parallelImportContext,
) (map[string]int, error) {
	parquetSchema := p.reader.MetaData().Schema

	// Build a map of Parquet column names (lowercase) for quick lookup.
	// columnMetadata.columnName is the user-visible name (group name for
	// LIST columns, leaf name otherwise) populated at file open.
	parquetColNames := make(map[string]bool)
	for parquetColIdx := range parquetSchema.NumColumns() {
		parquetColNames[strings.ToLower(p.columnMetadata[parquetColIdx].columnName)] = true
	}
	fieldNameToIdx := make(map[string]int)

	if importCtx.tableDesc == nil {
		// No table descriptor (test mode) - use targetCols as-is
		for i, col := range importCtx.targetCols {
			fieldNameToIdx[strings.ToLower(string(col))] = i
		}
		return fieldNameToIdx, nil
	}

	visibleCols := importCtx.tableDesc.VisibleColumns()

	// Determine which table columns to use
	var targetCols []string
	// If targetCols is empty, use all visible columns (automatic mapping)
	if len(importCtx.targetCols) == 0 {
		for _, col := range visibleCols {
			targetCols = append(targetCols, col.GetName())
		}
	} else {
		// Use explicitly specified columns
		for _, col := range importCtx.targetCols {
			targetCols = append(targetCols, string(col))
		}
	}

	// Build a map from column name (lowercase) to actual index in visibleCols
	// Detect case-only duplicates which are incompatible with Parquet's case-insensitive matching
	visibleColsByName := make(map[string]int)
	for i, col := range visibleCols {
		lower := strings.ToLower(col.GetName())
		if existingIdx, exists := visibleColsByName[lower]; exists {
			// Two columns differ only in case - can't do case-insensitive matching
			existing := visibleCols[existingIdx].GetName()
			current := col.GetName()
			return nil, errors.Newf(
				"table has columns %q and %q that differ only in case, which is incompatible with Parquet's case-insensitive column matching",
				existing, current)
		}
		visibleColsByName[lower] = i
	}

	// Build the mapping: parquet column name (lowercase) -> table column index in visibleCols
	for _, colName := range targetCols {
		// Find the actual index of this column in visibleCols
		visibleColIdx, found := visibleColsByName[strings.ToLower(colName)]
		if !found {
			return nil, errors.Newf("target column %q not found in table schema", colName)
		}
		fieldNameToIdx[strings.ToLower(colName)] = visibleColIdx
	}

	// Validate type compatibility ONLY for columns we're actually reading.
	for _, parquetColIdx := range p.columnsToRead {
		parquetCol := parquetSchema.Column(parquetColIdx)
		metadata := p.columnMetadata[parquetColIdx]
		colName := metadata.columnName

		// Find corresponding table column index in visibleCols.
		tableColIdx, found := fieldNameToIdx[strings.ToLower(colName)]
		if !found {
			// This shouldn't happen since columnsToRead was determined based on
			// the target columns, but handle it defensively.
			continue
		}

		// Get target column type from the table descriptor.
		targetCol := visibleCols[tableColIdx]
		targetType := targetCol.GetType()

		// Validate type compatibility using cached metadata.
		if metadata.isList {
			// LIST columns validate against the target's array contents type;
			// a JSONB target is accepted unconditionally since any supported
			// primitive list can be serialized to JSON.
			if targetType.Family() == types.JsonFamily {
				continue
			}
			if targetType.Family() != types.ArrayFamily {
				return nil, errors.Newf(
					"column %q: Parquet LIST requires an ARRAY or JSONB target type, got %s",
					colName, targetType.Family())
			}
			elementTargetType := targetType.ArrayContents()
			if err := validateWithLogicalType(
				parquetCol.PhysicalType(), metadata.logicalType, elementTargetType,
			); err != nil {
				return nil, errors.Wrapf(err, "column %q (LIST element type)", colName)
			}
		} else {
			if err := validateWithLogicalType(
				parquetCol.PhysicalType(), metadata.logicalType, targetType,
			); err != nil {
				return nil, errors.Wrapf(err, "column %q", colName)
			}
		}
	}

	// Validate that all required table columns are present in the Parquet file
	// Required = non-nullable columns without defaults
	for _, colName := range targetCols {
		visibleColIdx := visibleColsByName[strings.ToLower(colName)]
		targetCol := visibleCols[visibleColIdx]

		// Check if this column exists in the Parquet file (case-insensitive)
		if !parquetColNames[strings.ToLower(colName)] {
			// Column missing from Parquet file - check if it's required
			if !targetCol.IsNullable() && !targetCol.HasDefault() && !targetCol.IsComputed() {
				return nil, errors.Newf(
					"required table column %q (non-nullable, no default) not found in Parquet file",
					colName,
				)
			}
			// Optional column (nullable or has default) - will be set to NULL or default value
		}
	}

	return fieldNameToIdx, nil
}

// determineColumnsToRead determines which Parquet column indices need to be read
// based on the target table columns. Returns the list of Parquet column indices.
func determineColumnsToRead(
	importCtx *parallelImportContext,
	parquetSchema *schema.Schema,
	columnMetadata map[int]*parquetColumnMetadata,
) ([]int, error) {
	// This function is only called when importCtx is non-nil.
	// In test mode, newParquetRowProducer receives nil importCtx and reads all columns.
	if importCtx.tableDesc == nil {
		return nil, errors.AssertionFailedf("tableDesc must be non-nil when importCtx is provided")
	}

	// Determine which table columns we're importing into
	targetColSet := make(map[string]bool)
	visibleCols := importCtx.tableDesc.VisibleColumns()
	// If targetCols is empty, use all visible columns (automatic mapping)
	if len(importCtx.targetCols) == 0 {
		for _, col := range visibleCols {
			targetColSet[strings.ToLower(col.GetName())] = true
		}
	} else {
		// Use explicitly specified columns
		for _, col := range importCtx.targetCols {
			targetColSet[strings.ToLower(string(col))] = true
		}
	}

	// Find which Parquet columns match our target columns. columnMetadata
	// already holds the user-visible column name for every leaf (set via
	// parquetSchema.ColumnRoot in newParquetRowProducer), so it works for
	// LIST columns whose leaf name differs from the group name.
	var columnsToRead []int
	for parquetColIdx := range parquetSchema.NumColumns() {
		meta := columnMetadata[parquetColIdx]
		parquetColName := strings.ToLower(meta.columnName)

		if !targetColSet[parquetColName] {
			continue
		}

		// Surface any error deferred from detectListColumn at file-open
		// time. We only fail here, after confirming the user actually
		// wants this column. Wrap with the user-visible column name so
		// the message identifies the column the user requested rather
		// than an internal parquet group name.
		if meta.detectionErr != nil {
			return nil, errors.Wrapf(meta.detectionErr, "column %q", meta.columnName)
		}

		parquetCol := parquetSchema.Column(parquetColIdx)
		maxDefLevel := parquetCol.MaxDefinitionLevel()
		if maxDefLevel > 1 && !meta.isList {
			// detectListColumn already accepted any supported LIST column. A
			// definition level above 1 on a non-LIST leaf means MAPs or deeper
			// nesting, which the importer does not yet handle.
			return nil, unimplemented.NewWithIssueDetailf(162543,
				"parquet-nested-type",
				"column %q has nested or repeated structure (definition level %d); "+
					"only simple required/optional columns and single-level LISTs are supported",
				parquetColName, maxDefLevel)
		}
		columnsToRead = append(columnsToRead, parquetColIdx)
	}

	return columnsToRead, nil
}

// parquetRowConsumer implements importRowConsumer for Parquet files.
type parquetRowConsumer struct {
	importCtx      *parallelImportContext
	fieldNameToIdx map[string]int                 // Maps Parquet column name -> full-table visible column index
	columnMetadata map[int]*parquetColumnMetadata // Maps column index -> metadata (for conversion)
	reader         *file.Reader                   // Parquet file reader (for accessing schema)
	colMapping     []int                          // Precomputed mapping: parquet col idx -> target col idx (-1 if not mapped)
}

// newParquetRowConsumer creates a consumer that converts Parquet rows to datums.
func newParquetRowConsumer(
	importCtx *parallelImportContext,
	producer *parquetRowProducer,
	fileCtx *importFileContext,
	strict bool,
) (*parquetRowConsumer, error) {

	// Validate schema and build column mapping.
	fieldNameToIdx, err := producer.validateAndBuildColumnMapping(importCtx)
	if err != nil {
		return nil, err
	}

	// fieldNameToIdx maps column names to full-table visibleCols indices, which
	// is what type validation needs. DatumRowConverter, however, sizes Datums
	// and VisibleColTypes to len(targetCols) when an explicit column list is
	// given, so colMapping must store target-column indices (positions in
	// targetCols) to avoid out-of-bounds access when a non-last column is
	// excluded. When no explicit list is given (or in test mode without a
	// tableDesc), target-idx and full-table-idx coincide and fieldNameToIdx is
	// already what we want.
	nameToTargetIdx := fieldNameToIdx
	if importCtx.tableDesc != nil && len(importCtx.targetCols) > 0 {
		nameToTargetIdx = make(map[string]int, len(importCtx.targetCols))
		for targetIdx, colName := range importCtx.targetCols {
			nameToTargetIdx[strings.ToLower(string(colName))] = targetIdx
		}
	}

	// Precompute column mapping to avoid hash lookups in the hot path (FillDatums).
	// For each Parquet column index, store the corresponding target column index (-1 if not mapped).
	numCols := producer.reader.MetaData().Schema.NumColumns()
	colMapping := make([]int, numCols)
	for parquetColIdx := range numCols {
		colName := producer.columnMetadata[parquetColIdx].columnName
		if targetIdx, found := nameToTargetIdx[strings.ToLower(colName)]; found {
			colMapping[parquetColIdx] = targetIdx
			continue
		}
		colMapping[parquetColIdx] = -1
		if strict {
			return nil, errors.Newf(
				"column %q in Parquet file is not in the target table", colName)
		}
	}

	return &parquetRowConsumer{
		importCtx:      importCtx,
		fieldNameToIdx: fieldNameToIdx,
		columnMetadata: producer.columnMetadata,
		reader:         producer.reader,
		colMapping:     colMapping,
	}, nil
}

// FillDatums converts a Parquet row to CockroachDB datums.
// It reads typed values directly from column batches, avoiding boxing into `any`.
func (c *parquetRowConsumer) FillDatums(
	ctx context.Context, rowData any, rowNum int64, conv *row.DatumRowConverter,
) error {
	// rowData is a *parquetRowView from parquetRowProducer.Row()
	view, ok := rowData.(*parquetRowView)
	if !ok {
		return errors.Errorf("expected *parquetRowView, got %T", rowData)
	}

	rowIdx := view.rowIndex

	// For each column in the Parquet file, find the corresponding target column.
	for parquetColIdx := range view.numColumns {
		// Skip columns we didn't read.
		batch := view.batches[parquetColIdx]
		if batch == nil {
			continue
		}

		// Use precomputed mapping to find target column index (O(1) array access).
		targetColIdx := c.colMapping[parquetColIdx]
		if targetColIdx < 0 {
			// Column not in the target column set; skip it.
			continue
		}

		// Extract value from batch.
		value, isNull, err := batch.GetValueAt(int(rowIdx))
		if err != nil {
			return newImportRowError(err, fmt.Sprintf("row %d", rowNum), rowNum)
		}

		// Convert to datum.
		var datum tree.Datum
		if isNull {
			datum = tree.DNull
		} else {
			targetType := conv.VisibleColTypes[targetColIdx]
			metadata := c.columnMetadata[parquetColIdx]
			if metadata.isList {
				datum, err = assembleListDatum(value, targetType, metadata)
			} else {
				datum, err = convertWithLogicalType(value, targetType, metadata)
			}
			if err != nil {
				return newImportRowError(err, fmt.Sprintf("row %d", rowNum), rowNum)
			}
		}

		conv.Datums[targetColIdx] = datum
	}

	// Set any nil datums to DNull (for columns not in Parquet file)
	// This is critical - uninitialized datums will cause crashes!
	for i := range conv.Datums {
		if conv.Datums[i] == nil {
			conv.Datums[i] = tree.DNull
		}
	}

	return nil
}

// assembleListDatum converts a slice of Parquet element values into a
// tree.Datum. The target type determines the output format:
//   - ArrayFamily: builds a tree.DArray, converting each element via
//     convertWithLogicalType.
//   - JsonFamily: builds a JSONB array, converting each element via
//     parquetElementToJSON.
func assembleListDatum(
	value any, targetType *types.T, metadata *parquetColumnMetadata,
) (tree.Datum, error) {
	elements, ok := value.([]any)
	if !ok {
		return nil, errors.AssertionFailedf("expected []any for LIST column, got %T", value)
	}

	switch targetType.Family() {
	case types.ArrayFamily:
		return assembleListAsArray(elements, targetType, metadata)
	case types.JsonFamily:
		return assembleListAsJSON(elements, metadata)
	default:
		return nil, errors.AssertionFailedf(
			"LIST columns can only target ARRAY or JSONB types, got %s; should have been "+
				"rejected during schema validation", targetType.Family())
	}
}

// assembleListAsArray builds a tree.DArray from Parquet element values,
// converting each element with convertWithLogicalType.
func assembleListAsArray(
	elements []any, targetType *types.T, metadata *parquetColumnMetadata,
) (tree.Datum, error) {
	elementType := targetType.ArrayContents()
	arr := tree.NewDArray(elementType)
	for i, elem := range elements {
		if elem == nil {
			if err := arr.Append(tree.DNull); err != nil {
				return nil, errors.Wrapf(err, "appending LIST element %d", i)
			}
			continue
		}
		elemDatum, err := convertWithLogicalType(elem, elementType, metadata)
		if err != nil {
			return nil, errors.Wrapf(err, "converting LIST element %d", i)
		}
		if err := arr.Append(elemDatum); err != nil {
			return nil, errors.Wrapf(err, "appending LIST element %d", i)
		}
	}
	return arr, nil
}

// assembleListAsJSON builds a JSONB array datum from Parquet element values,
// converting each element with parquetElementToJSON.
func assembleListAsJSON(elements []any, metadata *parquetColumnMetadata) (tree.Datum, error) {
	ab := json.NewArrayBuilder(len(elements))
	for i, elem := range elements {
		if elem == nil {
			ab.Add(json.NullJSONValue)
			continue
		}
		j, err := parquetElementToJSON(elem, metadata)
		if err != nil {
			return nil, errors.Wrapf(err, "converting LIST element %d to JSON", i)
		}
		ab.Add(j)
	}
	return tree.NewDJSON(ab.Build()), nil
}

// parquetElementToJSON converts a single raw Parquet element value to a
// json.JSON value, honoring the element's logical type annotation so that
// dates, decimals, UUIDs, intervals, and timestamps land in JSON in their
// human-readable form rather than as raw physical values.
//
// The conversion routes through convertWithLogicalType (which interprets the
// logical-type metadata) and then tree.AsJSON (which knows how to format each
// datum kind as JSON). EnumLogicalType is short-circuited to a JSON string
// because convertWithLogicalType's enum path needs an enum-typed target
// descriptor, which is not available in a JSONB context.
func parquetElementToJSON(elem any, metadata *parquetColumnMetadata) (json.JSON, error) {
	// EnumLogicalType: treat the BYTE_ARRAY payload as a plain string.
	if b, ok := elem.([]byte); ok {
		if _, isEnum := metadata.logicalType.(schema.EnumLogicalType); isEnum {
			return json.FromString(string(b)), nil
		}
	}

	targetType, err := jsonElementTargetType(elem, metadata)
	if err != nil {
		return nil, err
	}
	datum, err := convertWithLogicalType(elem, targetType, metadata)
	if err != nil {
		return nil, err
	}
	return tree.AsJSON(datum, sessiondatapb.DataConversionConfig{}, time.UTC)
}

// jsonElementTargetType picks a CRDB type to feed convertWithLogicalType when
// the surrounding LIST is targeting JSONB. The type is chosen so that
// convertWithLogicalType's logical-type branches fire correctly (e.g. a UUID
// logical type needs types.Uuid as a hint to pick the convertUuid* path) and
// its physical-type fallbacks produce a sensible datum (e.g. plain int32 with
// no logical type becomes DInt rather than DDecimal).
//
// Each branch listed below mirrors one of the typed branches inside
// convertWithLogicalType. Logical-type cases come first so they win over the
// raw element-type fallback when both would apply.
func jsonElementTargetType(elem any, metadata *parquetColumnMetadata) (*types.T, error) {
	switch metadata.logicalType.(type) {
	case schema.StringLogicalType:
		return types.String, nil
	case schema.JSONLogicalType:
		return types.Jsonb, nil
	case schema.UUIDLogicalType:
		return types.Uuid, nil
	case schema.IntervalLogicalType:
		return types.Interval, nil
	case schema.DateLogicalType:
		return types.Date, nil
	case *schema.DecimalLogicalType:
		return types.Decimal, nil
	case *schema.TimestampLogicalType:
		return types.TimestampTZ, nil
	case *schema.TimeLogicalType:
		return types.Time, nil
	}
	switch elem.(type) {
	case bool:
		return types.Bool, nil
	case int32, int64:
		return types.Int, nil
	case float32, float64:
		return types.Float, nil
	case []byte:
		// Unannotated BYTE_ARRAY defaults to text, matching the flat-path
		// behavior in convertBytesBasedOnTargetType. Bytes ride through to
		// the resulting JSON string verbatim with no UTF-8 validation, so
		// non-UTF-8 inputs (e.g. {0xff}) reach the target unchanged. Callers
		// with genuinely binary data should target an ARRAY<BYTES> column
		// (which routes through convertBytesBasedOnTargetType's BytesFamily
		// branch) or use FIXED_LEN_BYTE_ARRAY, which defaults to bytes
		// below.
		return types.String, nil
	case parquet.FixedLenByteArray:
		return types.Bytes, nil
	case parquet.Int96:
		return types.TimestampTZ, nil
	}
	return nil, errors.AssertionFailedf("unsupported element type %T for JSON conversion", elem)
}
