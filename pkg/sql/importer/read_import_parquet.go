// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	columnsToRead []int                          // Parquet column indices to read
	numColumns    int                            // Total number of columns in Parquet file
	columnReaders map[int]file.ColumnChunkReader // Maps Parquet column index -> reader

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
	for i := 0; i < totalRowGroups; i++ {
		totalRows += reader.RowGroup(i).NumRows()
	}

	numColumns := reader.MetaData().Schema.NumColumns()
	batchSize := int64(defaultParquetBatchSize)
	parquetSchema := reader.MetaData().Schema
	// Determine which columns to read
	var columnsToRead []int
	if importCtx == nil {
		// No import context (test mode) - read all columns
		columnsToRead = make([]int, numColumns)
		for i := 0; i < numColumns; i++ {
			columnsToRead[i] = i
		}
	} else {
		// Determine which Parquet columns to read based on target table columns
		columnsToRead, err = determineColumnsToRead(importCtx, parquetSchema)
		if err != nil {
			return nil, err
		}
	}

	// Cache schema metadata once per file for all columns we'll read.
	columnMetadata := make(map[int]*parquetColumnMetadata)
	for _, colIdx := range columnsToRead {
		col := parquetSchema.Column(colIdx)
		columnMetadata[colIdx] = &parquetColumnMetadata{
			logicalType:   deriveLogicalType(col),
			convertedType: col.ConvertedType(), // Keep for reference/debugging
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
		return nil
	}

	// Get the row group reader
	rowGroup := p.reader.RowGroup(p.currentRowGroup)
	p.rowsInGroup = rowGroup.NumRows()
	p.currentRowInGroup = 0

	// Set up column chunk readers only for columns we need to read
	p.columnReaders = make(map[int]file.ColumnChunkReader, len(p.columnsToRead))
	for _, colIdx := range p.columnsToRead {
		colReader, err := rowGroup.Column(colIdx)
		if err != nil {
			return errors.Wrapf(err, "failed to get column reader for column %d", colIdx)
		}
		p.columnReaders[colIdx] = colReader
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

	// Read rowsToRead values only from columns we need
	for _, colIdx := range p.columnsToRead {
		colReader := p.columnReaders[colIdx]

		// Read a batch of typed values from this column
		batch, err := p.readBatchFromColumn(colReader, colIdx, rowsToRead)
		if err != nil {
			return errors.Wrapf(err, "failed to read batch from column %d", colIdx)
		}

		// Store the batch for this column (sparse array - only needed columns populated)
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

	// Build a map of Parquet column names (lowercase) for quick lookup
	parquetColNames := make(map[string]bool)
	for parquetColIdx := 0; parquetColIdx < parquetSchema.NumColumns(); parquetColIdx++ {
		parquetCol := parquetSchema.Column(parquetColIdx)
		parquetColNames[strings.ToLower(parquetCol.Name())] = true
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
	for i, colName := range targetCols {
		// Find the actual index of this column in visibleCols
		visibleColIdx, found := visibleColsByName[strings.ToLower(colName)]
		if !found {
			return nil, errors.Newf("target column %q not found in table schema", colName)
		}
		fieldNameToIdx[strings.ToLower(colName)] = visibleColIdx
		_ = i // Suppress unused variable warning
	}

	// Validate type compatibility ONLY for columns we're actually reading.
	for _, parquetColIdx := range p.columnsToRead {
		parquetCol := parquetSchema.Column(parquetColIdx)
		parquetColName := parquetCol.Name()

		// Find corresponding table column index in visibleCols
		tableColIdx, found := fieldNameToIdx[strings.ToLower(parquetColName)]
		if !found {
			// This shouldn't happen since columnsToRead was determined based on
			// the target columns, but handle it defensively.
			continue
		}

		// Get target column type from the table descriptor
		targetCol := visibleCols[tableColIdx]
		targetType := targetCol.GetType()

		// Validate type compatibility using cached metadata
		metadata := p.columnMetadata[parquetColIdx]
		if err := validateWithLogicalType(parquetCol.PhysicalType(), metadata.logicalType, targetType); err != nil {
			return nil, errors.Wrapf(err, "column %q", parquetColName)
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
	importCtx *parallelImportContext, parquetSchema *schema.Schema,
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

	// Find which Parquet columns match our target columns
	var columnsToRead []int
	for parquetColIdx := 0; parquetColIdx < parquetSchema.NumColumns(); parquetColIdx++ {
		parquetCol := parquetSchema.Column(parquetColIdx)
		parquetColName := strings.ToLower(parquetCol.Name())

		// If this Parquet column is in our target set, we need to read it
		if targetColSet[parquetColName] {
			// Validate that the column doesn't have nested or repeated structures.
			// Definition level > 1 indicates nested/repeated data (arrays, maps, etc.)
			// which we don't currently support.
			maxDefLevel := parquetCol.MaxDefinitionLevel()
			if maxDefLevel > 1 {
				return nil,
					errors.UnimplementedErrorf(
						errors.IssueLink{
							IssueURL: build.MakeIssueURL(162543),
							Detail:   "support parquet nested types for import",
						}, "column %q has nested or repeated structure (definition level %d); "+
							"only simple required and optional columns are supported",
						parquetColName, maxDefLevel)

			}
			columnsToRead = append(columnsToRead, parquetColIdx)
		}
	}

	return columnsToRead, nil
}

// parquetRowConsumer implements importRowConsumer for Parquet files.
type parquetRowConsumer struct {
	importCtx      *parallelImportContext
	fieldNameToIdx map[string]int                 // Maps Parquet column name -> table column index
	columnMetadata map[int]*parquetColumnMetadata // Maps column index -> metadata (for conversion)
	reader         *file.Reader                   // Parquet file reader (for accessing schema)
	strict         bool                           // If true, error on columns in Parquet not in table
	colMapping     []int                          // Precomputed mapping: parquet col idx -> table col idx (-1 if not mapped)
}

// newParquetRowConsumer creates a consumer that converts Parquet rows to datums.
func newParquetRowConsumer(
	importCtx *parallelImportContext,
	producer *parquetRowProducer,
	fileCtx *importFileContext,
	strict bool,
) (*parquetRowConsumer, error) {

	// Validate schema and build column mapping
	fieldNameToIdx, err := producer.validateAndBuildColumnMapping(importCtx)
	if err != nil {
		return nil, err
	}

	// Precompute column mapping to avoid hash lookups in the hot path (FillDatums).
	// For each Parquet column index, store the corresponding table column index (-1 if not mapped).
	numCols := producer.reader.MetaData().Schema.NumColumns()
	colMapping := make([]int, numCols)
	for parquetColIdx := 0; parquetColIdx < numCols; parquetColIdx++ {
		col := producer.reader.MetaData().Schema.Column(parquetColIdx)
		parquetColName := col.Name()
		if tableColIdx, found := fieldNameToIdx[strings.ToLower(parquetColName)]; found {
			colMapping[parquetColIdx] = tableColIdx
		} else {
			colMapping[parquetColIdx] = -1
			if strict {
				return nil, errors.Newf(
					"column %q in Parquet file is not in the target table", parquetColName)
			}
		}
	}

	return &parquetRowConsumer{
		importCtx:      importCtx,
		fieldNameToIdx: fieldNameToIdx,
		columnMetadata: producer.columnMetadata,
		reader:         producer.reader,
		strict:         strict,
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

	// For each column in the Parquet file, find the corresponding table column
	for parquetColIdx := 0; parquetColIdx < view.numColumns; parquetColIdx++ {
		// Skip columns we didn't read
		batch := view.batches[parquetColIdx]
		if batch == nil {
			continue
		}

		// Use precomputed mapping to find table column index (O(1) array access)
		tableColIdx := c.colMapping[parquetColIdx]
		if tableColIdx < 0 {
			// Column in Parquet file not in table
			if c.strict {
				// Get column name for error message
				col := c.reader.MetaData().Schema.Column(parquetColIdx)
				parquetColName := col.Name()
				return newImportRowError(
					errors.Newf("column %q in Parquet file is not in the target table", parquetColName),
					fmt.Sprintf("row %d", rowNum), rowNum)
			}
			// In non-strict mode, skip extra columns
			continue
		}

		// Extract value from batch
		value, isNull, err := batch.GetValueAt(int(rowIdx))
		if err != nil {
			return newImportRowError(err, fmt.Sprintf("row %d", rowNum), rowNum)
		}

		// Convert to datum
		var datum tree.Datum
		if isNull {
			datum = tree.DNull
		} else {
			// Convert Parquet value to datum using unified LogicalType conversion
			targetType := conv.VisibleColTypes[tableColIdx]
			metadata := c.columnMetadata[parquetColIdx]
			datum, err = convertWithLogicalType(value, targetType, metadata)
			if err != nil {
				return newImportRowError(err, fmt.Sprintf("row %d", rowNum), rowNum)
			}
		}

		// Set datum in converter
		conv.Datums[tableColIdx] = datum
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
