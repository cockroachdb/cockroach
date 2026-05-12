// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/errors"
)

// parquetListLevelInfo describes one LIST nesting level's definition and
// repetition level thresholds. For a nested LIST(LIST(INT32)) structure,
// there are two levels: level 0 (outer list) and level 1 (inner list).
type parquetListLevelInfo struct {
	// repLevel is the repetition level that indicates a new element starts at
	// this nesting depth. For the outermost list this is 1 (since rep=0 means
	// a new top-level row), for the next inner list it is 2, etc.
	repLevel int16

	// nullListDefLevel: at or below this definition level, the list at this
	// nesting depth is null. Set to -1 if the list is required (cannot be null).
	nullListDefLevel int16

	// emptyListDefLevel: exactly this definition level means the list at this
	// nesting depth is present but empty (no elements).
	emptyListDefLevel int16
}

// parquetListColumnInfo holds schema metadata for a LIST column (single-level
// or nested). Populated once during file open and reused across all batches.
//
// A single-level Parquet LIST uses 3-level encoding:
//
//	optional group <columnName> (LIST) {
//	  repeated group list {
//	    optional/required <type> element;
//	  }
//	}
//
// A nested LIST(LIST(T)) adds another pair of (group, repeated group) levels.
// The library flattens these to a single leaf column. Definition and repetition
// levels encode null-list, empty-list, null-element, and present-element states
// at each nesting depth.
type parquetListColumnInfo struct {
	// columnName is the top-level group node name (the user-visible column name,
	// e.g. "tags"), as opposed to the leaf name ("element").
	columnName string

	// Element's physical type and logical/converted type for value conversion.
	// These describe the leaf (innermost) primitive type.
	elementPhysicalType  parquet.Type
	elementLogicalType   schema.LogicalType
	elementConvertedType schema.ConvertedType

	// nestingDepth is the number of LIST levels.
	// 1 for LIST(INT), 2 for LIST(LIST(INT)), etc.
	nestingDepth int

	// levels describes each nesting level from outermost (index 0) to innermost.
	// Length equals nestingDepth.
	levels []parquetListLevelInfo

	// Leaf-level thresholds (apply to the innermost list's elements).
	maxDefLevel            int16 // Maximum definition level for the leaf column.
	presentElementDefLevel int16 // Exactly this: leaf element value is present.

	// Whether leaf elements within the innermost list can be NULL.
	elementIsOptional bool
}

// detectListColumn checks whether the leaf column at colIdx is part of a
// Parquet LIST encoding and returns the parsed metadata if so. Returns nil
// if the column is a flat (non-repeated) column.
func detectListColumn(parquetSchema *schema.Schema, colIdx int) (*parquetListColumnInfo, error) {
	col := parquetSchema.Column(colIdx)

	// Only columns with repetition levels > 0 can be LIST columns.
	if col.MaxRepetitionLevel() == 0 {
		return nil, nil
	}

	leafNode := col.SchemaNode()
	parent := leafNode.Parent()
	if parent == nil {
		return nil, nil
	}
	grandparent := parent.Parent()
	if grandparent == nil {
		return nil, nil
	}

	// Check that the parent is a repeated group (the "list" group).
	if parent.RepetitionType() != parquet.Repetitions.Repeated {
		return nil, errors.UnimplementedErrorf(
			errors.IssueLink{IssueURL: build.MakeIssueURL(162543),
				Detail: "support parquet nested types for import"},
			"column %q has nested structure but parent is not a repeated group",
			col.Name())
	}

	// Check that the grandparent has LIST logical type or LIST converted type.
	// Also detect MAP columns so we can return a specific error message.
	lt := grandparent.LogicalType()

	isListGroup := false
	if lt != nil {
		_, isListGroup = lt.(schema.ListLogicalType)
	}
	if !isListGroup {
		isListGroup = grandparent.ConvertedType() == schema.ConvertedTypes.List
	}
	if !isListGroup {
		isMapGroup := false
		if lt != nil {
			_, isMapGroup = lt.(schema.MapLogicalType)
		}
		if !isMapGroup {
			ct := grandparent.ConvertedType()
			isMapGroup = ct == schema.ConvertedTypes.Map || ct == schema.ConvertedTypes.MapKeyValue
		}
		if isMapGroup {
			return nil, errors.UnimplementedErrorf(
				errors.IssueLink{IssueURL: build.MakeIssueURL(162543),
					Detail: "support parquet nested types for import"},
				"column %q is a MAP type, which is not supported for import",
				grandparent.Name())
		}
		return nil, errors.UnimplementedErrorf(
			errors.IssueLink{IssueURL: build.MakeIssueURL(162543),
				Detail: "support parquet nested types for import"},
			"column %q has repeated parent but grandparent is not a LIST group",
			col.Name())
	}

	// Reject deeper nesting: the grandparent must be a direct child of the
	// schema root. If it has more ancestors, this is a nested LIST or a LIST
	// inside a STRUCT, which we don't support.
	if ggp := grandparent.Parent(); ggp != nil && ggp.Parent() != nil {
		return nil, errors.UnimplementedErrorf(
			errors.IssueLink{IssueURL: build.MakeIssueURL(162543),
				Detail: "support parquet nested types for import"},
			"column %q is a nested LIST (LIST of LIST or LIST inside STRUCT), which is not supported",
			grandparent.Name())
	}

	// Reject non-primitive leaves (the leaf must be a primitive node, not a group).
	if leafNode.Type() != schema.Primitive {
		return nil, errors.UnimplementedErrorf(
			errors.IssueLink{IssueURL: build.MakeIssueURL(162543),
				Detail: "support parquet nested types for import"},
			"column %q is a LIST of a non-primitive type, which is not supported",
			grandparent.Name())
	}

	// Validate the repeated group has exactly one child (the element).
	parentGroup, ok := parent.(*schema.GroupNode)
	if !ok || parentGroup.NumFields() != 1 {
		return nil, errors.UnimplementedErrorf(
			errors.IssueLink{IssueURL: build.MakeIssueURL(162543),
				Detail: "support parquet nested types for import"},
			"column %q has LIST structure with unexpected number of children in repeated group",
			grandparent.Name())
	}

	// Build the list info with def level thresholds.
	//
	// For optional list + optional element (the common case):
	//   grandparent (optional) -> adds 1 to defLevel => defLevel 0 = null list
	//   parent (repeated)      -> adds 1 to defLevel => defLevel 1 = empty list
	//   leaf (optional)        -> adds 1 to defLevel => defLevel 2 = null element, 3 = present
	//
	// For optional list + required element:
	//   grandparent (optional) -> adds 1 => defLevel 0 = null list
	//   parent (repeated)      -> adds 1 => defLevel 1 = empty list
	//   leaf (required)        -> no add  => defLevel 2 = present element
	//
	// For required list + optional element:
	//   grandparent (required) -> no add  => (list cannot be null)
	//   parent (repeated)      -> adds 1 => defLevel 0 = empty list
	//   leaf (optional)        -> adds 1 => defLevel 1 = null element, 2 = present
	elementIsOptional := leafNode.RepetitionType() == parquet.Repetitions.Optional

	var nullListDefLevel int16
	var emptyListDefLevel int16
	if grandparent.RepetitionType() == parquet.Repetitions.Optional {
		// Optional list: defLevel 0 = null list, defLevel 1 = empty list.
		nullListDefLevel = 0
		emptyListDefLevel = 1
	} else {
		// Required list: cannot be null. Empty list at defLevel 0.
		nullListDefLevel = -1 // sentinel: list can never be null
		emptyListDefLevel = 0
	}

	return &parquetListColumnInfo{
		columnName:           grandparent.Name(),
		elementPhysicalType:  col.PhysicalType(),
		elementLogicalType:   deriveLogicalType(col),
		elementConvertedType: col.ConvertedType(),
		nestingDepth:         1,
		levels: []parquetListLevelInfo{
			{
				repLevel:          1,
				nullListDefLevel:  nullListDefLevel,
				emptyListDefLevel: emptyListDefLevel,
			},
		},
		maxDefLevel:            col.MaxDefinitionLevel(),
		presentElementDefLevel: col.MaxDefinitionLevel(),
		elementIsOptional:      elementIsOptional,
	}, nil
}

// parquetListOverflowEntry holds one fully-assembled row that was read from
// a previous chunk but belongs to a later batch.
type parquetListOverflowEntry struct {
	values []any // element values (nil = null list, empty = empty list)
	isNull bool
}

// parquetListColumnReader wraps a Parquet column chunk reader for LIST columns
// and persists overflow state across ReadBatch calls. Unlike flat columns
// (where ReadBatch reads exactly N rows), LIST columns require reading level
// entries in fixed-size chunks. When a chunk straddles a batch boundary, the
// over-read rows are buffered here and drained on the next ReadBatch call.
//
// Created once per row group in advanceToNextRowGroup for each LIST column.
type parquetListColumnReader struct {
	// Schema metadata (immutable after construction).
	listInfo               *parquetListColumnInfo
	level                  parquetListLevelInfo
	presentElementDefLevel int16
	elementIsOptional      bool

	// Typed I/O closures (set up once in the constructor based on
	// the element's physical type).
	readChunk    func() (int64, int, error)
	extractValue func(valIdx int) any

	// Level buffers (allocated once, reused across ReadBatch calls).
	defBuf []int16
	repBuf []int16

	// Overflow state: fully-assembled rows from a previous chunk that
	// belong to a later batch. Drained first at the start of each
	// ReadBatch call.
	overflow []parquetListOverflowEntry

	// Partial row state: when a chunk boundary falls in the middle of a
	// row's elements, the in-progress row is stashed here and resumed
	// when the next chunk is read.
	partialRow    []any
	partialIsNull bool
	hasPartialRow bool
}

// listColumnChunkSize is the number of level entries read per ReadBatch chunk.
const listColumnChunkSize = 1024

// newParquetListColumnReader creates a stateful reader for a LIST column.
// The reader persists overflow state between ReadBatch calls so that no
// level entries are lost when a chunk straddles a batch boundary.
func newParquetListColumnReader(
	colReader file.ColumnChunkReader, listInfo *parquetListColumnInfo,
) (*parquetListColumnReader, error) {
	if listInfo.nestingDepth != 1 {
		return nil, errors.AssertionFailedf(
			"parquetListColumnReader only supports nestingDepth 1, got %d",
			listInfo.nestingDepth)
	}

	r := &parquetListColumnReader{
		listInfo:               listInfo,
		level:                  listInfo.levels[0],
		presentElementDefLevel: listInfo.presentElementDefLevel,
		elementIsOptional:      listInfo.elementIsOptional,
		defBuf:                 make([]int16, listColumnChunkSize),
		repBuf:                 make([]int16, listColumnChunkSize),
	}

	switch listInfo.elementPhysicalType {
	case parquet.Types.Boolean:
		valBuf := make([]bool, listColumnChunkSize)
		r.readChunk = func() (int64, int, error) {
			return colReader.(*file.BooleanColumnChunkReader).ReadBatch(
				listColumnChunkSize, valBuf, r.defBuf, r.repBuf)
		}
		r.extractValue = func(valIdx int) any { return valBuf[valIdx] }

	case parquet.Types.Int32:
		valBuf := make([]int32, listColumnChunkSize)
		r.readChunk = func() (int64, int, error) {
			return colReader.(*file.Int32ColumnChunkReader).ReadBatch(
				listColumnChunkSize, valBuf, r.defBuf, r.repBuf)
		}
		r.extractValue = func(valIdx int) any { return valBuf[valIdx] }

	case parquet.Types.Int64:
		valBuf := make([]int64, listColumnChunkSize)
		r.readChunk = func() (int64, int, error) {
			return colReader.(*file.Int64ColumnChunkReader).ReadBatch(
				listColumnChunkSize, valBuf, r.defBuf, r.repBuf)
		}
		r.extractValue = func(valIdx int) any { return valBuf[valIdx] }

	case parquet.Types.Int96:
		valBuf := make([]parquet.Int96, listColumnChunkSize)
		r.readChunk = func() (int64, int, error) {
			return colReader.(*file.Int96ColumnChunkReader).ReadBatch(
				listColumnChunkSize, valBuf, r.defBuf, r.repBuf)
		}
		r.extractValue = func(valIdx int) any { return valBuf[valIdx] }

	case parquet.Types.Float:
		valBuf := make([]float32, listColumnChunkSize)
		r.readChunk = func() (int64, int, error) {
			return colReader.(*file.Float32ColumnChunkReader).ReadBatch(
				listColumnChunkSize, valBuf, r.defBuf, r.repBuf)
		}
		r.extractValue = func(valIdx int) any { return valBuf[valIdx] }

	case parquet.Types.Double:
		valBuf := make([]float64, listColumnChunkSize)
		r.readChunk = func() (int64, int, error) {
			return colReader.(*file.Float64ColumnChunkReader).ReadBatch(
				listColumnChunkSize, valBuf, r.defBuf, r.repBuf)
		}
		r.extractValue = func(valIdx int) any { return valBuf[valIdx] }

	case parquet.Types.ByteArray:
		valBuf := make([]parquet.ByteArray, listColumnChunkSize)
		r.readChunk = func() (int64, int, error) {
			return colReader.(*file.ByteArrayColumnChunkReader).ReadBatch(
				listColumnChunkSize, valBuf, r.defBuf, r.repBuf)
		}
		r.extractValue = func(valIdx int) any {
			// Copy bytes since the buffer will be reused.
			copied := make([]byte, len(valBuf[valIdx]))
			copy(copied, valBuf[valIdx])
			return copied
		}

	case parquet.Types.FixedLenByteArray:
		valBuf := make([]parquet.FixedLenByteArray, listColumnChunkSize)
		r.readChunk = func() (int64, int, error) {
			return colReader.(*file.FixedLenByteArrayColumnChunkReader).ReadBatch(
				listColumnChunkSize, valBuf, r.defBuf, r.repBuf)
		}
		r.extractValue = func(valIdx int) any {
			copied := make([]byte, len(valBuf[valIdx]))
			copy(copied, valBuf[valIdx])
			return parquet.FixedLenByteArray(copied)
		}

	default:
		return nil, errors.Newf(
			"unsupported Parquet physical type for LIST element: %s",
			listInfo.elementPhysicalType.String())
	}

	return r, nil
}

// ReadBatch reads exactly rowsToRead rows from the LIST column and returns
// a parquetColumnBatch. It first drains any overflow rows saved from a
// previous call, then reads new chunks as needed. When a chunk contains
// entries beyond the batch boundary, all entries are processed and excess
// rows are saved as overflow for the next call.
func (r *parquetListColumnReader) ReadBatch(rowsToRead int64) (*parquetColumnBatch, error) {
	batch := &parquetColumnBatch{
		physicalType:  r.listInfo.elementPhysicalType,
		rowCount:      rowsToRead,
		maxDefLevel:   r.listInfo.maxDefLevel,
		isList:        true,
		isNull:        make([]bool, rowsToRead),
		listRowValues: make([][]any, rowsToRead),
	}

	rowsFilled := int64(0)

	// Phase 1: Drain overflow rows from a previous ReadBatch call.
	drained := 0
	for drained < len(r.overflow) && rowsFilled < rowsToRead {
		entry := r.overflow[drained]
		batch.listRowValues[rowsFilled] = entry.values
		batch.isNull[rowsFilled] = entry.isNull
		drained++
		rowsFilled++
	}
	r.overflow = r.overflow[drained:]
	if len(r.overflow) == 0 {
		// Allow GC of the old backing array once fully drained.
		r.overflow = nil
	}
	if rowsFilled >= rowsToRead {
		return batch, nil
	}

	// Phase 2: Resume partial row state from a previous chunk.
	currentElements := r.partialRow
	currentIsNull := r.partialIsNull
	inRow := r.hasPartialRow
	r.partialRow = nil
	r.partialIsNull = false
	r.hasPartialRow = false

	// addRow places a completed row into the batch or overflow.
	addRow := func(elements []any, isNull bool) {
		if rowsFilled < rowsToRead {
			batch.listRowValues[rowsFilled] = elements
			batch.isNull[rowsFilled] = isNull
			rowsFilled++
		} else {
			r.overflow = append(r.overflow, parquetListOverflowEntry{
				values: elements,
				isNull: isNull,
			})
		}
	}

	// Phase 3: Read chunks until we have enough rows. Process ALL entries
	// in each chunk to avoid losing data from the forward-only reader.
	for rowsFilled < rowsToRead {
		total, valuesRead, err := r.readChunk()
		if err != nil {
			return nil, errors.Wrap(err, "reading LIST column batch")
		}
		if total == 0 {
			if inRow {
				addRow(currentElements, currentIsNull)
				inRow = false
			}
			break
		}

		valIdx := 0
		for i := int64(0); i < total; i++ {
			if r.repBuf[i] == 0 {
				// New row boundary. Finalize the previous row if any.
				if inRow {
					addRow(currentElements, currentIsNull)
				}
				inRow = true
				currentElements = nil
				currentIsNull = false

				if r.defBuf[i] <= r.level.nullListDefLevel {
					// Null list.
					currentIsNull = true
				} else if r.defBuf[i] == r.level.emptyListDefLevel {
					// Empty list (present but no elements).
					currentElements = []any{}
				} else if r.defBuf[i] == r.presentElementDefLevel {
					// Non-empty list, first element is present.
					currentElements = []any{r.extractValue(valIdx)}
					valIdx++
				} else if r.elementIsOptional {
					// Non-empty list, first element is null.
					currentElements = []any{nil}
				} else {
					return nil, errors.AssertionFailedf(
						"unexpected definition level %d for required-element LIST column %q at rep=0",
						r.defBuf[i], r.listInfo.columnName)
				}
			} else {
				// Continuation of current list (repLevel > 0).
				if r.defBuf[i] == r.presentElementDefLevel {
					currentElements = append(currentElements, r.extractValue(valIdx))
					valIdx++
				} else if r.elementIsOptional {
					currentElements = append(currentElements, nil)
				} else {
					return nil, errors.AssertionFailedf(
						"unexpected definition level %d for required-element LIST column %q at rep>0",
						r.defBuf[i], r.listInfo.columnName)
				}
			}
		}

		if valIdx != valuesRead {
			return nil, errors.AssertionFailedf(
				"value index mismatch in LIST column %q: tracked %d, library reported %d",
				r.listInfo.columnName, valIdx, valuesRead)
		}

		// If the column is exhausted (returned fewer entries than requested),
		// the in-progress row is the final row and must be finalized now.
		if total < listColumnChunkSize && inRow {
			addRow(currentElements, currentIsNull)
			inRow = false
		}
	}

	// Phase 4: Save in-progress row as partial state for the next call.
	// This happens when a chunk boundary falls mid-row, or when we've
	// filled the batch but the current row hasn't been finalized yet
	// (we haven't seen the next rep=0 entry).
	if inRow {
		r.partialRow = currentElements
		r.partialIsNull = currentIsNull
		r.hasPartialRow = true
	}

	if rowsFilled < rowsToRead {
		return nil, errors.Newf(
			"expected %d rows from LIST column %q, but only %d were available",
			rowsToRead, r.listInfo.columnName, rowsFilled)
	}

	return batch, nil
}
