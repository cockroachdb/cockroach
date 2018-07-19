package exp

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
)

// TODO(peter): allow storage of both Datums and encoded data. The caller would
// have to know which is stored.

const (
	boolSize    = unsafe.Sizeof(tree.DBool(false))
	boolAlign   = unsafe.Alignof(tree.DBool(false))
	intSize     = unsafe.Sizeof(tree.DInt(0))
	intAlign    = unsafe.Alignof(tree.DInt(0))
	floatSize   = unsafe.Sizeof(tree.DFloat(0))
	floatAlign  = unsafe.Alignof(tree.DFloat(0))
	uuidSize    = unsafe.Sizeof(tree.DUuid{})
	uuidAlign   = unsafe.Alignof(tree.DUuid{})
	ipaddrSize  = unsafe.Sizeof(tree.DIPAddr{})
	ipaddrAlign = unsafe.Alignof(tree.DIPAddr{})
	dateSize    = unsafe.Sizeof(tree.DDate(0))
	dateAlign   = unsafe.Alignof(tree.DDate(0))
	timeSize    = unsafe.Sizeof(tree.DTime(0))
	timeAlign   = unsafe.Alignof(tree.DTime(0))
)

// RowBatchLayout ...
type RowBatchLayout struct {
	rowsAndCols uint32
	dataSize    int
	pointers    int
	offsets     []uint32
	types       []sqlbase.ColumnType
}

// MakeRowBatchLayout ...
func MakeRowBatchLayout(rows int, types ...sqlbase.ColumnType) RowBatchLayout {
	offsets := make([]uint32, len(types))
	dataOffset := uint32(4 * len(types))
	// Add space for the NULL bitmap.
	dataOffset += uint32((rows*len(types) + 7) / 8)
	ptrOffset := uint32(0)

	// WARNING: align must be a power of two.
	alloc := func(offset uint32, rows int, size, align uintptr) (uint32, uint32) {
		offset = (offset + uint32(align-1)) & ^uint32(align-1)
		return offset, offset + uint32(size)*uint32(rows)
	}

	// Perform a first pass where we pack align=8 and pointer types.
	for i := range types {
		switch types[i].SemanticType {
		case sqlbase.ColumnType_BOOL:
			offsets[i], dataOffset = alloc(dataOffset, rows, boolSize, boolAlign)

		case sqlbase.ColumnType_INT:
			offsets[i], dataOffset = alloc(dataOffset, rows, intSize, intAlign)

		case sqlbase.ColumnType_FLOAT:
			offsets[i], dataOffset = alloc(dataOffset, rows, floatSize, floatAlign)

		case sqlbase.ColumnType_UUID:
			offsets[i], dataOffset = alloc(dataOffset, rows, uuidSize, uuidAlign)

		// case sqlbase.ColumnType_IPADDR:
		// 	var v tree.DIPAddr
		// 	offsets[i], dataOffset = alloc(dataOffset, rows, ipaddrSize, ipaddrAlign)

		case sqlbase.ColumnType_DATE:
			offsets[i], dataOffset = alloc(dataOffset, rows, dateSize, dateAlign)

		case sqlbase.ColumnType_TIME:
			offsets[i], dataOffset = alloc(dataOffset, rows, timeSize, timeAlign)

		default:
			offsets[i] = ptrOffset
			ptrOffset += uint32(rows)
		}
	}

	return RowBatchLayout{
		rowsAndCols: (uint32(rows) << 16) | uint32(len(types)),
		dataSize:    int(dataOffset),
		pointers:    int(ptrOffset),
		offsets:     offsets,
		types:       types,
	}
}

// RowBatch stores a batch of rows with identical type structure. Internally,
// RowBatch stores the rows in a columnarized format where all of the values
// for each column are stored contiguously. Fixed-size column types such as
// DBool and DInt are stored by value, while variable-size column types such as
// DString and DBytes are stored by reference. The caller is required to know
// the type information and no safety checking is performed. (TODO: perform
// type checking in race builds).
//
// RowBatch.data contains both offsets to the location of each column as well
// as the column values for fixed-size columns. The start of RowBatch.data
// contains RowBatch.cols uint32 values which are the offsets. For fixed-size
// columns, the offset is a byte offset from the start of RowBatch.data to the
// start of the values for that column. For pointer-size values, the offset is
// an index into RowBatch.ptrs which is an array of unsafe.Pointers.
//
// To make this clearer, consider a RowBatch for of 2 rows for
// <BOOL,INT,STRING>. RowBatch.data will look like:
//
//     +------+------+------+---+---+---+---+----------+----------+
//     |   12 |   16 |    0 | B | B |   |   |        I |        I |
//     +------+------+------+---+---+---+---+----------+----------+
//
// The BOOL column data resides at bytes 12-14. Due to alignment, the INT
// column data resides at 16-32. The STRING column resides at RowBatch.ptrs
// 0-1.
type RowBatch struct {
	row uint32 // the row in the batch being accessed (see RowBatch.Row) NB: Go
	// 1.9 produces horrifyingly bad code for RowBatch assignment if there are
	// separate rows and cols fields of type uint16. So we combine them manually
	// here.
	rowsAndCols uint32
	data        unsafe.Pointer
	ptrs        unsafe.Pointer
}

// NewRowBatch ...
func NewRowBatch(layout RowBatchLayout) RowBatch {
	data := make([]byte, layout.dataSize)
	b := RowBatch{
		rowsAndCols: layout.rowsAndCols,
		data:        unsafe.Pointer(&data[0]),
	}
	for i := range layout.offsets {
		*b.getOffsetPtr(i) = layout.offsets[i]
	}

	if layout.pointers > 0 {
		ptrs := make([]unsafe.Pointer, layout.pointers)
		b.ptrs = unsafe.Pointer(&ptrs[0])
	}
	return b
}

func (b RowBatch) rows() int {
	return int(b.rowsAndCols >> 16)
}

func (b RowBatch) cols() int {
	return int(b.rowsAndCols & ((1 << 16) - 1))
}

func (b RowBatch) String() string {
	var buf bytes.Buffer
	for i, cols := 0, b.cols(); i < cols; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, "%d", b.getOffset(i))
	}
	return buf.String()
}

func (b RowBatch) getOffsetPtr(col int) *uint32 {
	return (*uint32)(unsafe.Pointer(uintptr(b.data) + uintptr(col)*4))
}

func (b RowBatch) getOffset(col int) uint32 {
	return *b.getOffsetPtr(col)
}

func (b RowBatch) getFixedCol(col int, size uintptr) unsafe.Pointer {
	return unsafe.Pointer(uintptr(b.data) + uintptr(b.getOffset(col)) + uintptr(b.row)*size)
}

func (b RowBatch) getPtrCol(col int) *unsafe.Pointer {
	const ptrSize = unsafe.Sizeof((*byte)(nil))
	return (*unsafe.Pointer)(unsafe.Pointer(uintptr(b.ptrs) + uintptr(b.getOffset(col)+b.row)*ptrSize))
}

// Row ...
func (b RowBatch) Row(idx int) Row {
	r := Row{RowBatch: b}
	r.row = uint32(idx)
	return r
}

// Row ...
type Row struct {
	RowBatch
}

// IsNull ...
func (r Row) IsNull(col int) bool {
	bit := uintptr(r.row * uint32(col))
	ptr := (*byte)(unsafe.Pointer(uintptr(r.data) + uintptr(4*r.cols()) + bit/8))
	return (*ptr & (1 << (bit % 8))) != 0
}

// SetNull ...
func (r Row) SetNull(col int) {
	bit := uintptr(r.row * uint32(col))
	ptr := (*byte)(unsafe.Pointer(uintptr(r.data) + uintptr(4*r.cols()) + bit/8))
	*ptr |= (1 << (bit % 8))
}

// ClearNull ...
func (r Row) ClearNull(col int) {
	bit := uintptr(r.row * uint32(col))
	ptr := (*byte)(unsafe.Pointer(uintptr(r.data) + uintptr(4*r.cols()) + bit/8))
	*ptr &^= (1 << (bit % 8))
}

// DBool ...
func (r Row) DBool(col int) *tree.DBool {
	return tree.MakeDBool(*(*tree.DBool)(r.getFixedCol(col, boolSize)))
}

// SetDBool ...
func (r Row) SetDBool(col int, v tree.DBool) {
	*(*tree.DBool)(r.getFixedCol(col, boolSize)) = v
}

// DInt ...
func (r Row) DInt(col int) *tree.DInt {
	return (*tree.DInt)(r.getFixedCol(col, intSize))
}

// SetDInt ...
func (r Row) SetDInt(col int, v tree.DInt) {
	*r.DInt(col) = v
}

// DFloat ...
func (r Row) DFloat(col int) *tree.DFloat {
	return (*tree.DFloat)(r.getFixedCol(col, floatSize))
}

// SetDFloat ...
func (r Row) SetDFloat(col int, v tree.DFloat) {
	*r.DFloat(col) = v
}

// func (r Row) DDecimal(col int) *tree.DDecimal {
// 	return nil
// }

// DString ...
func (r Row) DString(col int) *tree.DString {
	return (*tree.DString)(*r.getPtrCol(col))
}

// SetDString ...
func (r Row) SetDString(col int, v *tree.DString) {
	*r.getPtrCol(col) = unsafe.Pointer(v)
}

// DCollatedString ...
func (r Row) DCollatedString(col int) *tree.DCollatedString {
	return (*tree.DCollatedString)(*r.getPtrCol(col))
}

// SetDCollatedString ...
func (r Row) SetDCollatedString(col int, v *tree.DCollatedString) {
	*r.getPtrCol(col) = unsafe.Pointer(v)
}

// DBytes ...
func (r Row) DBytes(col int) *tree.DBytes {
	return (*tree.DBytes)(*r.getPtrCol(col))
}

// SetDBytes ...
func (r Row) SetDBytes(col int, v *tree.DBytes) {
	*r.getPtrCol(col) = unsafe.Pointer(v)
}

// DUuid ...
func (r Row) DUuid(col int) *tree.DUuid {
	return (*tree.DUuid)(r.getFixedCol(col, uuidSize))
}

// SetDUuid ...
func (r Row) SetDUuid(col int, v tree.DUuid) {
	*r.DUuid(col) = v
}

// DIPAddr ...
func (r Row) DIPAddr(col int) *tree.DIPAddr {
	return (*tree.DIPAddr)(r.getFixedCol(col, ipaddrSize))
}

// SetDIPAddr ...
func (r Row) SetDIPAddr(col int, v tree.DIPAddr) {
	*r.DIPAddr(col) = v
}

// DDate ...
func (r Row) DDate(col int) *tree.DDate {
	return (*tree.DDate)(r.getFixedCol(col, dateSize))
}

// SetDDate ...
func (r Row) SetDDate(col int, v tree.DDate) {
	*r.DDate(col) = v
}

// DTime ...
func (r Row) DTime(col int) *tree.DTime {
	return (*tree.DTime)(r.getFixedCol(col, timeSize))
}

// SetDTime ...
func (r Row) SetDTime(col int, v tree.DTime) {
	*r.DTime(col) = v
}

// func (r Row) DTimestamp(col int) *tree.DTimestamp {
// 	return nil
// }

// func (r Row) DTimestampTZ(col int) *tree.DTimestampTZ {
// 	return nil
// }

// func (r Row) DInterval(col int) *tree.DInterval {
// 	return nil
// }

// func (r Row) DJSON(col int) *tree.DJSON {
// 	return nil
// }

// func (r Row) DTuple(col int) *tree.DTuple {
// 	return nil
// }

// func (r Row) DArray(col int) *tree.DArray {
// 	return nil
// }

// func (r Row) DOid(col int) *tree.DOid {
// 	return nil
// }

// RowCompare ...
type RowCompare func(a, b Row) int

// MakeRowCompare creates a function for comparing two rows containing the
// given types and using the specified ordering. This is a primitive form of
// "compiling" a comparison function.
//
// TODO(peter): figure out how to handle NULLs. Note that checking for NULLs on
// each comparison is expensive. We really want to filter NULLs before we get
// to doing row comparisons as two rows won't compare equal if either row
// contains NULL columns that are being compared.
func MakeRowCompare(ordering sqlbase.ColumnOrdering, types []sqlbase.ColumnType) RowCompare {
	var cmp RowCompare
	// NB: the comparison function is built from back to front so the first
	// column being compared ends up as the first comparison function and the
	// remaining columns are only compared if the first column compares equal.
	for i := len(ordering) - 1; i >= 0; i-- {
		col, dir := ordering[i].ColIdx, ordering[i].Direction

		// TODO(peter): it is marginally faster if these could be constants
		// allowing different "specializations" of the comparison functions.
		less, greater := -1, 1
		if dir == encoding.Descending {
			less, greater = 1, -1
		}

		// TODO(peter): write a small code generator that populates this switch
		// statement.
		switch types[col].SemanticType {
		case sqlbase.ColumnType_INT:
			if cmp == nil {
				cmp = func(a, b Row) int {
					av, bv := *a.DInt(col), *b.DInt(col)
					if av < bv {
						return less
					} else if av > bv {
						return greater
					}
					return 0
				}
			} else {
				cmp = func(a, b Row) int {
					av, bv := *a.DInt(col), *b.DInt(col)
					if av < bv {
						return less
					} else if av > bv {
						return greater
					}
					return cmp(a, b)
				}
			}
		default:
			panic("unimplemented")
		}
	}
	return cmp
}
