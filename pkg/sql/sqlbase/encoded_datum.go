// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"bytes"
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// EncodingDirToDatumEncoding returns an equivalent DatumEncoding for the given
// encoding direction.
func EncodingDirToDatumEncoding(dir encoding.Direction) DatumEncoding {
	switch dir {
	case encoding.Ascending:
		return DatumEncoding_ASCENDING_KEY
	case encoding.Descending:
		return DatumEncoding_DESCENDING_KEY
	default:
		panic(fmt.Sprintf("invalid encoding direction: %d", dir))
	}
}

// EncDatum represents a datum that is "backed" by an encoding and/or by a
// tree.Datum. It allows "passing through" a Datum without decoding and
// reencoding.
type EncDatum struct {
	// Encoding type. Valid only if encoded is not nil.
	encoding DatumEncoding

	// Encoded datum (according to the encoding field).
	encoded []byte

	// Decoded datum.
	Datum tree.Datum
}

func (ed *EncDatum) stringWithAlloc(typ *types.T, a *DatumAlloc) string {
	if ed.Datum == nil {
		if ed.encoded == nil {
			return "<unset>"
		}
		if a == nil {
			a = &DatumAlloc{}
		}
		err := ed.EnsureDecoded(typ, a)
		if err != nil {
			return fmt.Sprintf("<error: %v>", err)
		}
	}
	return ed.Datum.String()
}

func (ed *EncDatum) String(typ *types.T) string {
	return ed.stringWithAlloc(typ, nil)
}

// BytesEqual is true if the EncDatum's encoded field is equal to the input.
func (ed *EncDatum) BytesEqual(b []byte) bool {
	return bytes.Equal(ed.encoded, b)
}

// EncodedString returns an immutable copy of this EncDatum's encoded field.
func (ed *EncDatum) EncodedString() string {
	return string(ed.encoded)
}

// EncDatumOverhead is the overhead of EncDatum in bytes.
const EncDatumOverhead = unsafe.Sizeof(EncDatum{})

// Size returns a lower bound on the total size of the receiver in bytes,
// including memory referenced by the receiver.
func (ed EncDatum) Size() uintptr {
	size := EncDatumOverhead
	if ed.encoded != nil {
		size += uintptr(len(ed.encoded))
	}
	if ed.Datum != nil {
		size += ed.Datum.Size()
	}
	return size
}

// EncDatumFromEncoded initializes an EncDatum with the given encoded
// value. The encoded value is stored as a shallow copy, so the caller must
// make sure the slice is not modified for the lifetime of the EncDatum.
// The underlying Datum is nil.
func EncDatumFromEncoded(enc DatumEncoding, encoded []byte) EncDatum {
	if len(encoded) == 0 {
		panic(fmt.Sprintf("empty encoded value"))
	}
	return EncDatum{
		encoding: enc,
		encoded:  encoded,
		Datum:    nil,
	}
}

// EncDatumFromBuffer initializes an EncDatum with an encoding that is
// possibly followed by other data. Similar to EncDatumFromEncoded,
// except that this function figures out where the encoding stops and returns a
// slice for the rest of the buffer.
func EncDatumFromBuffer(typ *types.T, enc DatumEncoding, buf []byte) (EncDatum, []byte, error) {
	if len(buf) == 0 {
		return EncDatum{}, nil, errors.New("empty encoded value")
	}
	switch enc {
	case DatumEncoding_ASCENDING_KEY, DatumEncoding_DESCENDING_KEY:
		var encLen int
		var err error
		encLen, err = encoding.PeekLength(buf)
		if err != nil {
			return EncDatum{}, nil, err
		}
		ed := EncDatumFromEncoded(enc, buf[:encLen])
		return ed, buf[encLen:], nil
	case DatumEncoding_VALUE:
		typeOffset, encLen, err := encoding.PeekValueLength(buf)
		if err != nil {
			return EncDatum{}, nil, err
		}
		ed := EncDatumFromEncoded(enc, buf[typeOffset:encLen])
		return ed, buf[encLen:], nil
	default:
		panic(fmt.Sprintf("unknown encoding %s", enc))
	}
}

// EncDatumValueFromBufferWithOffsetsAndType is just like calling
// EncDatumFromBuffer with DatumEncoding_VALUE, except it expects that you pass
// in the result of calling DecodeValueTag on the input buf. Use this if you've
// already called DecodeValueTag on buf already, to avoid it getting called
// more than necessary.
func EncDatumValueFromBufferWithOffsetsAndType(
	buf []byte, typeOffset int, dataOffset int, typ encoding.Type,
) (EncDatum, []byte, error) {
	encLen, err := encoding.PeekValueLengthWithOffsetsAndType(buf, dataOffset, typ)
	if err != nil {
		return EncDatum{}, nil, err
	}
	ed := EncDatumFromEncoded(DatumEncoding_VALUE, buf[typeOffset:encLen])
	return ed, buf[encLen:], nil
}

// DatumToEncDatum initializes an EncDatum with the given Datum.
func DatumToEncDatum(ctyp *types.T, d tree.Datum) EncDatum {
	if d == nil {
		panic("Cannot convert nil datum to EncDatum")
	}

	dTyp := d.ResolvedType()
	if d != tree.DNull && !ctyp.Equivalent(dTyp) && !dTyp.IsAmbiguous() {
		panic(fmt.Sprintf("invalid datum type given: %s, expected %s", dTyp, ctyp))
	}
	return EncDatum{Datum: d}
}

// UnsetDatum ensures subsequent IsUnset() calls return false.
func (ed *EncDatum) UnsetDatum() {
	ed.encoded = nil
	ed.Datum = nil
	ed.encoding = 0
}

// IsUnset returns true if EncDatumFromEncoded or DatumToEncDatum were not called.
func (ed *EncDatum) IsUnset() bool {
	return ed.encoded == nil && ed.Datum == nil
}

// IsNull returns true if the EncDatum value is NULL. Equivalent to checking if
// ed.Datum is DNull after calling EnsureDecoded.
func (ed *EncDatum) IsNull() bool {
	if ed.Datum != nil {
		return ed.Datum == tree.DNull
	}
	if ed.encoded == nil {
		panic("IsNull on unset EncDatum")
	}
	switch ed.encoding {
	case DatumEncoding_ASCENDING_KEY, DatumEncoding_DESCENDING_KEY:
		_, isNull := encoding.DecodeIfNull(ed.encoded)
		return isNull

	case DatumEncoding_VALUE:
		_, _, _, typ, err := encoding.DecodeValueTag(ed.encoded)
		if err != nil {
			panic(err)
		}
		return typ == encoding.Null

	default:
		panic(fmt.Sprintf("unknown encoding %s", ed.encoding))
	}
}

// EnsureDecoded ensures that the Datum field is set (decoding if it is not).
func (ed *EncDatum) EnsureDecoded(typ *types.T, a *DatumAlloc) error {
	if ed.Datum != nil {
		return nil
	}
	if ed.encoded == nil {
		return errors.AssertionFailedf("decoding unset EncDatum")
	}
	var err error
	var rem []byte
	switch ed.encoding {
	case DatumEncoding_ASCENDING_KEY:
		ed.Datum, rem, err = DecodeTableKey(a, typ, ed.encoded, encoding.Ascending)
	case DatumEncoding_DESCENDING_KEY:
		ed.Datum, rem, err = DecodeTableKey(a, typ, ed.encoded, encoding.Descending)
	case DatumEncoding_VALUE:
		ed.Datum, rem, err = DecodeTableValue(a, typ, ed.encoded)
	default:
		return errors.AssertionFailedf("unknown encoding %d", log.Safe(ed.encoding))
	}
	if err != nil {
		return errors.Wrapf(err, "error decoding %d bytes", log.Safe(len(ed.encoded)))
	}
	if len(rem) != 0 {
		ed.Datum = nil
		return errors.AssertionFailedf(
			"%d trailing bytes in encoded value: %+v", log.Safe(len(rem)), rem)
	}
	return nil
}

// Encoding returns the encoding that is already available (the latter indicated
// by the bool return value).
func (ed *EncDatum) Encoding() (DatumEncoding, bool) {
	if ed.encoded == nil {
		return 0, false
	}
	return ed.encoding, true
}

// Encode appends the encoded datum to the given slice using the requested
// encoding.
// Note: DatumEncoding_VALUE encodings are not unique because they can contain
// a column ID so they should not be used to test for equality.
func (ed *EncDatum) Encode(
	typ *types.T, a *DatumAlloc, enc DatumEncoding, appendTo []byte,
) ([]byte, error) {
	if ed.encoded != nil && enc == ed.encoding {
		// We already have an encoding that matches that we can use.
		return append(appendTo, ed.encoded...), nil
	}
	if err := ed.EnsureDecoded(typ, a); err != nil {
		return nil, err
	}
	switch enc {
	case DatumEncoding_ASCENDING_KEY:
		return EncodeTableKey(appendTo, ed.Datum, encoding.Ascending)
	case DatumEncoding_DESCENDING_KEY:
		return EncodeTableKey(appendTo, ed.Datum, encoding.Descending)
	case DatumEncoding_VALUE:
		return EncodeTableValue(appendTo, ColumnID(encoding.NoColumnID), ed.Datum, a.scratch)
	default:
		panic(fmt.Sprintf("unknown encoding requested %s", enc))
	}
}

// Compare returns:
//    -1 if the receiver is less than rhs,
//    0  if the receiver is equal to rhs,
//    +1 if the receiver is greater than rhs.
func (ed *EncDatum) Compare(
	typ *types.T, a *DatumAlloc, evalCtx *tree.EvalContext, rhs *EncDatum,
) (int, error) {
	// TODO(radu): if we have both the Datum and a key encoding available, which
	// one would be faster to use?
	if ed.encoding == rhs.encoding && ed.encoded != nil && rhs.encoded != nil {
		switch ed.encoding {
		case DatumEncoding_ASCENDING_KEY:
			return bytes.Compare(ed.encoded, rhs.encoded), nil
		case DatumEncoding_DESCENDING_KEY:
			return bytes.Compare(rhs.encoded, ed.encoded), nil
		}
	}
	if err := ed.EnsureDecoded(typ, a); err != nil {
		return 0, err
	}
	if err := rhs.EnsureDecoded(typ, a); err != nil {
		return 0, err
	}
	return ed.Datum.Compare(evalCtx, rhs.Datum), nil
}

// GetInt decodes an EncDatum that is known to be of integer type and returns
// the integer value. It is a more convenient and more efficient alternative to
// calling EnsureDecoded and casting the Datum.
func (ed *EncDatum) GetInt() (int64, error) {
	if ed.Datum != nil {
		if ed.Datum == tree.DNull {
			return 0, errors.Errorf("NULL INT value")
		}
		return int64(*ed.Datum.(*tree.DInt)), nil
	}

	switch ed.encoding {
	case DatumEncoding_ASCENDING_KEY:
		if _, isNull := encoding.DecodeIfNull(ed.encoded); isNull {
			return 0, errors.Errorf("NULL INT value")
		}
		_, val, err := encoding.DecodeVarintAscending(ed.encoded)
		return val, err

	case DatumEncoding_DESCENDING_KEY:
		if _, isNull := encoding.DecodeIfNull(ed.encoded); isNull {
			return 0, errors.Errorf("NULL INT value")
		}
		_, val, err := encoding.DecodeVarintDescending(ed.encoded)
		return val, err

	case DatumEncoding_VALUE:
		_, dataOffset, _, typ, err := encoding.DecodeValueTag(ed.encoded)
		if err != nil {
			return 0, err
		}
		// NULL, true, and false are special, because their values are fully encoded by their value tag.
		if typ == encoding.Null {
			return 0, errors.Errorf("NULL INT value")
		}

		_, val, err := encoding.DecodeUntaggedIntValue(ed.encoded[dataOffset:])
		return val, err

	default:
		return 0, errors.Errorf("unknown encoding %s", ed.encoding)
	}
}

// EncDatumRow is a row of EncDatums.
type EncDatumRow []EncDatum

func (r EncDatumRow) stringToBuf(types []types.T, a *DatumAlloc, b *bytes.Buffer) {
	if len(types) != len(r) {
		panic(fmt.Sprintf("mismatched types (%v) and row (%v)", types, r))
	}
	b.WriteString("[")
	for i := range r {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(r[i].stringWithAlloc(&types[i], a))
	}
	b.WriteString("]")
}

// Copy makes a copy of this EncDatumRow. Convenient for tests. Use an
// EncDatumRowAlloc in non-test code.
func (r EncDatumRow) Copy() EncDatumRow {
	if r == nil {
		return nil
	}
	rCopy := make(EncDatumRow, len(r))
	copy(rCopy, r)
	return rCopy
}

func (r EncDatumRow) String(types []types.T) string {
	var b bytes.Buffer
	r.stringToBuf(types, &DatumAlloc{}, &b)
	return b.String()
}

// EncDatumRowOverhead is the overhead of EncDatumRow in bytes.
const EncDatumRowOverhead = unsafe.Sizeof(EncDatumRow{})

// Size returns a lower bound on the total size all EncDatum's in the receiver,
// including memory referenced by all EncDatum's.
func (r EncDatumRow) Size() uintptr {
	size := EncDatumRowOverhead
	for _, ed := range r {
		size += ed.Size()
	}
	return size
}

// EncDatumRowToDatums converts a given EncDatumRow to a Datums.
func EncDatumRowToDatums(
	types []types.T, datums tree.Datums, row EncDatumRow, da *DatumAlloc,
) error {
	if len(types) != len(row) {
		panic(fmt.Sprintf("mismatched types (%v) and row (%v)", types, row))
	}
	if len(row) != len(datums) {
		return errors.Errorf(
			"Length mismatch (%d and %d) between datums and row", len(datums), len(row))
	}
	for i, encDatum := range row {
		if encDatum.IsUnset() {
			datums[i] = tree.DNull
			continue
		}
		err := encDatum.EnsureDecoded(&types[i], da)
		if err != nil {
			return err
		}
		datums[i] = encDatum.Datum
	}
	return nil
}

// Compare returns the relative ordering of two EncDatumRows according to a
// ColumnOrdering:
//   -1 if the receiver comes before the rhs in the ordering,
//   +1 if the receiver comes after the rhs in the ordering,
//   0 if the relative order does not matter (i.e. the two rows have the same
//     values for the columns in the ordering).
//
// Note that a return value of 0 does not (in general) imply that the rows are
// equal; for example, rows [1 1 5] and [1 1 6] when compared against ordering
// {{0, asc}, {1, asc}} (i.e. ordered by first column and then by second
// column).
func (r EncDatumRow) Compare(
	types []types.T,
	a *DatumAlloc,
	ordering ColumnOrdering,
	evalCtx *tree.EvalContext,
	rhs EncDatumRow,
) (int, error) {
	if len(r) != len(types) || len(rhs) != len(types) {
		panic(fmt.Sprintf("length mismatch: %d types, %d lhs, %d rhs\n%+v\n%+v\n%+v", len(types), len(r), len(rhs), types, r, rhs))
	}
	for _, c := range ordering {
		cmp, err := r[c.ColIdx].Compare(&types[c.ColIdx], a, evalCtx, &rhs[c.ColIdx])
		if err != nil {
			return 0, err
		}
		if cmp != 0 {
			if c.Direction == encoding.Descending {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
}

// CompareToDatums is a version of Compare which compares against decoded Datums.
func (r EncDatumRow) CompareToDatums(
	types []types.T,
	a *DatumAlloc,
	ordering ColumnOrdering,
	evalCtx *tree.EvalContext,
	rhs tree.Datums,
) (int, error) {
	for _, c := range ordering {
		if err := r[c.ColIdx].EnsureDecoded(&types[c.ColIdx], a); err != nil {
			return 0, err
		}
		cmp := r[c.ColIdx].Datum.Compare(evalCtx, rhs[c.ColIdx])
		if cmp != 0 {
			if c.Direction == encoding.Descending {
				cmp = -cmp
			}
			return cmp, nil
		}
	}
	return 0, nil
}

// EncDatumRows is a slice of EncDatumRows having the same schema.
type EncDatumRows []EncDatumRow

func (r EncDatumRows) String(types []types.T) string {
	var a DatumAlloc
	var b bytes.Buffer
	b.WriteString("[")
	for i, r := range r {
		if i > 0 {
			b.WriteString(" ")
		}
		r.stringToBuf(types, &a, &b)
	}
	b.WriteString("]")
	return b.String()
}

// EncDatumRowContainer holds rows and can cycle through them.
// Must be Reset upon initialization.
type EncDatumRowContainer struct {
	rows  EncDatumRows
	index int
}

// Peek returns the current element at the top of the container.
func (c *EncDatumRowContainer) Peek() EncDatumRow {
	return c.rows[c.index]
}

// Pop returns the next row from the container. Will cycle through the rows
// again if we reach the end.
func (c *EncDatumRowContainer) Pop() EncDatumRow {
	if c.index < 0 {
		c.index = len(c.rows) - 1
	}
	row := c.rows[c.index]
	c.index--
	return row
}

// Push adds a row to the container.
func (c *EncDatumRowContainer) Push(row EncDatumRow) {
	c.rows = append(c.rows, row)
	c.index = len(c.rows) - 1
}

// Reset clears the container and resets the indexes.
// Must be called upon creating a container.
func (c *EncDatumRowContainer) Reset() {
	c.rows = c.rows[:0]
	c.index = -1
}

// IsEmpty returns whether the container is "empty", which means that it's about
// to cycle through its rows again on the next Pop.
func (c *EncDatumRowContainer) IsEmpty() bool {
	return c.index == -1
}

// EncDatumRowAlloc is a helper that speeds up allocation of EncDatumRows
// (preferably of the same length).
type EncDatumRowAlloc struct {
	buf []EncDatum
	// Preallocate a small initial batch (helps cases where
	// we only allocate a few small rows).
	prealloc [16]EncDatum
}

// AllocRow allocates an EncDatumRow with the given number of columns.
func (a *EncDatumRowAlloc) AllocRow(cols int) EncDatumRow {
	if a.buf == nil {
		// First call.
		a.buf = a.prealloc[:]
	}
	if len(a.buf) < cols {
		// If the rows are small, allocate storage for a bunch of rows at once.
		bufLen := cols
		if cols <= 16 {
			bufLen *= 16
		} else if cols <= 64 {
			bufLen *= 4
		}
		a.buf = make([]EncDatum, bufLen)
	}
	// Chop off a row from buf, and limit its capacity to avoid corrupting the
	// following row in the unlikely case that the caller appends to the slice.
	result := EncDatumRow(a.buf[:cols:cols])
	a.buf = a.buf[cols:]
	return result
}

// CopyRow allocates an EncDatumRow and copies the given row to it.
func (a *EncDatumRowAlloc) CopyRow(row EncDatumRow) EncDatumRow {
	rowCopy := a.AllocRow(len(row))
	copy(rowCopy, row)
	return rowCopy
}
