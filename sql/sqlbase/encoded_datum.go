// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package sqlbase

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/pkg/errors"
)

// EncDatum represents a datum that is "backed" by an encoding and/or by a
// parser.Datum. It allows "passing through" a Datum without decoding and
// reencoding. TODO(radu): It will also allow comparing encoded datums directly
// (for certain encodings).
type EncDatum struct {
	Type ColumnType_Kind

	// Encoding type. Valid only if encoded is not nil.
	encoding DatumEncoding

	// Encoded datum (according to the encoding field).
	encoded []byte

	// Decoded datum.
	Datum parser.Datum
}

func (ed *EncDatum) stringWithAlloc(a *DatumAlloc) string {
	if ed.Datum == nil {
		if a == nil {
			a = &DatumAlloc{}
		}
		err := ed.Decode(a)
		if err != nil {
			return fmt.Sprintf("<error: %v>", err)
		}
	}
	return ed.Datum.String()
}

func (ed *EncDatum) String() string {
	return ed.stringWithAlloc(nil)
}

// SetEncoded initializes the EncDatum with the given encoded value. The encoded
// value is stored as a shallow copy, so the caller must make sure the slice is
// not modified for the lifetime of the EncDatum.
func (ed *EncDatum) SetEncoded(typ ColumnType_Kind, enc DatumEncoding, val []byte) {
	if len(val) == 0 {
		panic("empty encoded value")
	}
	ed.Type = typ
	ed.encoding = enc
	ed.encoded = val
	ed.Datum = nil
}

// SetFromBuffer initializes the EncDatum with an encoding that is possibly
// followed by other data. Similar to SetEncoded, except that this function
// figures out where the encoding stops and returns a slice for the rest of the
// buffer.
func (ed *EncDatum) SetFromBuffer(
	typ ColumnType_Kind, enc DatumEncoding, buf []byte,
) (remaining []byte, err error) {
	switch enc {
	case DatumEncoding_ASCENDING_KEY, DatumEncoding_DESCENDING_KEY:
		encLen, err := encoding.PeekLength(buf)
		if err != nil {
			return nil, err
		}
		ed.SetEncoded(typ, enc, buf[:encLen])
		return buf[encLen:], nil
	case DatumEncoding_VALUE:
		typeOffset, encLen, err := encoding.PeekValueLength(buf)
		if err != nil {
			return nil, err
		}
		ed.SetEncoded(typ, enc, buf[typeOffset:encLen])
		return buf[encLen:], nil
	default:
		panic(fmt.Sprintf("unknown encoding %s", ed.encoding))
	}
}

// SetDatum initializes the EncDatum with the given Datum.
func (ed *EncDatum) SetDatum(typ ColumnType_Kind, d parser.Datum) {
	if d == nil {
		panic("nil datum given")
	}
	if d != parser.DNull && !typ.ToDatumType().TypeEqual(d) {
		panic(fmt.Sprintf("invalid datum type given: %s, expected %s",
			d.Type(), typ.ToDatumType().Type()))
	}
	ed.Type = typ
	ed.encoded = nil
	ed.Datum = d
}

// IsUnset returns true if SetEncoded or SetDatum were not called.
func (ed *EncDatum) IsUnset() bool {
	return ed.encoded == nil && ed.Datum == nil
}

// Decode ensures that Datum is set (decoding if necessary).
func (ed *EncDatum) Decode(a *DatumAlloc) error {
	if ed.Datum != nil {
		return nil
	}
	if ed.encoded == nil {
		panic("decoding unset EncDatum")
	}
	datType := ed.Type.ToDatumType()
	var err error
	var rem []byte
	switch ed.encoding {
	case DatumEncoding_ASCENDING_KEY:
		ed.Datum, rem, err = DecodeTableKey(a, datType, ed.encoded, encoding.Ascending)
	case DatumEncoding_DESCENDING_KEY:
		ed.Datum, rem, err = DecodeTableKey(a, datType, ed.encoded, encoding.Descending)
	case DatumEncoding_VALUE:
		ed.Datum, rem, err = DecodeTableValue(a, datType, ed.encoded)
	default:
		panic(fmt.Sprintf("unknown encoding %s", ed.encoding))
	}
	if err != nil {
		return err
	}
	if len(rem) != 0 {
		ed.Datum = nil
		return errors.Errorf("%d trailing bytes in encoded value", len(rem))
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
func (ed *EncDatum) Encode(a *DatumAlloc, enc DatumEncoding, appendTo []byte) ([]byte, error) {
	if ed.encoded != nil && enc == ed.encoding {
		// We already have an encoding that matches
		return append(appendTo, ed.encoded...), nil
	}
	if err := ed.Decode(a); err != nil {
		return nil, err
	}
	switch enc {
	case DatumEncoding_ASCENDING_KEY:
		return EncodeTableKey(appendTo, ed.Datum, encoding.Ascending)
	case DatumEncoding_DESCENDING_KEY:
		return EncodeTableKey(appendTo, ed.Datum, encoding.Descending)
	case DatumEncoding_VALUE:
		return EncodeTableValue(appendTo, ColumnID(encoding.NoColumnID), ed.Datum)
	default:
		panic(fmt.Sprintf("unknown encoding requested %s", enc))
	}
}

// Compare returns:
//    -1 if the receiver is less than rhs,
//    0  if the receiver is equal to rhs,
//    +1 if the receiver is greater than rhs.
func (ed *EncDatum) Compare(a *DatumAlloc, rhs *EncDatum) (int, error) {
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
	if err := ed.Decode(a); err != nil {
		return 0, err
	}
	if err := rhs.Decode(a); err != nil {
		return 0, err
	}
	return ed.Datum.Compare(rhs.Datum), nil
}

// EncDatumRow is a row of EncDatums.
type EncDatumRow []EncDatum

func (r EncDatumRow) stringToBuf(a *DatumAlloc, b *bytes.Buffer) {
	b.WriteString("[")
	for i := range r {
		if i > 0 {
			b.WriteString(" ")
		}
		b.WriteString(r[i].stringWithAlloc(a))
	}
	b.WriteString("]")
}

func (r EncDatumRow) String() string {
	var b bytes.Buffer
	r.stringToBuf(&DatumAlloc{}, &b)
	return b.String()
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
func (r EncDatumRow) Compare(a *DatumAlloc, ordering ColumnOrdering, rhs EncDatumRow) (int, error) {
	for _, c := range ordering {
		cmp, err := r[c.ColIdx].Compare(a, &rhs[c.ColIdx])
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

// EncDatumRows is a slice of EncDatumRows.
type EncDatumRows []EncDatumRow

func (r EncDatumRows) String() string {
	var a DatumAlloc
	var b bytes.Buffer
	b.WriteString("[")
	for i, r := range r {
		if i > 0 {
			b.WriteString(" ")
		}
		r.stringToBuf(&a, &b)
	}
	b.WriteString("]")
	return b.String()
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
