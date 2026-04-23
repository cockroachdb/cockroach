// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colencoding

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// DecodeTableValueToCol decodes a value encoded by EncodeTableValue, writing
// the result to the rowIdx'th position of the vecIdx'th vector in
// coldata.TypedVecs.
// See the analog in rowenc/column_type_encoding.go.
//
// mvccTimestamp is the version timestamp of the KV value being decoded. It's
// used to resolve PENDING_COMMIT_TIMESTAMP() markers (see
// EncodeCommitTimestampValue) for ALLOW_COMMIT_TIMESTAMP columns: after
// intent resolution the version key's timestamp is the writer's commit
// timestamp, so substituting it gives the column its final value at read
// time. An empty mvccTimestamp falls back to NULL (same-txn read-your-own-
// writes don't have a finalized commit timestamp yet, and surfacing the
// txn's pre-commit provisional timestamp would lie because it can still be
// pushed forward).
func DecodeTableValueToCol(
	da *tree.DatumAlloc,
	vecs *coldata.TypedVecs,
	vecIdx int,
	rowIdx int,
	typ encoding.Type,
	dataOffset int,
	valTyp *types.T,
	buf []byte,
	mvccTimestamp hlc.Timestamp,
) ([]byte, error) {
	// NULL is special because it is a valid value for any type.
	if typ == encoding.Null {
		vecs.Nulls[vecIdx].SetNull(rowIdx)
		return buf[dataOffset:], nil
	}
	// PENDING_COMMIT_TIMESTAMP() markers carry no payload of their own; the
	// decoded value comes entirely from the MVCC version timestamp. valTyp must
	// be TIMESTAMP or TIMESTAMPTZ (the column-type restriction enforced by the
	// catalog-level ALLOW_COMMIT_TIMESTAMP validation).
	if typ == encoding.CommitTimestamp {
		if mvccTimestamp.IsEmpty() {
			vecs.Nulls[vecIdx].SetNull(rowIdx)
			return buf[dataOffset:], nil
		}
		switch valTyp.Family() {
		case types.TimestampFamily, types.TimestampTZFamily:
			colIdx := vecs.ColsMap[vecIdx]
			// Round to microseconds to match what the row engine produces via
			// tree.MakeDTimestampTZ(_, time.Microsecond). MVCC HLC physical
			// times are always within the supported range so we don't bother
			// with a bounds check.
			vecs.TimestampCols[colIdx][rowIdx] = mvccTimestamp.GoTime().Round(time.Microsecond)
			return buf[dataOffset:], nil
		default:
			return buf, errors.AssertionFailedf(
				"PENDING_COMMIT_TIMESTAMP() marker decoded for non-timestamp column type %s",
				valTyp.SQLStringForError())
		}
	}

	// Bool is special because the value is stored in the value tag, so we have
	// to keep the reference to the original slice.
	origBuf := buf
	buf = buf[dataOffset:]

	// Find the position of the target vector among the typed columns of its
	// type.
	colIdx := vecs.ColsMap[vecIdx]

	var err error
	switch valTyp.Family() {
	case types.BoolFamily:
		var b bool
		// A boolean's value is encoded in its tag directly, so we don't have an
		// "Untagged" version of this function. Note that we also use the
		// original buffer.
		buf, b, err = encoding.DecodeBoolValue(origBuf)
		vecs.BoolCols[colIdx][rowIdx] = b
	case types.BytesFamily, types.StringFamily, types.EnumFamily:
		var data []byte
		buf, data, err = encoding.DecodeUntaggedBytesValue(buf)
		vecs.BytesCols[colIdx].Set(rowIdx, data)
	case types.DateFamily:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		vecs.Int64Cols[colIdx][rowIdx] = i
	case types.DecimalFamily:
		buf, err = encoding.DecodeIntoUntaggedDecimalValue(&vecs.DecimalCols[colIdx][rowIdx], buf)
	case types.FloatFamily:
		var f float64
		buf, f, err = encoding.DecodeUntaggedFloatValue(buf)
		vecs.Float64Cols[colIdx][rowIdx] = f
	case types.IntFamily:
		var i int64
		buf, i, err = encoding.DecodeUntaggedIntValue(buf)
		switch valTyp.Width() {
		case 16:
			vecs.Int16Cols[colIdx][rowIdx] = int16(i)
		case 32:
			vecs.Int32Cols[colIdx][rowIdx] = int32(i)
		default:
			// Pre-2.1 BIT was using INT encoding with arbitrary sizes.
			// We map these to 64-bit INT now. See #34161.
			vecs.Int64Cols[colIdx][rowIdx] = i
		}
	case types.JsonFamily:
		var data []byte
		buf, data, err = encoding.DecodeUntaggedBytesValue(buf)
		vecs.JSONCols[colIdx].Bytes.Set(rowIdx, data)
	case types.UuidFamily:
		var data uuid.UUID
		buf, data, err = encoding.DecodeUntaggedUUIDValue(buf)
		// TODO(yuzefovich): we could peek inside the encoding package to skip a
		// couple of conversions.
		if err != nil {
			return buf, err
		}
		vecs.BytesCols[colIdx].Set(rowIdx, data.GetBytes())
	case types.TimestampFamily, types.TimestampTZFamily:
		var t time.Time
		buf, t, err = encoding.DecodeUntaggedTimeValue(buf)
		vecs.TimestampCols[colIdx][rowIdx] = t
	case types.IntervalFamily:
		var d duration.Duration
		buf, d, err = encoding.DecodeUntaggedDurationValue(buf)
		vecs.IntervalCols[colIdx][rowIdx] = d
	// Types backed by tree.Datums.
	default:
		var d tree.Datum
		d, buf, err = valueside.DecodeUntaggedDatum(da, valTyp, buf)
		if err != nil {
			return buf, err
		}
		vecs.DatumCols[colIdx].Set(rowIdx, d)
	}
	return buf, err
}
