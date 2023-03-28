// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package parquet

import (
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// batchAlloc pre-allocates the arrays required to pass encoded datums to the
// WriteBatch method on file.ColumnChunkWriter implementations (ex.
// (*file.BooleanColumnChunkWriter) WriteBatch).
//
// This scheme works because the arrays are merely used as "carriers" to pass
// multiple encoded datums to WriteBatch. Every WriteBatch implementation
// synchronously copies values out of the array and returns without having saved
// a reference to the array for re-use.
//
// This means any array below will not be in use outside the writeBatch
// function below.
type batchAlloc struct {
	_                      util.NoCopy
	boolBatch              [1]bool
	int32Batch             [1]int32
	int64Batch             [1]int64
	byteArrayBatch         [1]parquet.ByteArray
	fixedLenByteArrayBatch [1]parquet.FixedLenByteArray
}

// nonNilDefLevel represents a definition level of 1, meaning that the value is non-nil.
// nilDefLevel represents a definition level of 0, meaning that the value is nil.
//
// For more info on definition levels, refer to
// https://github.com/apache/parquet-format/blob/master/README.md#nested-encoding.
var nonNilDefLevel = []int16{1}
var nilDefLevel = []int16{0}

// A writeFn encodes a datum and writes it using the provided column chunk writer.
type writeFn func(d tree.Datum, w file.ColumnChunkWriter, a *batchAlloc) error

func writeInt32(d tree.Datum, w file.ColumnChunkWriter, a *batchAlloc) error {
	if d == tree.DNull {
		return writeNilBatch[int32](w)
	}
	di, ok := tree.AsDInt(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DInt, found %T", d)
	}
	return writeBatch[int32](w, a.int32Batch[:], int32(di))
}

func writeInt64(d tree.Datum, w file.ColumnChunkWriter, a *batchAlloc) error {
	if d == tree.DNull {
		return writeNilBatch[int64](w)
	}
	di, ok := tree.AsDInt(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DInt, found %T", d)
	}
	return writeBatch[int64](w, a.int64Batch[:], int64(di))
}

func writeBool(d tree.Datum, w file.ColumnChunkWriter, a *batchAlloc) error {
	if d == tree.DNull {
		return writeNilBatch[bool](w)
	}
	di, ok := tree.AsDBool(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DBool, found %T", d)
	}
	return writeBatch[bool](w, a.boolBatch[:], bool(di))
}

func writeString(d tree.Datum, w file.ColumnChunkWriter, a *batchAlloc) error {
	if d == tree.DNull {
		return writeNilBatch[parquet.ByteArray](w)
	}
	di, ok := tree.AsDString(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DString, found %T", d)
	}
	var b parquet.ByteArray
	b, err := json.UnsafeGetBytes(string(di))
	if err != nil {
		return err
	}
	// UnsafeGetBytes may return a []byte(nil) instead of a []byte{}. Using the
	// former will result in parquet readers decoding the binary data into
	// [1]byte{'\x00'}, which is incorrect because it represents a string of
	// length 1 instead of 0.
	if len(b) == 0 {
		b = []byte{}
	}
	return writeBatch[parquet.ByteArray](w, a.byteArrayBatch[:], b)
}

func writeTimestamp(d tree.Datum, w file.ColumnChunkWriter, a *batchAlloc) error {
	if d == tree.DNull {
		return writeNilBatch[parquet.ByteArray](w)
	}

	_, ok := tree.AsDTimestamp(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DTimestamp, found %T", d)
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtBareStrings)
	d.Format(fmtCtx)

	return writeBatch[parquet.ByteArray](w, a.byteArrayBatch[:], parquet.ByteArray(fmtCtx.CloseAndGetString()))
}

func writeUUID(d tree.Datum, w file.ColumnChunkWriter, a *batchAlloc) error {
	if d == tree.DNull {
		return writeNilBatch[parquet.FixedLenByteArray](w)
	}

	di, ok := tree.AsDUuid(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DUuid, found %T", d)
	}
	return writeBatch[parquet.FixedLenByteArray](w, a.fixedLenByteArrayBatch[:], di.UUID.GetBytes())
}

func writeDecimal(d tree.Datum, w file.ColumnChunkWriter, a *batchAlloc) error {
	if d == tree.DNull {
		return writeNilBatch[parquet.ByteArray](w)
	}
	di, ok := tree.AsDDecimal(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DDecimal, found %T", d)
	}
	return writeBatch[parquet.ByteArray](w, a.byteArrayBatch[:], parquet.ByteArray(di.String()))
}

// parquetDatatypes are the physical types used in the parquet library.
type parquetDatatypes interface {
	bool | int32 | int64 | parquet.ByteArray | parquet.FixedLenByteArray
}

// batchWriter is an interface representing parquet column chunk writers such as
// file.Int64ColumnChunkWriter and file.BooleanColumnChunkWriter.
type batchWriter[T parquetDatatypes] interface {
	WriteBatch(values []T, defLevels, repLevels []int16) (valueOffset int64, err error)
}

func writeBatch[T parquetDatatypes](w file.ColumnChunkWriter, batchAlloc []T, v T) (err error) {
	bw, ok := w.(batchWriter[T])
	if !ok {
		return errors.AssertionFailedf("expected batchWriter of type %T, but found %T instead", []T(nil), w)
	}

	batchAlloc[0] = v
	_, err = bw.WriteBatch(batchAlloc, nonNilDefLevel, nil)
	return err
}

func writeNilBatch[T parquetDatatypes](w file.ColumnChunkWriter) (err error) {
	bw, ok := w.(batchWriter[T])
	if !ok {
		return errors.AssertionFailedf("expected batchWriter of type %T, but found %T instead", []T(nil), w)
	}
	_, err = bw.WriteBatch([]T(nil), nilDefLevel, nil)
	return err
}
