// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachparquet

import (
	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/file"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// nonNilDefLevel represents a def level of 1, meaning that the value is non-nil.
// nilDefLevel represents a def level of 0, meaning that the value is nil.
//
// For more info on def levels, refer to
// https://github.com/apache/parquet-format/blob/master/README.md#nested-encoding.
var nonNilDefLevel = []int16{1}
var nilDefLevel = []int16{0}

type columnWriter func(d tree.Datum, w file.ColumnChunkWriter) error

func int32ColumnWriter(d tree.Datum, w file.ColumnChunkWriter) error {
	if d == tree.DNull {
		return writeNilBatch[int32](w)
	}
	di, ok := tree.AsDInt(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DInt, found %T", d)
	}
	return writeBatch[int32](w, int32(di))
}

func int64ColumnWriter(d tree.Datum, w file.ColumnChunkWriter) error {
	if d == tree.DNull {
		return writeNilBatch[int64](w)
	}
	di, ok := tree.AsDInt(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DInt, found %T", d)
	}
	return writeBatch[int64](w, int64(di))
}

func boolColumnWriter(d tree.Datum, w file.ColumnChunkWriter) error {
	if d == tree.DNull {
		return writeNilBatch[bool](w)
	}
	di, ok := tree.AsDBool(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DBool, found %T", d)
	}
	return writeBatch[bool](w, bool(di))
}

func stringColumnWriter(d tree.Datum, w file.ColumnChunkWriter) error {
	if d == tree.DNull {
		return writeNilBatch[parquet.ByteArray](w)
	}
	di, ok := tree.AsDString(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DString, found %T", d)
	}
	return writeBatch[parquet.ByteArray](w, parquet.ByteArray(di))
}

func timestampColumnWriter(d tree.Datum, w file.ColumnChunkWriter) error {
	if d == tree.DNull {
		return writeNilBatch[parquet.ByteArray](w)
	}

	_, ok := tree.AsDTimestamp(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DTimestamp, found %T", d)
	}

	fmtCtx := tree.NewFmtCtx(tree.FmtBareStrings)
	d.Format(fmtCtx)

	return writeBatch[parquet.ByteArray](w, parquet.ByteArray(fmtCtx.CloseAndGetString()))
}

func uuidColumnWriter(d tree.Datum, w file.ColumnChunkWriter) error {
	if d == tree.DNull {
		return writeNilBatch[parquet.FixedLenByteArray](w)
	}

	di, ok := tree.AsDUuid(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DUuid, found %T", d)
	}
	return writeBatch[parquet.FixedLenByteArray](w, di.UUID.GetBytes())
}

func decimalColumnWriter(d tree.Datum, w file.ColumnChunkWriter) error {
	if d == tree.DNull {
		return writeNilBatch[parquet.ByteArray](w)
	}
	di, ok := tree.AsDDecimal(d)
	if !ok {
		return pgerror.Newf(pgcode.DatatypeMismatch, "expected DDecimal, found %T", d)
	}
	return writeBatch[parquet.ByteArray](w, parquet.ByteArray(di.String()))
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

func writeBatch[T parquetDatatypes](w file.ColumnChunkWriter, v T) (err error) {
	bw, ok := w.(batchWriter[T])
	if !ok {
		return errors.AssertionFailedf("expected batchWriter of type %T, but found %T instead", []T(nil), w)
	}
	_, err = bw.WriteBatch([]T{v}, nonNilDefLevel, nil)
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
