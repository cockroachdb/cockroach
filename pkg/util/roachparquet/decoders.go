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
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

type decoder interface{}

type typedDecoder[T parquetDatatypes] interface {
	decoder
	decode(v T) (tree.Datum, error)
}

func decode[T parquetDatatypes](dec decoder, v T) (tree.Datum, error) {
	td, ok := dec.(typedDecoder[T])
	if !ok {
		return nil, errors.AssertionFailedf("expected typedDecoder[%T], but found %T", v, dec)
	}
	return td.decode(v)
}

type boolDecoder struct{}

func (boolDecoder) decode(v bool) (tree.Datum, error) {
	return tree.MakeDBool(tree.DBool(v)), nil
}

type stringDecoder struct{}

func (stringDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.NewDString(string(v)), nil
}

type int64Decoder struct{}

func (int64Decoder) decode(v int64) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(v)), nil
}

type int32Decoder struct{}

func (int32Decoder) decode(v int32) (tree.Datum, error) {
	return tree.NewDInt(tree.DInt(v)), nil
}

type decimalDecoder struct{}

func (decimalDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	return tree.ParseDDecimal(string(v))
}

type timestampDecoder struct{}

func (timestampDecoder) decode(v parquet.ByteArray) (tree.Datum, error) {
	dtStr := string(v)
	d, dependsOnCtx, err := tree.ParseDTimestamp(nil, dtStr, time.Microsecond)
	if dependsOnCtx {
		return nil, errors.New("TimestampTZ depends on context")
	}
	if err != nil {
		return nil, err
	}
	// Converts the timezone from "loc(+0000)" to "UTC", which are equivalent.
	d.Time = d.Time.UTC()
	return d, nil
}

type uUIDDecoder struct{}

func (uUIDDecoder) decode(v parquet.FixedLenByteArray) (tree.Datum, error) {
	uid, err := uuid.FromBytes(v)
	if err != nil {
		return nil, err
	}
	return tree.NewDUuid(tree.DUuid{UUID: uid}), nil
}

// Defeat the linter.
var _, _ = boolDecoder{}.decode(false)
var _, _ = stringDecoder{}.decode(parquet.ByteArray{})
var _, _ = int32Decoder{}.decode(0)
var _, _ = int64Decoder{}.decode(0)
var _, _ = decimalDecoder{}.decode(parquet.ByteArray{})
var _, _ = timestampDecoder{}.decode(parquet.ByteArray{})
var _, _ = uUIDDecoder{}.decode(parquet.FixedLenByteArray{})
