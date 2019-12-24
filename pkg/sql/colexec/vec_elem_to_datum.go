// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colexec

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/execerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/lib/pq/oid"
)

// PhysicalTypeColElemToDatum converts an element in a colvec to a datum of
// semtype ct. Note that this function handles nulls as well, so there is no
// need for a separate null check.
func PhysicalTypeColElemToDatum(
	col coldata.Vec, rowIdx uint16, da sqlbase.DatumAlloc, ct *types.T,
) tree.Datum {
	if col.MaybeHasNulls() {
		if col.Nulls().NullAt(rowIdx) {
			return tree.DNull
		}
	}
	switch ct.Family() {
	case types.BoolFamily:
		if col.Bool()[rowIdx] {
			return tree.DBoolTrue
		}
		return tree.DBoolFalse
	case types.IntFamily:
		switch ct.Width() {
		case 16:
			return da.NewDInt(tree.DInt(col.Int16()[rowIdx]))
		case 32:
			return da.NewDInt(tree.DInt(col.Int32()[rowIdx]))
		default:
			return da.NewDInt(tree.DInt(col.Int64()[rowIdx]))
		}
	case types.FloatFamily:
		return da.NewDFloat(tree.DFloat(col.Float64()[rowIdx]))
	case types.DecimalFamily:
		return da.NewDDecimal(tree.DDecimal{Decimal: col.Decimal()[rowIdx]})
	case types.DateFamily:
		return tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(col.Int64()[rowIdx]))
	case types.StringFamily:
		b := col.Bytes().Get(int(rowIdx))
		if ct.Oid() == oid.T_name {
			return da.NewDName(tree.DString(string(b)))
		}
		return da.NewDString(tree.DString(string(b)))
	case types.BytesFamily:
		return da.NewDBytes(tree.DBytes(col.Bytes().Get(int(rowIdx))))
	case types.OidFamily:
		return da.NewDOid(tree.MakeDOid(tree.DInt(col.Int64()[rowIdx])))
	case types.UuidFamily:
		id, err := uuid.FromBytes(col.Bytes().Get(int(rowIdx)))
		if err != nil {
			execerror.VectorizedInternalPanic(err)
		}
		return da.NewDUuid(tree.DUuid{UUID: id})
	case types.TimestampFamily:
		return da.NewDTimestamp(tree.DTimestamp{Time: col.Timestamp()[rowIdx]})
	case types.TimestampTZFamily:
		return da.NewDTimestampTZ(tree.DTimestampTZ{Time: col.Timestamp()[rowIdx]})
	case types.IntervalFamily:
		return da.NewDInterval(tree.DInterval{Duration: col.Interval()[rowIdx]})
	default:
		execerror.VectorizedInternalPanic(fmt.Sprintf("Unsupported column type %s", ct.String()))
		// This code is unreachable, but the compiler cannot infer that.
		return nil
	}
}
