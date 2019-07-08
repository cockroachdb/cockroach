// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exec

import (
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/exec/coldata"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	semtypes "github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/lib/pq/oid"
)

// PhysicalTypeColElemToDatum converts an element in a colvec to a datum of semtype ct.
func PhysicalTypeColElemToDatum(
	col coldata.Vec, rowIdx uint16, da sqlbase.DatumAlloc, ct semtypes.T,
) tree.Datum {
	switch ct.Family() {
	case semtypes.BoolFamily:
		if col.Bool()[rowIdx] {
			return tree.DBoolTrue
		}
		return tree.DBoolFalse
	case semtypes.IntFamily:
		switch ct.Width() {
		case 8:
			return da.NewDInt(tree.DInt(col.Int8()[rowIdx]))
		case 16:
			return da.NewDInt(tree.DInt(col.Int16()[rowIdx]))
		case 32:
			return da.NewDInt(tree.DInt(col.Int32()[rowIdx]))
		default:
			return da.NewDInt(tree.DInt(col.Int64()[rowIdx]))
		}
	case semtypes.FloatFamily:
		return da.NewDFloat(tree.DFloat(col.Float64()[rowIdx]))
	case semtypes.DecimalFamily:
		return da.NewDDecimal(tree.DDecimal{Decimal: col.Decimal()[rowIdx]})
	case semtypes.DateFamily:
		return tree.NewDDate(pgdate.MakeCompatibleDateFromDisk(col.Int64()[rowIdx]))
	case semtypes.StringFamily:
		b := col.Bytes().Get(int(rowIdx))
		if ct.Oid() == oid.T_name {
			return da.NewDName(tree.DString(*(*string)(unsafe.Pointer(&b))))
		}
		return da.NewDString(tree.DString(*(*string)(unsafe.Pointer(&b))))
	case semtypes.BytesFamily:
		return da.NewDBytes(tree.DBytes(col.Bytes().Get(int(rowIdx))))
	case semtypes.OidFamily:
		return da.NewDOid(tree.MakeDOid(tree.DInt(col.Int64()[rowIdx])))
	default:
		panic(fmt.Sprintf("Unsupported column type %s", ct.String()))
	}
}
