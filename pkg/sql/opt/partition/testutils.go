// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package partition

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ParseDatumPath parses a span key string like "/1/2/3".
// Only NULL and a subset of types are currently supported.
func ParseDatumPath(evalCtx *eval.Context, str string, typs []types.Family) []tree.Datum {
	var res []tree.Datum
	for i, valStr := range tree.ParsePath(str) {
		if i >= len(typs) {
			panic(errors.AssertionFailedf("invalid types"))
		}

		if valStr == "NULL" {
			res = append(res, tree.DNull)
			continue
		}
		var val tree.Datum
		var err error
		switch typs[i] {
		case types.BoolFamily:
			val, err = tree.ParseDBool(valStr)
		case types.IntFamily:
			val, err = tree.ParseDInt(valStr)
		case types.FloatFamily:
			val, err = tree.ParseDFloat(valStr)
		case types.DecimalFamily:
			val, err = tree.ParseDDecimal(valStr)
		case types.DateFamily:
			val, _, err = tree.ParseDDate(evalCtx, valStr)
		case types.TimestampFamily:
			val, _, err = tree.ParseDTimestamp(evalCtx, valStr, time.Microsecond)
		case types.TimestampTZFamily:
			val, _, err = tree.ParseDTimestampTZ(evalCtx, valStr, time.Microsecond)
		case types.StringFamily:
			val = tree.NewDString(valStr)
		case types.BytesFamily:
			val = tree.NewDBytes(tree.DBytes(valStr))
		case types.OidFamily:
			var dInt *tree.DInt
			dInt, err = tree.ParseDInt(valStr)
			if err == nil {
				var o oid.Oid
				o, err = tree.IntToOid(*dInt)
				val = tree.NewDOid(o)
			}
		case types.UuidFamily:
			val, err = tree.ParseDUuidFromString(valStr)
		case types.INetFamily:
			val, err = tree.ParseDIPAddrFromINetString(valStr)
		case types.TimeFamily:
			val, _, err = tree.ParseDTime(evalCtx, valStr, time.Microsecond)
		case types.TimeTZFamily:
			val, _, err = tree.ParseDTimeTZ(evalCtx, valStr, time.Microsecond)
		default:
			panic(errors.AssertionFailedf("type %s not supported", typs[i].String()))
		}
		if err != nil {
			panic(err)
		}
		res = append(res, val)
	}
	return res
}
