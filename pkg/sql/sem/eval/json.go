// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// PopulateDatumWithJSON is used for the json to record function family, like
// json_populate_record. It's less restrictive than the casting system, which
// is why it's implemented separately.
func PopulateDatumWithJSON(
	ctx context.Context, evalCtx *Context, j json.JSON, desiredType *types.T,
) (tree.Datum, error) {
	if j == json.NullJSONValue {
		return tree.DNull, nil
	}
	switch desiredType.Family() {
	case types.ArrayFamily:
		if j.Type() != json.ArrayJSONType {
			return nil, pgerror.Newf(pgcode.InvalidTextRepresentation, "expected JSON array")
		}
		n := j.Len()
		elementTyp := desiredType.ArrayContents()
		d := tree.NewDArray(elementTyp)
		d.Array = make(tree.Datums, n)
		for i := 0; i < n; i++ {
			elt, err := j.FetchValIdx(i)
			if err != nil {
				return nil, err
			}
			d.Array[i], err = PopulateDatumWithJSON(ctx, evalCtx, elt, elementTyp)
			if err != nil {
				return nil, err
			}
		}
		return d, nil
	case types.TupleFamily:
		tup := tree.NewDTupleWithLen(desiredType, len(desiredType.TupleContents()))
		for i := range tup.D {
			tup.D[i] = tree.DNull
		}
		err := PopulateRecordWithJSON(ctx, evalCtx, j, desiredType, tup)
		return tup, err
	}
	var s string
	switch j.Type() {
	case json.StringJSONType:
		t, err := j.AsText()
		if err != nil {
			return nil, err
		}
		if t == nil {
			return nil, errors.AssertionFailedf("JSON NULL value was checked above")
		}
		s = *t
	default:
		s = j.String()
	}
	return PerformCast(ctx, evalCtx, tree.NewDString(s), desiredType)
}

// PopulateRecordWithJSON is used for the json to record function family, like
// json_populate_record. Given a JSON object, a desired tuple type, and a tuple
// of the same length as the desired type, this function will populate the tuple
// by setting each named field in the tuple to the value of the key with the
// same name in the input JSON object. Fields in the tuple that are not present
// in the JSON will not be modified, and JSON keys that are not in the tuple
// will be ignored.
// Each field will be set by a best-effort coercion to its type from the JSON
// field. The logic is more permissive than casts.
func PopulateRecordWithJSON(
	ctx context.Context, evalCtx *Context, j json.JSON, desiredType *types.T, tup *tree.DTuple,
) error {
	if j.Type() != json.ObjectJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue, "expected JSON object")
	}
	tupleTypes := desiredType.TupleContents()
	labels := desiredType.TupleLabels()
	if labels == nil {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"anonymous records cannot be used with json{b}_populate_record{set}",
		)
	}
	for i := range tupleTypes {
		val, err := j.FetchValKey(labels[i])
		if err != nil || val == nil {
			// No value? Use the value that was already in the tuple.
			continue
		}
		tup.D[i], err = PopulateDatumWithJSON(ctx, evalCtx, val, tupleTypes[i])
		if err != nil {
			return err
		}
	}
	return nil
}
