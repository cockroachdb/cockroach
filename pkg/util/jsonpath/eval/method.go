// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
)

var (
	errEvalSizeNotArray = pgerror.Newf(pgcode.SQLJSONArrayNotFound, "jsonpath item method .size() can only be applied to an array")
)

func (ctx *jsonpathCtx) evalMethod(path jsonpath.Path, jsonValue json.JSON) ([]json.JSON, error) {
	switch path.(type) {
	case jsonpath.Size:
		size, err := ctx.evalSize(jsonValue)
		if err != nil {
			return nil, err
		}
		return []json.JSON{json.FromInt(size)}, nil
	default:
		return nil, errUnimplemented
	}
}

func (ctx *jsonpathCtx) evalSize(jsonValue json.JSON) (int, error) {
	if jsonValue.Type() != json.ArrayJSONType {
		if ctx.strict {
			return -1, errEvalSizeNotArray
		}
		return 1, nil
	}
	return jsonValue.Len(), nil
}
