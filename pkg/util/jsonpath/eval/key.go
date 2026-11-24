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
	errKeyAccessOnNonObject = pgerror.Newf(pgcode.SQLJSONMemberNotFound, "jsonpath member accessor can only be applied to an object")
	errWildcardOnNonObject  = pgerror.Newf(pgcode.SQLJSONObjectNotFound, "jsonpath wildcard member accessor can only be applied to an object")
)

func (ctx *jsonpathCtx) evalKey(
	key jsonpath.Key, jsonValue json.JSON, unwrap bool,
) ([]json.JSON, error) {
	if jsonValue.Type() == json.ObjectJSONType {
		val, err := jsonValue.FetchValKey(string(key))
		if err != nil {
			return nil, err
		}
		if val == nil {
			if ctx.strict {
				return nil, maybeThrowError(ctx,
					pgerror.Newf(pgcode.SQLJSONMemberNotFound, "JSON object does not contain key %q", string(key)))
			}
			return nil, nil
		}
		return []json.JSON{val}, nil
	} else if unwrap && jsonValue.Type() == json.ArrayJSONType {
		return ctx.unwrapCurrentTargetAndEval(key, jsonValue, false /* unwrapNext */)
	} else if ctx.strict {
		return nil, maybeThrowError(ctx, errKeyAccessOnNonObject)
	}
	return nil, nil
}

func (ctx *jsonpathCtx) evalAnyKey(
	anyKey jsonpath.AnyKey, jsonValue json.JSON, unwrap bool,
) ([]json.JSON, error) {
	if jsonValue.Type() == json.ObjectJSONType {
		return ctx.executeAnyItem(nil /* jsonPath */, jsonValue, !ctx.strict /* unwrapNext */)
	} else if unwrap && jsonValue.Type() == json.ArrayJSONType {
		return ctx.unwrapCurrentTargetAndEval(anyKey, jsonValue, false /* unwrapNext */)
	} else if ctx.strict {
		return nil, maybeThrowError(ctx, errWildcardOnNonObject)
	}
	return nil, nil
}
