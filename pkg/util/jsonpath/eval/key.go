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

func (ctx *jsonpathCtx) evalKey(
	key jsonpath.Key, jsonValue json.JSON, unwrap bool,
) ([]json.JSON, error) {
	if jsonValue.Type() == json.ObjectJSONType {
		val, err := jsonValue.FetchValKey(string(key))
		if err != nil {
			return nil, err
		}
		if val == nil && ctx.strict {
			return nil, pgerror.Newf(pgcode.SQLJSONMemberNotFound, "JSON object does not contain key %q", string(key))
		} else if val != nil {
			return []json.JSON{val}, nil
		}
		return []json.JSON{}, nil
	} else if unwrap && jsonValue.Type() == json.ArrayJSONType {
		return ctx.unwrapCurrentTargetAndEval(key, jsonValue, false /* unwrapNext */)
	} else if ctx.strict {
		return nil, pgerror.Newf(pgcode.SQLJSONMemberNotFound, "jsonpath member accessor can only be applied to an object")
	}
	return []json.JSON{}, nil
}
