// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/jsonpath"
	"github.com/cockroachdb/errors"
)

func (ctx *jsonpathCtx) evalKey(k jsonpath.Key, current []tree.DJSON) ([]tree.DJSON, error) {
	var agg []tree.DJSON
	for _, res := range current {
		if res.JSON.Type() == json.ObjectJSONType {
			val, err := res.JSON.FetchValKey(string(k))
			if err != nil {
				return nil, err
			}
			if val == nil {
				if ctx.strict {
					return nil, pgerror.Newf(pgcode.SQLJSONMemberNotFound, "JSON object does not contain key %q", string(k))
				}
				continue
			}
			agg = append(agg, *ctx.a.NewDJSON(tree.DJSON{JSON: val}))
		} else if !ctx.strict && res.JSON.Type() == json.ArrayJSONType {
			arr, ok := res.JSON.AsArray()
			if !ok {
				return nil, errors.AssertionFailedf("array expected")
			}
			for _, elem := range arr {
				results, err := ctx.eval(k, []tree.DJSON{*ctx.a.NewDJSON(tree.DJSON{JSON: elem})})
				if err != nil {
					return nil, err
				}
				agg = append(agg, results...)
			}
		} else if ctx.strict {
			return nil, pgerror.Newf(pgcode.SQLJSONMemberNotFound, "jsonpath member accessor can only be applied to an object")
		}
	}
	return agg, nil
}
