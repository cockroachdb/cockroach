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
	"github.com/cockroachdb/errors"
)

func (ctx *jsonpathCtx) evalKey(k jsonpath.Key, current []json.JSON) ([]json.JSON, error) {
	var agg []json.JSON
	for _, j := range current {
		if j.Type() == json.ObjectJSONType {
			val, err := j.FetchValKey(string(k))
			if err != nil {
				return nil, err
			}
			if val == nil {
				if ctx.strict {
					return nil, pgerror.Newf(pgcode.SQLJSONMemberNotFound, "JSON object does not contain key %q", string(k))
				}
				continue
			}
			agg = append(agg, val)
		} else if !ctx.strict && j.Type() == json.ArrayJSONType {
			arr, ok := j.AsArray()
			if !ok {
				return nil, errors.AssertionFailedf("array expected")
			}
			for _, elem := range arr {
				results, err := ctx.eval(k, []json.JSON{elem})
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
