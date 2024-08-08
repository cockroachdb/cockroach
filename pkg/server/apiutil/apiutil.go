// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package apiutil

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
)

// WriteJSONResponse returns a payload as JSON to the HTTP client.
func WriteJSONResponse(ctx context.Context, w http.ResponseWriter, code int, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	res, err := json.Marshal(payload)
	if err != nil {
		srverrors.APIV2InternalError(ctx, err, w)
		return
	}
	_, _ = w.Write(res)
}

func GetQueryStringVal(queryStringVals url.Values, arg string) QueryStringVal[string] {
	if queryStringVals.Has(arg) {
		return QueryStringVal[string]{Value: queryStringVals.Get(arg), Exists: true}
	}
	return QueryStringVal[string]{Exists: false}
}

func GetIntQueryStringVal(queryStringVals url.Values, arg string) QueryStringVal[int] {
	if queryStringVals.Has(arg) {
		queryArgStr := queryStringVals.Get(arg)
		queryArgInt, err := strconv.Atoi(queryArgStr)
		if err == nil {
			return QueryStringVal[int]{Value: queryArgInt, Exists: true}
		}
	}
	return QueryStringVal[int]{Exists: false}
}

func GetIntQueryStringVals(queryStringVals url.Values, arg string) QueryStringVal[[]int] {
	if queryStringVals.Has(arg) {
		queryArgStrs := queryStringVals[arg]
		queryArgInts := make([]int, 0, len(queryArgStrs))
		for _, a := range queryArgStrs {

			i, err := strconv.Atoi(a)
			if err != nil {
				return QueryStringVal[[]int]{Exists: false}
			}
			queryArgInts = append(queryArgInts, i)
		}

		return QueryStringVal[[]int]{Value: queryArgInts, Exists: true}
	}
	return QueryStringVal[[]int]{Exists: false}
}

type QueryStringVal[T any] struct {
	Value  T
	Exists bool
}
