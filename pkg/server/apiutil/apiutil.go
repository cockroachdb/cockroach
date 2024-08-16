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

// GetQueryStringVal gets the string value of a Query String parameter, param, from the provided url query string
// values.
//
// Returns a QueryStringVal struct with the value, if the param exists in queryStringVals. If not, the Exists field
// will be false.
func GetQueryStringVal(queryStringVals url.Values, param string) QueryStringVal[string] {
	if queryStringVals.Has(param) {
		return QueryStringVal[string]{Value: queryStringVals.Get(param), Exists: true}
	}
	return QueryStringVal[string]{Exists: false}
}

// GetIntQueryStringVal gets the int value of a Query String parameter, param, from the provided url query string
// values.
//
// Returns a QueryStringVal struct with the value, if the param exists in queryStringVals and can be cast to an int.
// If not, the Exists field will be false.
func GetIntQueryStringVal(queryStringVals url.Values, param string) QueryStringVal[int] {
	if queryStringVals.Has(param) {
		queryArgStr := queryStringVals.Get(param)
		queryArgInt, err := strconv.Atoi(queryArgStr)
		if err == nil {
			return QueryStringVal[int]{Value: queryArgInt, Exists: true}
		}
	}
	return QueryStringVal[int]{Exists: false}
}

// GetIntQueryStringVals gets all int values of a Query String parameter, param, from the provided url query string
// values.
//
// Returns a QueryStringVal struct with the values, if the params exists in queryStringVals and can be cast to an int.
// If not, the Exists field will be false.
func GetIntQueryStringVals(queryStringVals url.Values, param string) QueryStringVal[[]int] {
	if queryStringVals.Has(param) {
		queryArgStrs := queryStringVals[param]
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
