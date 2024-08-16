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
// Returns the value of the param if it exists. Otherwise, returns an empty string.

func GetQueryStringVal(queryStringVals url.Values, param string) string {
	if queryStringVals.Has(param) {
		return queryStringVals.Get(param)
	}
	return ""
}

// GetIntQueryStringVal gets the int value of a Query String parameter, param, from the provided url query string
// values.
//
// Returns an int if the param exists and can be cast into an int. Otherwise, returns 0.
func GetIntQueryStringVal(queryStringVals url.Values, param string) int {
	if queryStringVals.Has(param) {
		queryArgStr := queryStringVals.Get(param)
		queryArgInt, err := strconv.Atoi(queryArgStr)
		if err == nil {
			return queryArgInt
		}
	}
	return 0
}

// GetIntQueryStringVals gets all int values of a Query String parameter, param, from the provided url query string
// values.
//
// Returns an int slice if the param exists and ALL the values can be cast to ints. Otherwise, returns an empty slice.
func GetIntQueryStringVals(queryStringVals url.Values, param string) []int {
	if queryStringVals.Has(param) {
		queryArgStrs := queryStringVals[param]
		queryArgInts := make([]int, 0, len(queryArgStrs))
		for _, a := range queryArgStrs {

			i, err := strconv.Atoi(a)
			if err != nil {
				return []int{}
			}
			queryArgInts = append(queryArgInts, i)
		}

		return queryArgInts
	}
	return []int{}
}

type QueryStringVal[T any] struct {
	Value  T
	Exists bool
}
