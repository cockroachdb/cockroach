// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/errors"
)

var streamAPIs = map[string]func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error){}

// RegisterStreamAPI registers a streaming replication API with an API name.
func RegisterStreamAPI(
	apiName string, api func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error),
) {
	streamAPIs[apiName] = api
}

func init() {
	streaming.StreamAPIFactoryHook = func(apiName string, evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		if api, ok := streamAPIs[apiName]; ok {
			return api(evalCtx, args)
		}
		return nil, errors.Errorf("streaming replication API %s not registered", apiName)
	}
}
