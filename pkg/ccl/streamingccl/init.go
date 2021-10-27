package streamingccl

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/pkg/errors"
)


var streamAPIs = map[string]func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error){}

func RegisterStreamAPI(apiName string, api func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error)) {
	streamAPIs[apiName] = api
}

func init() {
	streaming.StreamAPIFactoryHook = func(apiName string, evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		if api, ok := streamAPIs[apiName]; !ok {
			return nil, errors.Errorf("streaming replication API %s not registered", apiName)
		} else {
			return api(evalCtx, args)
		}
	}
}
