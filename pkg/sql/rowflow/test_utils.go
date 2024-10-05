// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowflow

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// MakeTestRouter creates a router to be used by tests.
func MakeTestRouter(
	ctx context.Context,
	flowCtx *execinfra.FlowCtx,
	spec *execinfrapb.OutputRouterSpec,
	streams []execinfra.RowReceiver,
	types []*types.T,
	wg *sync.WaitGroup,
) (execinfra.RowReceiver, error) {
	r, err := makeRouter(spec, streams)
	if err != nil {
		return nil, err
	}
	r.init(ctx, flowCtx, 0 /* processorID */, types)
	r.Start(ctx, wg, nil /* flowCtxCancel */)
	return r, nil
}
