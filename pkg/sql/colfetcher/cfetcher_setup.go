// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colfetcher

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// cFetcherTableArgs describes the information about the index we're fetching
// from. Note that only columns that need to be fetched (i.e. requested by the
// caller) are included in the internal state.
type cFetcherTableArgs struct {
	spec descpb.IndexFetchSpec
	// ColIdxMap is a mapping from ColumnID to the ordinal of the corresponding
	// column within spec.FetchedColumns.
	ColIdxMap catalog.TableColMap
	// typs are the types from spec.FetchedColumns.
	typs []*types.T
}

var cFetcherTableArgsPool = sync.Pool{
	New: func() interface{} {
		return &cFetcherTableArgs{}
	},
}

func (a *cFetcherTableArgs) Release() {
	*a = cFetcherTableArgs{
		// The types are small objects, so we don't bother deeply resetting this
		// slice.
		typs: a.typs[:0],
	}
	cFetcherTableArgsPool.Put(a)
}

func (a *cFetcherTableArgs) populateTypes(cols []descpb.IndexFetchSpec_Column) {
	if cap(a.typs) < len(cols) {
		a.typs = make([]*types.T, len(cols))
	} else {
		a.typs = a.typs[:len(cols)]
	}
	for i := range cols {
		a.typs[i] = cols[i].Type
	}
}

// populateTableArgs fills in cFetcherTableArgs.
func populateTableArgs(
	ctx context.Context, flowCtx *execinfra.FlowCtx, fetchSpec *descpb.IndexFetchSpec,
) (_ *cFetcherTableArgs, _ error) {
	args := cFetcherTableArgsPool.Get().(*cFetcherTableArgs)

	*args = cFetcherTableArgs{
		spec: *fetchSpec,
		typs: args.typs,
	}
	// Before we can safely use types from the fetch spec, we need to make sure
	// they are hydrated. In row execution engine it is done during the processor
	// initialization, but neither ColBatchScan nor cFetcher are processors, so we
	// need to do the hydration ourselves.
	resolver := flowCtx.NewTypeResolver(flowCtx.Txn)
	for i := range args.spec.FetchedColumns {
		if err := typedesc.EnsureTypeIsHydrated(ctx, args.spec.FetchedColumns[i].Type, &resolver); err != nil {
			return nil, err
		}
	}
	for i := range args.spec.KeyAndSuffixColumns {
		if err := typedesc.EnsureTypeIsHydrated(ctx, args.spec.KeyAndSuffixColumns[i].Type, &resolver); err != nil {
			return nil, err
		}
	}
	args.populateTypes(args.spec.FetchedColumns)
	for i := range args.spec.FetchedColumns {
		args.ColIdxMap.Set(args.spec.FetchedColumns[i].ColumnID, i)
	}

	return args, nil
}
