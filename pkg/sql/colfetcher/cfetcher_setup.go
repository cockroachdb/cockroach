// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package colfetcher

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// cFetcherTableArgs describes the information about the index we're fetching
// from. Note that only columns that need to be fetched (i.e. requested by the
// caller) are included in the internal state.
type cFetcherTableArgs struct {
	spec fetchpb.IndexFetchSpec
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

func (a cFetcherTableArgs) RequiresRawMVCCValues() bool {
	return row.FetchSpecRequiresRawMVCCValues(a.spec)
}

func (a *cFetcherTableArgs) Release() {
	*a = cFetcherTableArgs{
		// The types are small objects, so we don't bother deeply resetting this
		// slice.
		typs: a.typs[:0],
	}
	cFetcherTableArgsPool.Put(a)
}

func (a *cFetcherTableArgs) populateTypes(cols []fetchpb.IndexFetchSpec_Column) {
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
//   - allowUnhydratedEnums, if set, indicates that the type hydration of enums
//     should be skipped. This should only be used when the enums will be
//     serialized and won't be accessed directly.
func populateTableArgs(
	ctx context.Context,
	fetchSpec *fetchpb.IndexFetchSpec,
	typeResolver *descs.DistSQLTypeResolver,
	allowUnhydratedEnums bool,
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
	for _, c := range args.spec.FetchedColumns {
		t := c.Type
		if allowUnhydratedEnums && t.Family() == types.EnumFamily {
			continue
		}
		if err := typedesc.EnsureTypeIsHydrated(ctx, t, typeResolver); err != nil {
			return nil, err
		}
	}
	for _, c := range args.spec.KeyAndSuffixColumns {
		t := c.Type
		if allowUnhydratedEnums && t.Family() == types.EnumFamily {
			continue
		}
		if err := typedesc.EnsureTypeIsHydrated(ctx, t, typeResolver); err != nil {
			return nil, err
		}
	}
	args.populateTypes(args.spec.FetchedColumns)
	for i := range args.spec.FetchedColumns {
		args.ColIdxMap.Set(args.spec.FetchedColumns[i].ColumnID, i)
	}

	return args, nil
}
