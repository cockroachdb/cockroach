// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfra

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// DecodeDatum decodes the given bytes slice into a datum of the given type. It
// returns an error if the decoding is not valid, or if there are any remaining
// bytes.
func DecodeDatum(datumAlloc *tree.DatumAlloc, typ *types.T, data []byte) (tree.Datum, error) {
	datum, rem, err := valueside.Decode(datumAlloc, typ, data)
	if err != nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(err,
			"error decoding %d bytes", errors.Safe(len(data)))
	}
	if len(rem) != 0 {
		return nil, errors.AssertionFailedf(
			"%d trailing bytes in encoded value", errors.Safe(len(rem)))
	}
	return datum, nil
}

// HydrateTypesInDatumInfo hydrates all user-defined types in the provided
// DatumInfo slice.
func HydrateTypesInDatumInfo(
	ctx context.Context, resolver *descs.DistSQLTypeResolver, info []execinfrapb.DatumInfo,
) error {
	for i := range info {
		if err := typedesc.EnsureTypeIsHydrated(ctx, info[i].Type, resolver); err != nil {
			return err
		}
	}
	return nil
}

// IncludeRUEstimateInExplainAnalyze determines whether EXPLAIN
// ANALYZE should return an estimate for the number of RUs consumed by
// tenants.
//
// This setting is defined here instead of in package 'sql' to avoid
// a dependency cycle.
var IncludeRUEstimateInExplainAnalyze = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.tenant_ru_estimation.enabled",
	"determines whether EXPLAIN ANALYZE should return an estimate for the query's RU consumption",
	true,
	settings.WithName("sql.explain_analyze.include_ru_estimation.enabled"),
)
