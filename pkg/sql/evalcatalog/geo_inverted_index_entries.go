// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package evalcatalog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// NumGeometryInvertedIndexEntries is part of the eval.CatalogBuiltins
// interface.
func (ec *Builtins) NumGeometryInvertedIndexEntries(
	ctx context.Context, tableID catid.DescID, indexID catid.IndexID, g *tree.DGeometry,
) (int, error) {
	geoConfig, err := getIndexGeoConfig(ctx, ec.dc, ec.txn, tableID, indexID)
	if err != nil {
		return 0, err
	}
	if geoConfig.S2Geometry == nil {
		return 0, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"index_id %d is not a geography inverted index", indexID,
		)
	}
	keys, err := rowenc.EncodeGeoInvertedIndexTableKeys(ctx, g, nil, geoConfig)
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

// NumGeographyInvertedIndexEntries is part of the eval.CatalogBuiltins
// interface.
func (ec *Builtins) NumGeographyInvertedIndexEntries(
	ctx context.Context, tableID catid.DescID, indexID catid.IndexID, g *tree.DGeography,
) (int, error) {
	geoConfig, err := getIndexGeoConfig(ctx, ec.dc, ec.txn, tableID, indexID)
	if err != nil {
		return 0, err
	}
	if geoConfig.S2Geography == nil {
		return 0, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"index_id %d is not a geography inverted index", indexID,
		)
	}
	keys, err := rowenc.EncodeGeoInvertedIndexTableKeys(ctx, g, nil, geoConfig)
	if err != nil {
		return 0, err
	}
	return len(keys), nil
}

func getIndexGeoConfig(
	ctx context.Context,
	dc *descs.Collection,
	txn *kv.Txn,
	tableID catid.DescID,
	indexID catid.IndexID,
) (geopb.Config, error) {
	tableDesc, err := dc.ByIDWithLeased(txn).WithoutNonPublic().Get().Table(ctx, tableID)
	if err != nil {
		return geopb.Config{}, err
	}
	index, err := catalog.MustFindIndexByID(tableDesc, indexID)
	if err != nil {
		return geopb.Config{}, err
	}
	return index.GetGeoConfig(), nil
}
