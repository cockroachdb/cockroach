// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package indexstorageparam implements storageparam.Setter for a
// descpb.IndexDescriptor.
package indexstorageparam

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// Setter observes storage parameters for indexes.
type Setter struct {
	IndexDesc *descpb.IndexDescriptor
}

var _ storageparam.Setter = (*Setter)(nil)

func getS2ConfigFromIndex(indexDesc *descpb.IndexDescriptor) *geopb.S2Config {
	var s2Config *geopb.S2Config
	if indexDesc.GeoConfig.S2Geometry != nil {
		s2Config = indexDesc.GeoConfig.S2Geometry.S2Config
	}
	if indexDesc.GeoConfig.S2Geography != nil {
		s2Config = indexDesc.GeoConfig.S2Geography.S2Config
	}
	return s2Config
}

func (po *Setter) applyS2ConfigSetting(
	ctx context.Context, evalCtx *eval.Context, key string, expr tree.Datum, min int64, max int64,
) error {
	s2Config := getS2ConfigFromIndex(po.IndexDesc)
	if s2Config == nil {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"index setting %q can only be set on GEOMETRY or GEOGRAPHY spatial indexes",
			key,
		)
	}

	val, err := paramparse.DatumAsInt(ctx, evalCtx, key, expr)
	if err != nil {
		return errors.Wrapf(err, "error decoding %q", key)
	}
	if val < min || val > max {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"%q value must be between %d and %d inclusive",
			key,
			min,
			max,
		)
	}
	switch key {
	case `s2_max_level`:
		s2Config.MaxLevel = int32(val)
	case `s2_level_mod`:
		s2Config.LevelMod = int32(val)
	case `s2_max_cells`:
		s2Config.MaxCells = int32(val)
	}

	return nil
}

func (po *Setter) applyGeometryIndexSetting(
	ctx context.Context, evalCtx *eval.Context, key string, expr tree.Datum,
) error {
	if po.IndexDesc.GeoConfig.S2Geometry == nil {
		return pgerror.Newf(pgcode.InvalidParameterValue, "%q can only be applied to GEOMETRY spatial indexes", key)
	}
	val, err := paramparse.DatumAsFloat(ctx, evalCtx, key, expr)
	if err != nil {
		return errors.Wrapf(err, "error decoding %q", key)
	}
	switch key {
	case `geometry_min_x`:
		po.IndexDesc.GeoConfig.S2Geometry.MinX = val
	case `geometry_max_x`:
		po.IndexDesc.GeoConfig.S2Geometry.MaxX = val
	case `geometry_min_y`:
		po.IndexDesc.GeoConfig.S2Geometry.MinY = val
	case `geometry_max_y`:
		po.IndexDesc.GeoConfig.S2Geometry.MaxY = val
	default:
		return pgerror.Newf(pgcode.InvalidParameterValue, "unknown key: %q", key)
	}
	return nil
}

// Set implements the Setter interface.
func (po *Setter) Set(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	key string,
	expr tree.Datum,
) error {
	switch key {
	case `fillfactor`:
		return storageparam.SetFillFactor(ctx, evalCtx, key, expr)
	case `s2_max_level`:
		return po.applyS2ConfigSetting(ctx, evalCtx, key, expr, 0, 30)
	case `s2_level_mod`:
		return po.applyS2ConfigSetting(ctx, evalCtx, key, expr, 1, 3)
	case `s2_max_cells`:
		return po.applyS2ConfigSetting(ctx, evalCtx, key, expr, 1, 32)
	case `geometry_min_x`, `geometry_max_x`, `geometry_min_y`, `geometry_max_y`:
		return po.applyGeometryIndexSetting(ctx, evalCtx, key, expr)
	// `bucket_count` is handled in schema changer when creating hash sharded
	// indexes.
	case `bucket_count`:
		return nil
	case `vacuum_cleanup_index_scale_factor`,
		`buffering`,
		`fastupdate`,
		`gin_pending_list_limit`,
		`pages_per_range`,
		`autosummarize`:
		return unimplemented.NewWithIssuef(43299, "storage parameter %q", key)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}

// Reset implements the StorageParameterObserver interface.
func (po *Setter) Reset(ctx context.Context, evalCtx *eval.Context, key string) error {
	return errors.AssertionFailedf("non-implemented codepath")
}

// RunPostChecks implements the Setter interface.
func (po *Setter) RunPostChecks() error {
	s2Config := getS2ConfigFromIndex(po.IndexDesc)
	if s2Config != nil {
		if (s2Config.MaxLevel)%s2Config.LevelMod != 0 {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"s2_max_level (%d) must be divisible by s2_level_mod (%d)",
				s2Config.MaxLevel,
				s2Config.LevelMod,
			)
		}
	}

	if cfg := po.IndexDesc.GeoConfig.S2Geometry; cfg != nil {
		if cfg.MaxX <= cfg.MinX {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"geometry_max_x (%f) must be greater than geometry_min_x (%f)",
				cfg.MaxX,
				cfg.MinX,
			)
		}
		if cfg.MaxY <= cfg.MinY {
			return pgerror.Newf(
				pgcode.InvalidParameterValue,
				"geometry_max_y (%f) must be greater than geometry_min_y (%f)",
				cfg.MaxY,
				cfg.MinY,
			)
		}
	}
	return nil
}
