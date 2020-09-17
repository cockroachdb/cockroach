// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package paramparse

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// ApplyStorageParameters applies given storage parameters with the
// given observer.
func ApplyStorageParameters(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	params tree.StorageParams,
	paramObserver StorageParamObserver,
) error {
	for _, sp := range params {
		key := string(sp.Key)
		if sp.Value == nil {
			return errors.Errorf("storage parameter %q requires a value", key)
		}
		// Expressions may be an unresolved name.
		// Cast these as strings.
		expr := UnresolvedNameToStrVal(sp.Value)

		// Convert the expressions to a datum.
		typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, types.Any)
		if err != nil {
			return err
		}
		if typedExpr, err = evalCtx.NormalizeExpr(typedExpr); err != nil {
			return err
		}
		datum, err := typedExpr.Eval(evalCtx)
		if err != nil {
			return err
		}

		// Apply the param.
		if err := paramObserver.Apply(evalCtx, key, datum); err != nil {
			return err
		}
	}
	return paramObserver.RunPostChecks()
}

// StorageParamObserver applies a storage parameter to an underlying item.
type StorageParamObserver interface {
	Apply(evalCtx *tree.EvalContext, key string, datum tree.Datum) error
	RunPostChecks() error
}

// TableStorageParamObserver observes storage parameters for tables.
type TableStorageParamObserver struct{}

var _ StorageParamObserver = (*TableStorageParamObserver)(nil)

func applyFillFactorStorageParam(evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
	val, err := DatumAsFloat(evalCtx, key, datum)
	if err != nil {
		return err
	}
	if val < 0 || val > 100 {
		return errors.Newf("%q must be between 0 and 100", key)
	}
	if evalCtx != nil {
		evalCtx.ClientNoticeSender.BufferClientNotice(
			evalCtx.Context,
			pgnotice.Newf("storage parameter %q is ignored", key),
		)
	}
	return nil
}

// RunPostChecks implements the StorageParamObserver interface.
func (a *TableStorageParamObserver) RunPostChecks() error {
	return nil
}

// Apply implements the StorageParamObserver interface.
func (a *TableStorageParamObserver) Apply(
	evalCtx *tree.EvalContext, key string, datum tree.Datum,
) error {
	switch key {
	case `fillfactor`:
		return applyFillFactorStorageParam(evalCtx, key, datum)
	case `autovacuum_enabled`:
		var boolVal bool
		if stringVal, err := DatumAsString(evalCtx, key, datum); err == nil {
			boolVal, err = ParseBoolVar(key, stringVal)
			if err != nil {
				return err
			}
		} else {
			s, err := GetSingleBool(key, datum)
			if err != nil {
				return err
			}
			boolVal = bool(*s)
		}
		if !boolVal && evalCtx != nil {
			evalCtx.ClientNoticeSender.BufferClientNotice(
				evalCtx.Context,
				pgnotice.Newf(`storage parameter "%s = %s" is ignored`, key, datum.String()),
			)
		}
		return nil
	case `toast_tuple_target`,
		`parallel_workers`,
		`toast.autovacuum_enabled`,
		`autovacuum_vacuum_threshold`,
		`toast.autovacuum_vacuum_threshold`,
		`autovacuum_vacuum_scale_factor`,
		`toast.autovacuum_vacuum_scale_factor`,
		`autovacuum_analyze_threshold`,
		`autovacuum_analyze_scale_factor`,
		`autovacuum_vacuum_cost_delay`,
		`toast.autovacuum_vacuum_cost_delay`,
		`autovacuum_vacuum_cost_limit`,
		`autovacuum_freeze_min_age`,
		`toast.autovacuum_freeze_min_age`,
		`autovacuum_freeze_max_age`,
		`toast.autovacuum_freeze_max_age`,
		`autovacuum_freeze_table_age`,
		`toast.autovacuum_freeze_table_age`,
		`autovacuum_multixact_freeze_min_age`,
		`toast.autovacuum_multixact_freeze_min_age`,
		`autovacuum_multixact_freeze_max_age`,
		`toast.autovacuum_multixact_freeze_max_age`,
		`autovacuum_multixact_freeze_table_age`,
		`toast.autovacuum_multixact_freeze_table_age`,
		`log_autovacuum_min_duration`,
		`toast.log_autovacuum_min_duration`,
		`user_catalog_table`:
		return unimplemented.NewWithIssuef(43299, "storage parameter %q", key)
	}
	return errors.Errorf("invalid storage parameter %q", key)
}

// IndexStorageParamObserver observes storage parameters for indexes.
type IndexStorageParamObserver struct {
	IndexDesc *descpb.IndexDescriptor
}

var _ StorageParamObserver = (*IndexStorageParamObserver)(nil)

func getS2ConfigFromIndex(indexDesc *descpb.IndexDescriptor) *geoindex.S2Config {
	var s2Config *geoindex.S2Config
	if indexDesc.GeoConfig.S2Geometry != nil {
		s2Config = indexDesc.GeoConfig.S2Geometry.S2Config
	}
	if indexDesc.GeoConfig.S2Geography != nil {
		s2Config = indexDesc.GeoConfig.S2Geography.S2Config
	}
	return s2Config
}

func (a *IndexStorageParamObserver) applyS2ConfigSetting(
	evalCtx *tree.EvalContext, key string, expr tree.Datum, min int64, max int64,
) error {
	s2Config := getS2ConfigFromIndex(a.IndexDesc)
	if s2Config == nil {
		return errors.Newf("index setting %q can only be set on GEOMETRY or GEOGRAPHY spatial indexes", key)
	}

	val, err := DatumAsInt(evalCtx, key, expr)
	if err != nil {
		return errors.Wrapf(err, "error decoding %q", key)
	}
	if val < min || val > max {
		return errors.Newf("%q value must be between %d and %d inclusive", key, min, max)
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

func (a *IndexStorageParamObserver) applyGeometryIndexSetting(
	evalCtx *tree.EvalContext, key string, expr tree.Datum,
) error {
	if a.IndexDesc.GeoConfig.S2Geometry == nil {
		return errors.Newf("%q can only be applied to GEOMETRY spatial indexes", key)
	}
	val, err := DatumAsFloat(evalCtx, key, expr)
	if err != nil {
		return errors.Wrapf(err, "error decoding %q", key)
	}
	switch key {
	case `geometry_min_x`:
		a.IndexDesc.GeoConfig.S2Geometry.MinX = val
	case `geometry_max_x`:
		a.IndexDesc.GeoConfig.S2Geometry.MaxX = val
	case `geometry_min_y`:
		a.IndexDesc.GeoConfig.S2Geometry.MinY = val
	case `geometry_max_y`:
		a.IndexDesc.GeoConfig.S2Geometry.MaxY = val
	default:
		return errors.Newf("unknown key: %q", key)
	}
	return nil
}

// Apply implements the StorageParamObserver interface.
func (a *IndexStorageParamObserver) Apply(
	evalCtx *tree.EvalContext, key string, expr tree.Datum,
) error {
	switch key {
	case `fillfactor`:
		return applyFillFactorStorageParam(evalCtx, key, expr)
	case `s2_max_level`:
		return a.applyS2ConfigSetting(evalCtx, key, expr, 0, 30)
	case `s2_level_mod`:
		return a.applyS2ConfigSetting(evalCtx, key, expr, 1, 3)
	case `s2_max_cells`:
		return a.applyS2ConfigSetting(evalCtx, key, expr, 1, 32)
	case `geometry_min_x`, `geometry_max_x`, `geometry_min_y`, `geometry_max_y`:
		return a.applyGeometryIndexSetting(evalCtx, key, expr)
	case `vacuum_cleanup_index_scale_factor`,
		`buffering`,
		`fastupdate`,
		`gin_pending_list_limit`,
		`pages_per_range`,
		`autosummarize`:
		return unimplemented.NewWithIssuef(43299, "storage parameter %q", key)
	}
	return errors.Errorf("invalid storage parameter %q", key)
}

// RunPostChecks implements the StorageParamObserver interface.
func (a *IndexStorageParamObserver) RunPostChecks() error {
	s2Config := getS2ConfigFromIndex(a.IndexDesc)
	if s2Config != nil {
		if (s2Config.MaxLevel)%s2Config.LevelMod != 0 {
			return errors.Newf(
				"s2_max_level (%d) must be divisible by s2_level_mod (%d)",
				s2Config.MaxLevel,
				s2Config.LevelMod,
			)
		}
	}

	if cfg := a.IndexDesc.GeoConfig.S2Geometry; cfg != nil {
		if cfg.MaxX <= cfg.MinX {
			return errors.Newf(
				"geometry_max_x (%f) must be greater than geometry_min_x (%f)",
				cfg.MaxX,
				cfg.MinX,
			)
		}
		if cfg.MaxY <= cfg.MinY {
			return errors.Newf(
				"geometry_max_y (%f) must be greater than geometry_min_y (%f)",
				cfg.MaxY,
				cfg.MinY,
			)
		}
	}
	return nil
}
