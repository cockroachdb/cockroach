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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geoindex"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

// SetStorageParameters sets the given storage parameters using the
// given observer.
func SetStorageParameters(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	params tree.StorageParams,
	paramObserver StorageParamObserver,
) error {
	for _, sp := range params {
		key := string(sp.Key)
		if sp.Value == nil {
			return pgerror.Newf(pgcode.InvalidParameterValue, "storage parameter %q requires a value", key)
		}
		telemetry.Inc(sqltelemetry.SetTableStorageParameter(key))

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

		if err := paramObserver.onSet(ctx, semaCtx, evalCtx, key, datum); err != nil {
			return err
		}
	}
	return paramObserver.runPostChecks()
}

// ResetStorageParameters sets the given storage parameters using the
// given observer.
func ResetStorageParameters(
	ctx context.Context,
	evalCtx *tree.EvalContext,
	params tree.NameList,
	paramObserver StorageParamObserver,
) error {
	for _, p := range params {
		telemetry.Inc(sqltelemetry.ResetTableStorageParameter(string(p)))
		if err := paramObserver.onReset(evalCtx, string(p)); err != nil {
			return err
		}
	}
	return paramObserver.runPostChecks()
}

// StorageParamObserver applies a storage parameter to an underlying item.
type StorageParamObserver interface {
	// onSet is called during CREATE [TABLE | INDEX] ... WITH (...) or
	// ALTER [TABLE | INDEX] ... WITH (...).
	onSet(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error
	// onReset is called during ALTER [TABLE | INDEX] ... RESET (...)
	onReset(evalCtx *tree.EvalContext, key string) error
	// runPostChecks is called after all storage parameters have been set.
	// This allows checking whether multiple storage parameters together
	// form a valid configuration.
	runPostChecks() error
}

// TableStorageParamObserver observes storage parameters for tables.
type TableStorageParamObserver struct {
	tableDesc          *tabledesc.Mutable
	setAutomaticColumn bool
}

var _ StorageParamObserver = (*TableStorageParamObserver)(nil)

// NewTableStorageParamObserver returns a new TableStorageParamObserver.
func NewTableStorageParamObserver(tableDesc *tabledesc.Mutable) *TableStorageParamObserver {
	return &TableStorageParamObserver{tableDesc: tableDesc}
}

// runPostChecks implements the StorageParamObserver interface.
func (po *TableStorageParamObserver) runPostChecks() error {
	ttl := po.tableDesc.GetRowLevelTTL()
	if po.setAutomaticColumn && (ttl == nil || ttl.DurationExpr == "") {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			`"ttl_expire_after" must be set if "ttl_automatic_column" is set`,
		)
	}
	if err := tabledesc.ValidateRowLevelTTL(ttl); err != nil {
		return err
	}
	return nil
}

func boolFromDatum(evalCtx *tree.EvalContext, key string, datum tree.Datum) (bool, error) {
	if stringVal, err := DatumAsString(evalCtx, key, datum); err == nil {
		return ParseBoolVar(key, stringVal)
	}
	s, err := GetSingleBool(key, datum)
	if err != nil {
		return false, err
	}
	return bool(*s), nil
}

type tableParam struct {
	onSet   func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error
	onReset func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error
}

var tableParams = map[string]tableParam{
	`fillfactor`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			return setFillFactorStorageParam(evalCtx, key, datum)
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			// Operation is a no-op so do nothing.
			return nil
		},
	},
	`autovacuum_enabled`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
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
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			// Operation is a no-op so do nothing.
			return nil
		},
	},
	`ttl`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			setTrue, err := boolFromDatum(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if setTrue && po.tableDesc.RowLevelTTL == nil {
				// Set the base struct, but do not populate it.
				// An error from runPostChecks will appear if the requisite fields are not set.
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			if !setTrue && po.tableDesc.RowLevelTTL != nil {
				return unimplemented.NewWithIssue(75428, "unsetting TTL not yet implemented")
			}
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			po.tableDesc.RowLevelTTL = nil
			return nil
		},
	},
	`ttl_automatic_column`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			setTrue, err := boolFromDatum(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if setTrue {
				po.setAutomaticColumn = true
			}
			if !setTrue && po.tableDesc.RowLevelTTL != nil {
				return unimplemented.NewWithIssue(76916, "unsetting TTL automatic column not yet implemented")
			}
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			return unimplemented.NewWithIssue(76916, "unsetting TTL automatic column not yet implemented")
		},
	},
	`ttl_expire_after`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			var d *tree.DInterval
			if stringVal, err := DatumAsString(evalCtx, key, datum); err == nil {
				d, err = tree.ParseDInterval(evalCtx.SessionData().GetIntervalStyle(), stringVal)
				if err != nil || d == nil {
					return pgerror.Newf(
						pgcode.InvalidParameterValue,
						`value of "ttl_expire_after" must be an interval`,
					)
				}
			} else {
				var ok bool
				d, ok = datum.(*tree.DInterval)
				if !ok || d == nil {
					return pgerror.Newf(
						pgcode.InvalidParameterValue,
						`value of "%s" must be an interval`,
						key,
					)
				}
			}

			if d.Duration.Compare(duration.MakeDuration(0, 0, 0)) < 0 {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					`value of "%s" must be at least zero`,
					key,
				)
			}
			if po.tableDesc.RowLevelTTL == nil {
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			po.tableDesc.RowLevelTTL.DurationExpr = catpb.Expression(tree.Serialize(d))
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			return errors.WithHintf(
				pgerror.Newf(
					pgcode.InvalidParameterValue,
					`resetting "ttl_expire_after" is not permitted`,
				),
				"use `RESET (ttl)` to remove TTL from the table",
			)
		},
	},
	`ttl_select_batch_size`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			if po.tableDesc.RowLevelTTL == nil {
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			val, err := DatumAsInt(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLBatchSize(key, val); err != nil {
				return err
			}
			po.tableDesc.RowLevelTTL.SelectBatchSize = val
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			if po.tableDesc.RowLevelTTL != nil {
				po.tableDesc.RowLevelTTL.SelectBatchSize = 0
			}
			return nil
		},
	},
	`ttl_delete_batch_size`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			if po.tableDesc.RowLevelTTL == nil {
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			val, err := DatumAsInt(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLBatchSize(key, val); err != nil {
				return err
			}
			po.tableDesc.RowLevelTTL.DeleteBatchSize = val
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			if po.tableDesc.RowLevelTTL != nil {
				po.tableDesc.RowLevelTTL.DeleteBatchSize = 0
			}
			return nil
		},
	},
	`ttl_range_concurrency`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			if po.tableDesc.RowLevelTTL == nil {
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			val, err := DatumAsInt(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLRangeConcurrency(key, val); err != nil {
				return err
			}
			po.tableDesc.RowLevelTTL.RangeConcurrency = val
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			if po.tableDesc.RowLevelTTL != nil {
				po.tableDesc.RowLevelTTL.RangeConcurrency = 0
			}
			return nil
		},
	},
	`ttl_delete_rate_limit`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			if po.tableDesc.RowLevelTTL == nil {
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			val, err := DatumAsInt(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLRateLimit(key, val); err != nil {
				return err
			}
			po.tableDesc.RowLevelTTL.DeleteRateLimit = val
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			if po.tableDesc.RowLevelTTL != nil {
				po.tableDesc.RowLevelTTL.DeleteRateLimit = 0
			}
			return nil
		},
	},
	`ttl_job_cron`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			if po.tableDesc.RowLevelTTL == nil {
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			str, err := DatumAsString(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLCronExpr(key, str); err != nil {
				return err
			}
			po.tableDesc.RowLevelTTL.DeletionCron = str
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			if po.tableDesc.RowLevelTTL != nil {
				po.tableDesc.RowLevelTTL.DeletionCron = ""
			}
			return nil
		},
	},
	`ttl_pause`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			b, err := boolFromDatum(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if po.tableDesc.RowLevelTTL == nil {
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			po.tableDesc.RowLevelTTL.Pause = b
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			po.tableDesc.RowLevelTTL.Pause = false
			return nil
		},
	},
	`ttl_row_stats_poll_interval`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			d, err := DatumAsDuration(evalCtx, key, datum)
			if err != nil {
				return err
			}
			if po.tableDesc.RowLevelTTL == nil {
				po.tableDesc.RowLevelTTL = &catpb.RowLevelTTL{}
			}
			if err := tabledesc.ValidateTTLRowStatsPollInterval(key, d); err != nil {
				return err
			}
			po.tableDesc.RowLevelTTL.RowStatsPollInterval = d
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			po.tableDesc.RowLevelTTL.RowStatsPollInterval = 0
			return nil
		},
	},
	`exclude_data_from_backup`: {
		onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext,
			evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
			if po.tableDesc.Temporary {
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"cannot set data in a temporary table to be excluded from backup")
			}

			// Check that the table does not have any incoming FK references. During a
			// backup, the rows of a table with ephemeral data will not be backed up, and
			// could result in a violation of FK constraints on restore. To prevent this,
			// we only allow a table with no incoming FK references to be marked as
			// ephemeral.
			if len(po.tableDesc.InboundFKs) != 0 {
				return errors.New("cannot set data in a table with inbound foreign key constraints to be excluded from backup")
			}

			excludeDataFromBackup, err := boolFromDatum(evalCtx, key, datum)
			if err != nil {
				return err
			}
			// If the table descriptor being changed has the same value for the
			// `ExcludeDataFromBackup` flag, no-op.
			if po.tableDesc.ExcludeDataFromBackup == excludeDataFromBackup {
				return nil
			}
			po.tableDesc.ExcludeDataFromBackup = excludeDataFromBackup
			return nil
		},
		onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
			po.tableDesc.ExcludeDataFromBackup = false
			return nil
		},
	},
}

func init() {
	for _, param := range []string{
		`toast_tuple_target`,
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
		`user_catalog_table`,
	} {
		tableParams[param] = tableParam{
			onSet: func(ctx context.Context, po *TableStorageParamObserver, semaCtx *tree.SemaContext, evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
				return unimplemented.NewWithIssuef(43299, "storage parameter %q", key)
			},
			onReset: func(po *TableStorageParamObserver, evalCtx *tree.EvalContext, key string) error {
				return nil
			},
		}
	}
}

// onSet implements the StorageParamObserver interface.
func (po *TableStorageParamObserver) onSet(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	key string,
	datum tree.Datum,
) error {
	if strings.HasPrefix(key, "ttl_") && len(po.tableDesc.AllMutations()) > 0 {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot modify TTL settings while another schema change on the table is being processed",
		)
	}
	if p, ok := tableParams[key]; ok {
		return p.onSet(ctx, po, semaCtx, evalCtx, key, datum)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}

// onReset implements the StorageParamObserver interface.
func (po *TableStorageParamObserver) onReset(evalCtx *tree.EvalContext, key string) error {
	if strings.HasPrefix(key, "ttl_") && len(po.tableDesc.AllMutations()) > 0 {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot modify TTL settings while another schema change on the table is being processed",
		)
	}
	if p, ok := tableParams[key]; ok {
		return p.onReset(po, evalCtx, key)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}

func setFillFactorStorageParam(evalCtx *tree.EvalContext, key string, datum tree.Datum) error {
	val, err := DatumAsFloat(evalCtx, key, datum)
	if err != nil {
		return err
	}
	if val < 0 || val > 100 {
		return pgerror.Newf(pgcode.InvalidParameterValue, "%q must be between 0 and 100", key)
	}
	if evalCtx != nil {
		evalCtx.ClientNoticeSender.BufferClientNotice(
			evalCtx.Context,
			pgnotice.Newf("storage parameter %q is ignored", key),
		)
	}
	return nil
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

func (po *IndexStorageParamObserver) applyS2ConfigSetting(
	evalCtx *tree.EvalContext, key string, expr tree.Datum, min int64, max int64,
) error {
	s2Config := getS2ConfigFromIndex(po.IndexDesc)
	if s2Config == nil {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			"index setting %q can only be set on GEOMETRY or GEOGRAPHY spatial indexes",
			key,
		)
	}

	val, err := DatumAsInt(evalCtx, key, expr)
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

func (po *IndexStorageParamObserver) applyGeometryIndexSetting(
	evalCtx *tree.EvalContext, key string, expr tree.Datum,
) error {
	if po.IndexDesc.GeoConfig.S2Geometry == nil {
		return pgerror.Newf(pgcode.InvalidParameterValue, "%q can only be applied to GEOMETRY spatial indexes", key)
	}
	val, err := DatumAsFloat(evalCtx, key, expr)
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

// onSet implements the StorageParamObserver interface.
func (po *IndexStorageParamObserver) onSet(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	key string,
	expr tree.Datum,
) error {
	switch key {
	case `fillfactor`:
		return setFillFactorStorageParam(evalCtx, key, expr)
	case `s2_max_level`:
		return po.applyS2ConfigSetting(evalCtx, key, expr, 0, 30)
	case `s2_level_mod`:
		return po.applyS2ConfigSetting(evalCtx, key, expr, 1, 3)
	case `s2_max_cells`:
		return po.applyS2ConfigSetting(evalCtx, key, expr, 1, 32)
	case `geometry_min_x`, `geometry_max_x`, `geometry_min_y`, `geometry_max_y`:
		return po.applyGeometryIndexSetting(evalCtx, key, expr)
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

// onReset implements the StorageParameterObserver interface.
func (po *IndexStorageParamObserver) onReset(evalCtx *tree.EvalContext, key string) error {
	return errors.AssertionFailedf("non-implemented codepath")
}

// runPostChecks implements the StorageParamObserver interface.
func (po *IndexStorageParamObserver) runPostChecks() error {
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
