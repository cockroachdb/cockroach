// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tablestorageparam implements storageparam.Setter for
// tabledesc.Mutable.
package tablestorageparam

import (
	"context"
	"math"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// Setter observes storage parameters for tables.
type Setter struct {

	// TableDesc is the table that is modified by the Setter.
	TableDesc *tabledesc.Mutable

	// UpdatedRowLevelTTL is kept separate from the RowLevelTTL in TableDesc
	// in case changes need to be made in schema changer.
	UpdatedRowLevelTTL *catpb.RowLevelTTL
}

var _ storageparam.Setter = (*Setter)(nil)

// NewSetter returns a new Setter.
func NewSetter(tableDesc *tabledesc.Mutable) *Setter {
	var updatedRowLevelTTL *catpb.RowLevelTTL
	if tableDesc.HasRowLevelTTL() {
		updatedRowLevelTTL = protoutil.Clone(tableDesc.GetRowLevelTTL()).(*catpb.RowLevelTTL)
	}
	return &Setter{
		TableDesc:          tableDesc,
		UpdatedRowLevelTTL: updatedRowLevelTTL,
	}
}

// RunPostChecks implements the Setter interface.
func (po *Setter) RunPostChecks() error {
	if err := tabledesc.ValidateRowLevelTTL(po.UpdatedRowLevelTTL); err != nil {
		return err
	}
	return nil
}

func boolFromDatum(
	ctx context.Context, evalCtx *eval.Context, key string, datum tree.Datum,
) (bool, error) {
	if stringVal, err := paramparse.DatumAsString(ctx, evalCtx, key, datum); err == nil {
		return paramparse.ParseBoolVar(key, stringVal)
	}
	s, err := paramparse.GetSingleBool(key, datum)
	if err != nil {
		return false, err
	}
	return bool(*s), nil
}

func intFromDatum(
	ctx context.Context, evalCtx *eval.Context, key string, datum tree.Datum,
) (int64, error) {
	intDatum := datum
	if stringVal, err := paramparse.DatumAsString(ctx, evalCtx, key, datum); err == nil {
		if intDatum, err = tree.ParseDInt(stringVal); err != nil {
			return 0, errors.Wrapf(err, "invalid integer value for %s", key)
		}
	}
	s, err := paramparse.DatumAsInt(ctx, evalCtx, key, intDatum)
	if err != nil {
		return 0, err
	}
	return s, nil
}

func floatFromDatum(
	ctx context.Context, evalCtx *eval.Context, key string, datum tree.Datum,
) (float64, error) {
	floatDatum := datum
	if stringVal, err := paramparse.DatumAsString(ctx, evalCtx, key, datum); err == nil {
		if floatDatum, err = tree.ParseDFloat(stringVal); err != nil {
			return 0, errors.Wrapf(err, "invalid float value for %s", key)
		}
	}
	s, err := paramparse.DatumAsFloat(ctx, evalCtx, key, floatDatum)
	if err != nil {
		return 0, err
	}
	return s, nil
}

func (po *Setter) hasRowLevelTTL() bool {
	return po.UpdatedRowLevelTTL != nil
}

func (po *Setter) getOrCreateRowLevelTTL() *catpb.RowLevelTTL {
	rowLevelTTL := po.UpdatedRowLevelTTL
	if rowLevelTTL == nil {
		rowLevelTTL = &catpb.RowLevelTTL{}
		po.UpdatedRowLevelTTL = rowLevelTTL
	}
	return rowLevelTTL
}

type tableParam struct {
	onSet   func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error
	onReset func(ctx context.Context, po *Setter, evalCtx *eval.Context, key string) error
}

var ttlAutomaticColumnNotice = pgnotice.Newf("ttl_automatic_column is no longer used. " +
	"Setting ttl_expire_after automatically creates a TTL column. " +
	"Resetting ttl_expire_after removes the automatically created column.")

var ttlRangeConcurrencyNotice = pgnotice.Newf("ttl_range_concurrency is no longer configurable.")

var tableParams = map[string]tableParam{
	`fillfactor`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			return storageparam.SetFillFactor(ctx, evalCtx, key, datum)
		},
		onReset: func(_ context.Context, po *Setter, _ *eval.Context, key string) error {
			// Operation is a no-op so do nothing.
			return nil
		},
	},
	`autovacuum_enabled`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			var boolVal bool
			if stringVal, err := paramparse.DatumAsString(ctx, evalCtx, key, datum); err == nil {
				boolVal, err = paramparse.ParseBoolVar(key, stringVal)
				if err != nil {
					return err
				}
			} else {
				s, err := paramparse.GetSingleBool(key, datum)
				if err != nil {
					return err
				}
				boolVal = bool(*s)
			}
			if !boolVal && evalCtx != nil {
				evalCtx.ClientNoticeSender.BufferClientNotice(
					ctx,
					pgnotice.Newf(`storage parameter "%s = %s" is ignored`, key, datum.String()),
				)
			}
			return nil
		},
		onReset: func(_ context.Context, po *Setter, _ *eval.Context, key string) error {
			// Operation is a no-op so do nothing.
			return nil
		},
	},
	`ttl`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			setTrue, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if setTrue {
				// Set the base struct, but do not populate it.
				// An error from runPostChecks will appear if the requisite fields are not set.
				po.getOrCreateRowLevelTTL()
			} else {
				return errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidParameterValue,
						`setting "ttl = 'off'" is not permitted`,
					),
					"use `RESET (ttl)` to remove TTL from the table",
				)
			}
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			po.UpdatedRowLevelTTL = nil
			return nil
		},
	},
	`ttl_automatic_column`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, ttlAutomaticColumnNotice)
			return nil
		},
		onReset: func(ctx context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, ttlAutomaticColumnNotice)
			return nil
		},
	},
	`ttl_expire_after`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			var d *tree.DInterval
			if stringVal, err := paramparse.DatumAsString(ctx, evalCtx, key, datum); err == nil {
				d, err = tree.ParseDInterval(evalCtx.SessionData().GetIntervalStyle(), stringVal)
				if err != nil {
					return pgerror.Wrapf(
						err,
						pgcode.InvalidParameterValue,
						`value of %q must be an interval`,
						key,
					)
				}
				if d == nil {
					return pgerror.Newf(
						pgcode.InvalidParameterValue,
						`value of %q must be an interval`,
						key,
					)
				}
			} else {
				var ok bool
				d, ok = datum.(*tree.DInterval)
				if !ok || d == nil {
					return pgerror.Newf(
						pgcode.InvalidParameterValue,
						`value of %q must be an interval`,
						key,
					)
				}
			}

			if d.Duration.Compare(duration.MakeDuration(0, 0, 0)) < 0 {
				return pgerror.Newf(
					pgcode.InvalidParameterValue,
					`value of %q must be at least zero`,
					key,
				)
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DurationExpr = catpb.Expression(tree.Serialize(d))
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DurationExpr = ""
			}
			return nil
		},
	},
	`ttl_expiration_expression`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			stringVal, err := paramparse.DatumAsString(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.ExpirationExpr = catpb.Expression(stringVal)
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.ExpirationExpr = ""
			}
			return nil
		},
	},
	`ttl_select_batch_size`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			val, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLBatchSize(key, val); err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.SelectBatchSize = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.SelectBatchSize = 0
			}
			return nil
		},
	},
	`ttl_delete_batch_size`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			val, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLBatchSize(key, val); err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DeleteBatchSize = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DeleteBatchSize = 0
			}
			return nil
		},
	},
	`ttl_range_concurrency`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, ttlRangeConcurrencyNotice)
			return nil
		},
		onReset: func(ctx context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, ttlRangeConcurrencyNotice)
			return nil
		},
	},
	`ttl_select_rate_limit`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			val, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLRateLimit(key, val); err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.SelectRateLimit = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.SelectRateLimit = 0
			}
			return nil
		},
	},
	`ttl_delete_rate_limit`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			val, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLRateLimit(key, val); err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DeleteRateLimit = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DeleteRateLimit = 0
			}
			return nil
		},
	},
	`ttl_label_metrics`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			val, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.LabelMetrics = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.LabelMetrics = false
			}
			return nil
		},
	},
	`ttl_job_cron`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			str, err := paramparse.DatumAsString(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLCronExpr(key, str); err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DeletionCron = str
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DeletionCron = ""
			}
			return nil
		},
	},
	`ttl_pause`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			b, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.Pause = b
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.Pause = false
			}
			return nil
		},
	},
	`ttl_row_stats_poll_interval`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			d, err := paramparse.DatumAsDuration(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := tabledesc.ValidateTTLRowStatsPollInterval(key, d); err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.RowStatsPollInterval = d
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.RowStatsPollInterval = 0
			}
			return nil
		},
	},
	`ttl_disable_changefeed_replication`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			b, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DisableChangefeedReplication = b
			return nil
		},
		onReset: func(ctx context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DisableChangefeedReplication = false
			}
			return nil
		},
	},
	`exclude_data_from_backup`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext,
			evalCtx *eval.Context, key string, datum tree.Datum) error {
			if po.TableDesc.Temporary {
				return pgerror.Newf(pgcode.FeatureNotSupported,
					"cannot set data in a temporary table to be excluded from backup")
			}

			// Check that the table does not have any incoming FK references. During a
			// backup, the rows of a table with ephemeral data will not be backed up, and
			// could result in a violation of FK constraints on restore. To prevent this,
			// we only allow a table with no incoming FK references to be marked as
			// ephemeral.
			if len(po.TableDesc.InboundFKs) != 0 {
				return errors.New("cannot set data in a table with inbound foreign key constraints to be excluded from backup")
			}

			excludeDataFromBackup, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			// If the table descriptor being changed has the same value for the
			// `ExcludeDataFromBackup` flag, no-op.
			if po.TableDesc.ExcludeDataFromBackup == excludeDataFromBackup {
				return nil
			}
			po.TableDesc.ExcludeDataFromBackup = excludeDataFromBackup
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			po.TableDesc.ExcludeDataFromBackup = false
			return nil
		},
	},
	catpb.AutoStatsEnabledTableSettingName: {
		onSet:   autoStatsEnabledSettingFunc,
		onReset: autoStatsTableSettingResetFunc,
	},
	catpb.AutoStatsMinStaleTableSettingName: {
		onSet: autoStatsMinStaleRowsSettingFunc(func(intVal int64) error {
			if intVal < 0 {
				return errors.Newf("cannot be set to a negative value: %d", intVal)
			}
			return nil
		}),
		onReset: autoStatsTableSettingResetFunc,
	},
	catpb.AutoStatsFractionStaleTableSettingName: {
		onSet: autoStatsFractionStaleRowsSettingFunc(func(floatVal float64) error {
			if floatVal < 0 {
				return errors.Newf("cannot set to a negative value: %f", floatVal)
			}
			return nil
		}),
		onReset: autoStatsTableSettingResetFunc,
	},
	catpb.AutoPartialStatsEnabledTableSettingName: {
		onSet:   autoStatsEnabledSettingFunc,
		onReset: autoStatsTableSettingResetFunc,
	},
	catpb.AutoPartialStatsMinStaleTableSettingName: {
		onSet: autoStatsMinStaleRowsSettingFunc(func(intVal int64) error {
			if intVal < 0 {
				return errors.Newf("cannot be set to a negative value: %d", intVal)
			}
			return nil
		}),
		onReset: autoStatsTableSettingResetFunc,
	},
	catpb.AutoPartialStatsFractionStaleTableSettingName: {
		onSet: autoStatsFractionStaleRowsSettingFunc(func(floatVal float64) error {
			if floatVal < 0 {
				return errors.Newf("cannot set to a negative value: %f", floatVal)
			}
			return nil
		}),
		onReset: autoStatsTableSettingResetFunc,
	},
	`sql_stats_forecasts_enabled`: {
		onSet: func(
			ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum,
		) error {
			enabled, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			po.TableDesc.ForecastStats = &enabled
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			po.TableDesc.ForecastStats = nil
			return nil
		},
	},
	`sql_stats_histogram_samples_count`: {
		onSet: func(
			ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum,
		) error {
			intVal, err := intFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err := nonNegativeIntWithMaximum(math.MaxUint32)(intVal); err != nil {
				return errors.Wrapf(err, "invalid integer value for %s", key)
			}
			uint32Val := uint32(intVal)
			po.TableDesc.HistogramSamples = &uint32Val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			po.TableDesc.HistogramSamples = nil
			return nil
		},
	},
	`sql_stats_histogram_buckets_count`: {
		onSet: func(
			ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum,
		) error {
			intVal, err := intFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			if err = nonNegativeIntWithMaximum(math.MaxUint32)(intVal); err != nil {
				return errors.Wrapf(err, "invalid integer value for %s", key)
			}
			uint32Val := uint32(intVal)
			po.TableDesc.HistogramBuckets = &uint32Val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			po.TableDesc.HistogramBuckets = nil
			return nil
		},
	},
	`schema_locked`: {
		onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
			boolVal, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return err
			}
			po.TableDesc.SchemaLocked = boolVal
			return nil
		},
		onReset: func(ctx context.Context, po *Setter, evalCtx *eval.Context, key string) error {
			po.TableDesc.SchemaLocked = false
			return nil
		},
	},
}

func nonNegativeIntWithMaximum(max int64) func(int64) error {
	return func(intVal int64) error {
		if intVal < 0 {
			return errors.Newf("cannot be set to a negative integer: %d", intVal)
		}
		if intVal > max {
			return errors.Newf("cannot be set to an integer larger than %d", max)
		}
		return nil
	}
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
			onSet: func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
				return unimplemented.NewWithIssuef(43299, "storage parameter %q", key)
			},
			onReset: func(_ context.Context, po *Setter, _ *eval.Context, key string) error {
				return nil
			},
		}
	}
}

func autoStatsEnabledSettingFunc(
	ctx context.Context,
	po *Setter,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	key string,
	datum tree.Datum,
) error {
	boolVal, err := boolFromDatum(ctx, evalCtx, key, datum)
	if err != nil {
		return err
	}
	if po.TableDesc.AutoStatsSettings == nil {
		po.TableDesc.AutoStatsSettings = &catpb.AutoStatsSettings{}
	}

	switch key {
	case catpb.AutoStatsEnabledTableSettingName:
		po.TableDesc.AutoStatsSettings.Enabled = &boolVal
		return nil
	case catpb.AutoPartialStatsEnabledTableSettingName:
		po.TableDesc.AutoStatsSettings.PartialEnabled = &boolVal
		return nil
	}
	return errors.AssertionFailedf("unable to set table setting %s", key)
}

func autoStatsMinStaleRowsSettingFunc(
	validateFunc func(v int64) error,
) func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
	return func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
		intVal, err := intFromDatum(ctx, evalCtx, key, datum)
		if err != nil {
			return err
		}
		if po.TableDesc.AutoStatsSettings == nil {
			po.TableDesc.AutoStatsSettings = &catpb.AutoStatsSettings{}
		}
		if err = validateFunc(intVal); err != nil {
			return errors.Wrapf(err, "invalid integer value for %s", key)
		}

		switch key {
		case catpb.AutoStatsMinStaleTableSettingName:
			po.TableDesc.AutoStatsSettings.MinStaleRows = &intVal
			return nil
		case catpb.AutoPartialStatsMinStaleTableSettingName:
			po.TableDesc.AutoStatsSettings.PartialMinStaleRows = &intVal
			return nil
		}
		return errors.AssertionFailedf("unable to set table setting %s", key)
	}
}

func autoStatsFractionStaleRowsSettingFunc(
	validateFunc func(v float64) error,
) func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) error {
	return func(ctx context.Context, po *Setter, semaCtx *tree.SemaContext,
		evalCtx *eval.Context, key string, datum tree.Datum) error {
		floatVal, err := floatFromDatum(ctx, evalCtx, key, datum)
		if err != nil {
			return err
		}
		if po.TableDesc.AutoStatsSettings == nil {
			po.TableDesc.AutoStatsSettings = &catpb.AutoStatsSettings{}
		}
		if err = validateFunc(floatVal); err != nil {
			return errors.Wrapf(err, "invalid float value for %s", key)
		}

		switch key {
		case catpb.AutoStatsFractionStaleTableSettingName:
			po.TableDesc.AutoStatsSettings.FractionStaleRows = &floatVal
			return nil
		case catpb.AutoPartialStatsFractionStaleTableSettingName:
			po.TableDesc.AutoStatsSettings.PartialFractionStaleRows = &floatVal
			return nil
		}
		return errors.AssertionFailedf("unable to set table setting %s", key)
	}
}

func autoStatsTableSettingResetFunc(
	_ context.Context, po *Setter, evalCtx *eval.Context, key string,
) error {
	if po.TableDesc.AutoStatsSettings == nil {
		return nil
	}
	autoStatsSettings := po.TableDesc.AutoStatsSettings
	switch key {
	case catpb.AutoStatsEnabledTableSettingName:
		autoStatsSettings.Enabled = nil
		return nil
	case catpb.AutoStatsMinStaleTableSettingName:
		autoStatsSettings.MinStaleRows = nil
		return nil
	case catpb.AutoStatsFractionStaleTableSettingName:
		autoStatsSettings.FractionStaleRows = nil
		return nil
	case catpb.AutoPartialStatsEnabledTableSettingName:
		autoStatsSettings.PartialEnabled = nil
		return nil
	case catpb.AutoPartialStatsMinStaleTableSettingName:
		autoStatsSettings.PartialMinStaleRows = nil
		return nil
	case catpb.AutoPartialStatsFractionStaleTableSettingName:
		autoStatsSettings.PartialFractionStaleRows = nil
		return nil
	}
	return errors.AssertionFailedf("unable to reset table setting %s", key)
}

// Set implements the Setter interface.
func (po *Setter) Set(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	key string,
	datum tree.Datum,
) error {
	if strings.HasPrefix(key, "ttl_") && len(po.TableDesc.AllMutations()) > 0 {
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

// Reset implements the Setter interface.
func (po *Setter) Reset(ctx context.Context, evalCtx *eval.Context, key string) error {
	if strings.HasPrefix(key, "ttl_") && len(po.TableDesc.AllMutations()) > 0 {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot modify TTL settings while another schema change on the table is being processed",
		)
	}
	if p, ok := tableParams[key]; ok {
		return p.onReset(ctx, po, evalCtx, key)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}
