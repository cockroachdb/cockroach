// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tablestorageparam implements storageparam.Setter for
// tabledesc.Mutable.
package tablestorageparam

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/paramparse"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/normalize"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/storageparam"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// Setter observes storage parameters for tables.
type Setter struct {

	// TableDesc is the table that is modified by the Setter.
	TableDesc *tabledesc.Mutable

	// UpdatedRowLevelTTL is kept separate from the RowLevelTTL in TableDesc
	// in case changes need to be made in schema changer.
	UpdatedRowLevelTTL *catpb.RowLevelTTL

	// NewObject bool tracks if this is a newly created object.
	NewObject bool
}

var _ storageparam.Setter = (*Setter)(nil)

// NewSetter returns a new Setter.
func NewSetter(tableDesc *tabledesc.Mutable, isNewObject bool) *Setter {
	var updatedRowLevelTTL *catpb.RowLevelTTL
	if tableDesc.HasRowLevelTTL() {
		updatedRowLevelTTL = protoutil.Clone(tableDesc.GetRowLevelTTL()).(*catpb.RowLevelTTL)
	}
	return &Setter{
		TableDesc:          tableDesc,
		NewObject:          isNewObject,
		UpdatedRowLevelTTL: updatedRowLevelTTL,
	}
}

// NewTTLSetter returns a new Setter that can only set TTL parameters.
func NewTTLSetter(ttlParams *catpb.RowLevelTTL, isNewObject bool) *Setter {
	return &Setter{
		NewObject:          isNewObject,
		UpdatedRowLevelTTL: ttlParams,
	}
}

// RunPostChecks implements the Setter interface.
func (po *Setter) RunPostChecks() error {
	if err := tabledesc.ValidateRowLevelTTL(po.UpdatedRowLevelTTL); err != nil {
		return err
	}
	return nil
}

// IsNewTableObject implements the Setter interface.
func (po *Setter) IsNewTableObject() bool {
	return po.NewObject
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
			return 0, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "invalid integer value for %s", key)
		}
	}
	s, err := paramparse.DatumAsInt(ctx, evalCtx, key, intDatum)
	if err != nil {
		return 0, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "error decoding %q", key)
	}
	return s, nil
}

func floatFromDatum(
	ctx context.Context, evalCtx *eval.Context, key string, datum tree.Datum,
) (float64, error) {
	floatDatum := datum
	if stringVal, err := paramparse.DatumAsString(ctx, evalCtx, key, datum); err == nil {
		if floatDatum, err = tree.ParseDFloat(stringVal); err != nil {
			return 0, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "invalid float value for %s", key)
		}
	}
	s, err := paramparse.DatumAsFloat(ctx, evalCtx, key, floatDatum)
	if err != nil {
		return 0, pgerror.Wrapf(err, pgcode.InvalidParameterValue, "error decoding %q", key)
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
	validateSetValue func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error)
	onSet            func(ctx context.Context, po *Setter, key string, value string) error
	getResetValue    func(ctx context.Context, evalCtx *eval.Context, key string) (string, error)
	onReset          func(ctx context.Context, po *Setter, key string, value string) error
}

var ttlAutomaticColumnNotice = pgnotice.Newf("ttl_automatic_column is no longer used. " +
	"Setting ttl_expire_after automatically creates a TTL column. " +
	"Resetting ttl_expire_after removes the automatically created column.")

var ttlRangeConcurrencyNotice = pgnotice.Newf("ttl_range_concurrency is no longer configurable.")

var skipRBRUniqueRowIDChecksNotice = pgnotice.Newf("When skip_rbr_unique_rowid_checks is enabled, " +
	"do not mix user-supplied values with DEFAULT unique_rowid() or unordered_unique_rowid() values. " +
	"User-supplied values could conflict with future unique_rowid() values that omit uniqueness checks.")

var tableParams = map[string]tableParam{
	`fillfactor`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			// SetFillFactor validates the value and issues a notice but doesn't actually set anything.
			if err := storageparam.SetFillFactor(ctx, evalCtx, key, datum); err != nil {
				return "", err
			}
			return "", nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			return nil
		},
	},
	`autovacuum_enabled`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			boolVal, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if !boolVal && evalCtx != nil {
				evalCtx.ClientNoticeSender.BufferClientNotice(
					ctx,
					pgnotice.Newf(`storage parameter "%s = %s" is ignored`, key, datum.String()),
				)
			}
			return "", nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			return nil
		},
	},
	`ttl`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			setTrue, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if !setTrue {
				return "", errors.WithHintf(
					pgerror.Newf(
						pgcode.InvalidParameterValue,
						`setting "ttl = 'off'" is not permitted`,
					),
					"use `RESET (ttl)` to remove TTL from the table",
				)
			}
			return fmt.Sprintf("%t", setTrue), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			// Set the base struct, but do not populate it.
			// An error from runPostChecks will appear if the requisite fields are not set.
			po.getOrCreateRowLevelTTL()
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			po.UpdatedRowLevelTTL = nil
			return nil
		},
	},
	`ttl_automatic_column`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, ttlAutomaticColumnNotice)
			return "", nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			return nil
		},
		getResetValue: func(ctx context.Context, evalCtx *eval.Context, key string) (string, error) {
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, ttlAutomaticColumnNotice)
			return "", nil
		},
		onReset: func(ctx context.Context, po *Setter, key string, value string) error {
			return nil
		},
	},
	`ttl_expire_after`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			var d *tree.DInterval
			if stringVal, err := paramparse.DatumAsString(ctx, evalCtx, key, datum); err == nil {
				d, err = tree.ParseDInterval(evalCtx.SessionData().GetIntervalStyle(), stringVal)
				if err != nil {
					return "", pgerror.Wrapf(
						err,
						pgcode.InvalidParameterValue,
						`value of %q must be an interval`,
						key,
					)
				}
				if d == nil {
					return "", pgerror.Newf(
						pgcode.InvalidParameterValue,
						`value of %q must be an interval`,
						key,
					)
				}
			} else {
				var ok bool
				d, ok = datum.(*tree.DInterval)
				if !ok || d == nil {
					return "", pgerror.Newf(
						pgcode.InvalidParameterValue,
						`value of %q must be an interval`,
						key,
					)
				}
			}

			if d.Duration.Compare(duration.MakeDuration(0, 0, 0)) < 0 {
				return "", pgerror.Newf(
					pgcode.InvalidParameterValue,
					`value of %q must be at least zero`,
					key,
				)
			}
			return tree.Serialize(d), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DurationExpr = catpb.Expression(value)
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DurationExpr = ""
			}
			return nil
		},
	},
	`ttl_expiration_expression`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			return paramparse.DatumAsString(ctx, evalCtx, key, datum)
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.ExpirationExpr = catpb.Expression(value)
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.ExpirationExpr = ""
			}
			return nil
		},
	},
	`ttl_select_batch_size`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			val, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := tabledesc.ValidateTTLBatchSize(key, val); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", val), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			val, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.SelectBatchSize = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.SelectBatchSize = 0
			}
			return nil
		},
	},
	`ttl_delete_batch_size`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			val, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := tabledesc.ValidateTTLBatchSize(key, val); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", val), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			val, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DeleteBatchSize = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DeleteBatchSize = 0
			}
			return nil
		},
	},
	`ttl_range_concurrency`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, ttlRangeConcurrencyNotice)
			return "", nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			return nil
		},
		getResetValue: func(ctx context.Context, evalCtx *eval.Context, key string) (string, error) {
			evalCtx.ClientNoticeSender.BufferClientNotice(ctx, ttlRangeConcurrencyNotice)
			return "", nil
		},
		onReset: func(ctx context.Context, po *Setter, key string, value string) error {
			return nil
		},
	},
	`ttl_select_rate_limit`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			val, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := tabledesc.ValidateTTLRateLimit(key, val); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", val), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			val, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.SelectRateLimit = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.SelectRateLimit = 0
			}
			return nil
		},
	},
	`ttl_delete_rate_limit`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			val, err := paramparse.DatumAsInt(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := tabledesc.ValidateTTLRateLimit(key, val); err != nil {
				return "", err
			}
			return fmt.Sprintf("%d", val), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			val, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DeleteRateLimit = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DeleteRateLimit = 0
			}
			return nil
		},
	},
	`ttl_label_metrics`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			val, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%t", val), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			val, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.LabelMetrics = val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.LabelMetrics = false
			}
			return nil
		},
	},
	`ttl_job_cron`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			str, err := paramparse.DatumAsString(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := tabledesc.ValidateTTLCronExpr(key, str); err != nil {
				return "", err
			}
			return str, nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DeletionCron = value
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DeletionCron = ""
			}
			return nil
		},
	},
	`ttl_pause`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			b, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%t", b), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.Pause = b
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.Pause = false
			}
			return nil
		},
	},
	`ttl_row_stats_poll_interval`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			d, err := paramparse.DatumAsDuration(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := tabledesc.ValidateNotNegativeInterval(key, d); err != nil {
				return "", err
			}
			return d.String(), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			d, err := time.ParseDuration(value)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.RowStatsPollInterval = d
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.RowStatsPollInterval = 0
			}
			return nil
		},
	},
	`ttl_disable_changefeed_replication`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			b, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%t", b), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			b, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			rowLevelTTL := po.getOrCreateRowLevelTTL()
			rowLevelTTL.DisableChangefeedReplication = b
			return nil
		},
		onReset: func(ctx context.Context, po *Setter, key string, value string) error {
			if po.hasRowLevelTTL() {
				po.UpdatedRowLevelTTL.DisableChangefeedReplication = false
			}
			return nil
		},
	},
	`exclude_data_from_backup`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			excludeDataFromBackup, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%t", excludeDataFromBackup), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
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
				return pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"cannot set data in a table with inbound foreign key constraints to be excluded from backup")
			}

			excludeDataFromBackup, err := strconv.ParseBool(value)
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
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			po.TableDesc.ExcludeDataFromBackup = false
			return nil
		},
	},
	catpb.AutoStatsEnabledTableSettingName: {
		validateSetValue: autoStatsEnabledPreSetFunc,
		onSet:            autoStatsEnabledSetFunc,
		onReset:          autoStatsTableSettingResetFunc,
	},
	catpb.AutoStatsMinStaleTableSettingName: {
		validateSetValue: autoStatsMinStaleRowsPreSetFunc(func(intVal int64) error {
			if intVal < 0 {
				return errors.Newf("cannot be set to a negative value: %d", intVal)
			}
			return nil
		}),
		onSet:   autoStatsMinStaleRowsSetFunc,
		onReset: autoStatsTableSettingResetFunc,
	},
	catpb.AutoStatsFractionStaleTableSettingName: {
		validateSetValue: autoStatsFractionStaleRowsPreSetFunc(func(floatVal float64) error {
			if floatVal < 0 {
				return errors.Newf("cannot set to a negative value: %f", floatVal)
			}
			return nil
		}),
		onSet:   autoStatsFractionStaleRowsSetFunc,
		onReset: autoStatsTableSettingResetFunc,
	},
	catpb.AutoPartialStatsEnabledTableSettingName: {
		validateSetValue: autoStatsEnabledPreSetFunc,
		onSet:            autoStatsEnabledSetFunc,
		onReset:          autoStatsTableSettingResetFunc,
	},
	catpb.AutoFullStatsEnabledTableSettingName: {
		validateSetValue: autoStatsEnabledPreSetFunc,
		onSet:            autoStatsEnabledSetFunc,
		onReset:          autoStatsTableSettingResetFunc,
	},
	catpb.AutoPartialStatsMinStaleTableSettingName: {
		validateSetValue: autoStatsMinStaleRowsPreSetFunc(func(intVal int64) error {
			if intVal < 0 {
				return errors.Newf("cannot be set to a negative value: %d", intVal)
			}
			return nil
		}),
		onSet:   autoStatsMinStaleRowsSetFunc,
		onReset: autoStatsTableSettingResetFunc,
	},
	catpb.AutoPartialStatsFractionStaleTableSettingName: {
		validateSetValue: autoStatsFractionStaleRowsPreSetFunc(func(floatVal float64) error {
			if floatVal < 0 {
				return errors.Newf("cannot set to a negative value: %f", floatVal)
			}
			return nil
		}),
		onSet:   autoStatsFractionStaleRowsSetFunc,
		onReset: autoStatsTableSettingResetFunc,
	},
	`sql_stats_forecasts_enabled`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			enabled, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%t", enabled), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			enabled, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			po.TableDesc.ForecastStats = &enabled
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			po.TableDesc.ForecastStats = nil
			return nil
		},
	},
	`sql_stats_histogram_samples_count`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			intVal, err := intFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := nonNegativeIntWithMaximum(math.MaxUint32)(intVal); err != nil {
				return "", errors.Wrapf(err, "invalid integer value for %s", key)
			}
			return fmt.Sprintf("%d", intVal), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			intVal, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			uint32Val := uint32(intVal)
			po.TableDesc.HistogramSamples = &uint32Val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			po.TableDesc.HistogramSamples = nil
			return nil
		},
	},
	`sql_stats_histogram_buckets_count`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			intVal, err := intFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := nonNegativeIntWithMaximum(math.MaxUint32)(intVal); err != nil {
				return "", errors.Wrapf(err, "invalid integer value for %s", key)
			}
			return fmt.Sprintf("%d", intVal), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			intVal, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			uint32Val := uint32(intVal)
			po.TableDesc.HistogramBuckets = &uint32Val
			return nil
		},
		onReset: func(_ context.Context, po *Setter, key string, value string) error {
			po.TableDesc.HistogramBuckets = nil
			return nil
		},
	},
	`schema_locked`: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			boolVal, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%t", boolVal), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			boolVal, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			po.TableDesc.SchemaLocked = boolVal
			return nil
		},
		getResetValue: func(ctx context.Context, evalCtx *eval.Context, key string) (string, error) {
			schemaLockedDefault := evalCtx.SessionData().CreateTableWithSchemaLocked
			// Before 25.3 tables were never created with schema_locked by default.
			if !evalCtx.Settings.Version.IsActive(ctx, clusterversion.V25_3) {
				schemaLockedDefault = false
			}
			return fmt.Sprintf("%t", schemaLockedDefault), nil
		},
		onReset: func(ctx context.Context, po *Setter, key string, value string) error {
			boolVal, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			po.TableDesc.SchemaLocked = boolVal
			return nil
		},
	},
	catpb.RBRUsingConstraintTableSettingName: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			// Handled by the schema changer.
			return "", nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			// Handled by the schema changer.
			return nil
		},
		onReset: func(ctx context.Context, po *Setter, key string, value string) error {
			po.TableDesc.RBRUsingConstraint = descpb.ConstraintID(0)
			return nil
		},
	},
	catpb.CanaryStatsWindowSettingName: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			d, err := paramparse.DatumAsDuration(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if err := tabledesc.ValidateNotNegativeInterval(key, d); err != nil {
				return "", err
			}
			return d.String(), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			d, err := time.ParseDuration(value)
			if err != nil {
				return err
			}
			po.TableDesc.StatsCanaryWindow = d
			return nil
		},
		onReset: func(ctx context.Context, po *Setter, key string, value string) error {
			po.TableDesc.StatsCanaryWindow = 0
			return nil
		},
	},
	catpb.RBRSkipUniqueRowIDChecksTableSettingName: {
		validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
			boolVal, err := boolFromDatum(ctx, evalCtx, key, datum)
			if err != nil {
				return "", err
			}
			if boolVal && evalCtx != nil {
				evalCtx.ClientNoticeSender.BufferClientNotice(
					ctx,
					skipRBRUniqueRowIDChecksNotice,
				)
			}
			return fmt.Sprintf("%t", boolVal), nil
		},
		onSet: func(ctx context.Context, po *Setter, key string, value string) error {
			boolVal, err := strconv.ParseBool(value)
			if err != nil {
				return err
			}
			po.TableDesc.SkipRBRUniqueRowIDChecks = boolVal
			if boolVal {
				telemetry.Inc(sqltelemetry.SkipRBRUniqueRowIDChecksCounter)
			}
			return nil
		},
		onReset: func(ctx context.Context, po *Setter, key string, value string) error {
			po.TableDesc.SkipRBRUniqueRowIDChecks = false
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
			validateSetValue: func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
				return "", unimplemented.NewWithIssuef(43299, "storage parameter %q", key)
			},
			onSet: func(ctx context.Context, po *Setter, key string, value string) error {
				return nil
			},
			onReset: func(_ context.Context, po *Setter, key string, value string) error {
				return nil
			},
		}
	}
}

func autoStatsEnabledPreSetFunc(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	key string,
	datum tree.Datum,
) (string, error) {
	boolVal, err := boolFromDatum(ctx, evalCtx, key, datum)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%t", boolVal), nil
}

func autoStatsEnabledSetFunc(ctx context.Context, po *Setter, key string, value string) error {
	boolVal, err := strconv.ParseBool(value)
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
	case catpb.AutoFullStatsEnabledTableSettingName:
		po.TableDesc.AutoStatsSettings.FullEnabled = &boolVal
		return nil
	}
	return errors.AssertionFailedf("unable to set table setting %s", key)
}

func autoStatsMinStaleRowsPreSetFunc(
	validateFunc func(v int64) error,
) func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
	return func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
		intVal, err := intFromDatum(ctx, evalCtx, key, datum)
		if err != nil {
			return "", err
		}
		if err = validateFunc(intVal); err != nil {
			return "", errors.Wrapf(err, "invalid integer value for %s", key)
		}
		return fmt.Sprintf("%d", intVal), nil
	}
}

func autoStatsMinStaleRowsSetFunc(ctx context.Context, po *Setter, key string, value string) error {
	intVal, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return err
	}
	if po.TableDesc.AutoStatsSettings == nil {
		po.TableDesc.AutoStatsSettings = &catpb.AutoStatsSettings{}
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

func autoStatsFractionStaleRowsPreSetFunc(
	validateFunc func(v float64) error,
) func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
	return func(ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, key string, datum tree.Datum) (string, error) {
		floatVal, err := floatFromDatum(ctx, evalCtx, key, datum)
		if err != nil {
			return "", err
		}
		if err = validateFunc(floatVal); err != nil {
			return "", errors.Wrapf(err, "invalid float value for %s", key)
		}
		return fmt.Sprintf("%f", floatVal), nil
	}
}

func autoStatsFractionStaleRowsSetFunc(
	ctx context.Context, po *Setter, key string, value string,
) error {
	floatVal, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return err
	}
	if po.TableDesc.AutoStatsSettings == nil {
		po.TableDesc.AutoStatsSettings = &catpb.AutoStatsSettings{}
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

func autoStatsTableSettingResetFunc(_ context.Context, po *Setter, key string, value string) error {
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
	case catpb.AutoFullStatsEnabledTableSettingName:
		autoStatsSettings.FullEnabled = nil
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
	// The declarative schema changer makes a Setter that can only handle TTL
	// params, but doesn't have a TableDesc. Verify that TTL params are the only
	// ones being set in this case.
	// TODO(rafi): Figure out a way to take TableDesc out of Setter so that
	//  declarative schema changer can use this code path for non-TTL params.
	if po.TableDesc == nil {
		if !strings.HasPrefix(key, "ttl") || !po.hasRowLevelTTL() {
			return errors.AssertionFailedf("cannot set %s in this context", redact.SafeString(key))
		}
	}
	if strings.HasPrefix(key, "ttl_") && po.TableDesc != nil && len(po.TableDesc.AllMutations()) > 0 {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot modify TTL settings while another schema change on the table is being processed",
		)
	}
	if p, ok := tableParams[key]; ok {
		value, err := p.validateSetValue(ctx, semaCtx, evalCtx, key, datum)
		if err != nil {
			return err
		}
		return p.onSet(ctx, po, key, value)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}

// Reset implements the Setter interface.
func (po *Setter) Reset(ctx context.Context, evalCtx *eval.Context, key string) error {
	// The declarative schema changer makes a Setter that can only handle TTL
	// params, but doesn't have a TableDesc. Verify that TTL params are the only
	// ones being set in this case.
	// TODO(rafi): Figure out a way to take TableDesc out of Setter so that
	//  declarative schema changer can use this code path for non-TTL params.
	if po.TableDesc == nil {
		if !strings.HasPrefix(key, "ttl") {
			return errors.AssertionFailedf("cannot reset %s in this context", redact.SafeString(key))
		}
	}
	if strings.HasPrefix(key, "ttl_") && po.TableDesc != nil && len(po.TableDesc.AllMutations()) > 0 {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot modify TTL settings while another schema change on the table is being processed",
		)
	}
	if p, ok := tableParams[key]; ok {
		value := ""
		var err error
		if p.getResetValue != nil {
			value, err = p.getResetValue(ctx, evalCtx, key)
			if err != nil {
				return err
			}
		}
		return p.onReset(ctx, po, key, value)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}

// SetToStringValue sets the param value to an already validated string representation.
// This function was introduced to be used by the declarative schema changer.
func (po *Setter) SetToStringValue(ctx context.Context, key string, value string) error {
	if p, ok := tableParams[key]; ok {
		return p.onSet(ctx, po, key, value)
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}

// ResetToZeroValue resets the param value to its zero value. This function
// was introduced to be used by the declarative schema changer. The declarative
// schema changer always resets to the zero value first, and makes a separate
// call to SetToStringValue if there is a non-zero reset value.
func (po *Setter) ResetToZeroValue(ctx context.Context, key string) error {
	if p, ok := tableParams[key]; ok {
		return p.onReset(ctx, po, key, "")
	}
	return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
}

// IsValidParamKey returns an error if the key is not a valid table storage
// parameter. This function was introduced to be used by the declarative schema
// changer for RESET validation.
func IsValidParamKey(key string) error {
	if _, ok := tableParams[key]; !ok {
		return pgerror.Newf(pgcode.InvalidParameterValue, "invalid storage parameter %q", key)
	}
	return nil
}

// GetResetValue returns the value that should be used when resetting a storage
// parameter. If the param has a getResetValue callback, it will be called to
// compute the value; otherwise an empty string is returned. This function was
// introduced to be used by the declarative schema changer.
func GetResetValue(ctx context.Context, evalCtx *eval.Context, key string) (string, error) {
	if err := IsValidParamKey(key); err != nil {
		return "", err
	}
	p := tableParams[key]
	if p.getResetValue != nil {
		return p.getResetValue(ctx, evalCtx, key)
	}
	return "", nil
}

// ParseAndValidate evaluates and validates a storage parameter value without
// applying it. It returns the validated string value that would be passed to
// onSet, allowing callers to perform validation before committing changes.
// This function was introduced to be used by the declarative schema changer.
func ParseAndValidate(
	ctx context.Context, semaCtx *tree.SemaContext, evalCtx *eval.Context, param tree.StorageParam,
) (string, error) {
	key := param.Key
	if param.Value == nil {
		return "", pgerror.Newf(pgcode.InvalidParameterValue, "storage parameter %q requires a value", key)
	}
	telemetry.Inc(sqltelemetry.SetTableStorageParameter(key))

	// Expressions may be an unresolved name.
	// Cast these as strings.
	expr := paramparse.UnresolvedNameToStrVal(param.Value)

	// Storage params handle their own scalar arguments, with no help from the
	// optimizer. As such, they cannot contain subqueries.
	defer semaCtx.Properties.Restore(semaCtx.Properties)
	semaCtx.Properties.Require("table storage parameters", tree.RejectSubqueries)

	// Convert the expressions to a datum.
	typedExpr, err := tree.TypeCheck(ctx, expr, semaCtx, types.AnyElement)
	if err != nil {
		return "", err
	}
	if typedExpr, err = normalize.Expr(ctx, evalCtx, typedExpr); err != nil {
		return "", err
	}
	datum, err := eval.Expr(ctx, evalCtx, typedExpr)
	if err != nil {
		return "", err
	}

	if err := IsValidParamKey(key); err != nil {
		return "", err
	}
	p := tableParams[key]
	value, err := p.validateSetValue(ctx, semaCtx, evalCtx, key, datum)
	if err != nil {
		return "", err
	}
	return value, nil
}
