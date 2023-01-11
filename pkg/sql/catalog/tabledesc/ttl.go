// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tabledesc

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/robfig/cron/v3"
)

// ValidateRowLevelTTL validates that the TTL options are valid.
func ValidateRowLevelTTL(ttl *catpb.RowLevelTTL) error {
	if ttl == nil {
		return nil
	}
	if !ttl.HasDurationExpr() && !ttl.HasExpirationExpr() {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			`"ttl_expire_after" and/or "ttl_expiration_expression" must be set`,
		)
	}
	if ttl.DeleteBatchSize != 0 {
		if err := ValidateTTLBatchSize("ttl_delete_batch_size", ttl.DeleteBatchSize); err != nil {
			return err
		}
	}
	if ttl.SelectBatchSize != 0 {
		if err := ValidateTTLBatchSize("ttl_select_batch_size", ttl.SelectBatchSize); err != nil {
			return err
		}
	}
	if ttl.DeletionCron != "" {
		if err := ValidateTTLCronExpr("ttl_job_cron", ttl.DeletionCron); err != nil {
			return err
		}
	}
	if ttl.DeleteRateLimit != 0 {
		if err := ValidateTTLRateLimit("ttl_delete_rate_limit", ttl.DeleteRateLimit); err != nil {
			return err
		}
	}
	if ttl.RowStatsPollInterval != 0 {
		if err := ValidateTTLRowStatsPollInterval("ttl_row_stats_poll_interval", ttl.RowStatsPollInterval); err != nil {
			return err
		}
	}
	return nil
}

// ValidateTTLExpirationExpr validates that the ttl_expiration_expression, if
// any, only references existing columns.
func ValidateTTLExpirationExpr(desc catalog.TableDescriptor) error {
	if !desc.HasRowLevelTTL() {
		return nil
	}
	expirationExpr := desc.GetRowLevelTTL().ExpirationExpr
	if expirationExpr == "" {
		return nil
	}
	expr, err := parser.ParseExpr(string(expirationExpr))
	if err != nil {
		return errors.Wrapf(err, "ttl_expiration_expression %q must be a valid expression", expirationExpr)
	}
	// Ideally, we would also call schemaexpr.ValidateTTLExpirationExpression
	// here, but that requires a SemaCtx which we don't have here.
	valid, err := schemaexpr.HasValidColumnReferences(desc, expr)
	if err != nil {
		return err
	}
	if !valid {
		return errors.Newf("row-level TTL expiration expression %q refers to unknown columns", expirationExpr)
	}
	return nil
}

// ValidateTTLExpirationColumn validates that the ttl_expire_after setting, if
// any, is in a valid state. It requires that the TTLDefaultExpirationColumn
// exists and has DEFAULT/ON UPDATE clauses.
func ValidateTTLExpirationColumn(desc catalog.TableDescriptor) error {
	if !desc.HasRowLevelTTL() {
		return nil
	}
	if !desc.GetRowLevelTTL().HasDurationExpr() {
		return nil
	}
	intervalExpr := desc.GetRowLevelTTL().DurationExpr
	col, err := catalog.MustFindColumnByTreeName(desc, colinfo.TTLDefaultExpirationColumnName)
	if err != nil {
		return errors.Wrapf(err, "expected column %s", colinfo.TTLDefaultExpirationColumnName)
	}
	expectedStr := `current_timestamp():::TIMESTAMPTZ + ` + string(intervalExpr)
	if col.GetDefaultExpr() != expectedStr {
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"expected DEFAULT expression of %s to be %s",
			colinfo.TTLDefaultExpirationColumnName,
			expectedStr,
		)
	}
	if col.GetOnUpdateExpr() != expectedStr {
		return pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"expected ON UPDATE expression of %s to be %s",
			colinfo.TTLDefaultExpirationColumnName,
			expectedStr,
		)
	}

	// For row-level TTL, only ascending PKs are permitted.
	pk := desc.GetPrimaryIndex()
	for i := 0; i < pk.NumKeyColumns(); i++ {
		dir := pk.GetKeyColumnDirection(i)
		if dir != catenumpb.IndexColumn_ASC {
			return unimplemented.NewWithIssuef(
				76912,
				`non-ascending ordering on PRIMARY KEYs are not supported with row-level TTL`,
			)
		}
	}

	return nil
}

// ValidateTTLBatchSize validates the batch size of a TTL.
func ValidateTTLBatchSize(key string, val int64) error {
	if val <= 0 {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			`"%s" must be at least 1`,
			key,
		)
	}
	return nil
}

// ValidateTTLCronExpr validates the cron expression of TTL.
func ValidateTTLCronExpr(key string, str string) error {
	if _, err := cron.ParseStandard(str); err != nil {
		return pgerror.Wrapf(
			err,
			pgcode.InvalidParameterValue,
			`invalid cron expression for "%s"`,
			key,
		)
	}
	return nil
}

// ValidateTTLRowStatsPollInterval validates the automatic statistics field
// of TTL.
func ValidateTTLRowStatsPollInterval(key string, val time.Duration) error {
	if val <= 0 {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			`"%s" must be at least 1`,
			key,
		)
	}
	return nil
}

// ValidateTTLRateLimit validates the rate limit parameters of TTL.
func ValidateTTLRateLimit(key string, val int64) error {
	if val <= 0 {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			`"%s" must be at least 1`,
			key,
		)
	}
	return nil
}
