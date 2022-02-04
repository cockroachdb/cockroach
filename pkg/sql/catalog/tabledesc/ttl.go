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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/robfig/cron/v3"
)

// ValidateRowLevelTTL validates that the TTL options are valid.
func ValidateRowLevelTTL(ttl *descpb.TableDescriptor_RowLevelTTL) error {
	if ttl == nil {
		return nil
	}
	if ttl.DurationExpr == "" {
		return pgerror.Newf(
			pgcode.InvalidParameterValue,
			`"ttl_expire_after" must be set`,
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
