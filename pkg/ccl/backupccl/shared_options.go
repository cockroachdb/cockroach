// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

const (
	backupRestoreOptDebugPauseOn = "debug_pause_on"
)

var allowedDebugPauseOnValues = map[string]struct{}{
	"error": {},
}

func validateDebugPauseOn(value string) error {
	if _, ok := allowedDebugPauseOnValues[value]; len(value) > 0 && !ok {
		return errors.Newf("%s cannot be set with the value %s", backupRestoreOptDebugPauseOn, value)
	}
	return nil
}

func errorForDebugPauseOnValue(ctx context.Context, onPauseValue string, err error) error {
	switch onPauseValue {
	case "error":
		const errorFmt = "job failed with error (%v) but is being paused due to the %s=%s option"
		log.Warningf(ctx, errorFmt, err, backupRestoreOptDebugPauseOn, onPauseValue)

		return jobs.MarkPauseRequestError(errors.Wrapf(err,
			"pausing job due to the %s=%s setting",
			backupRestoreOptDebugPauseOn, onPauseValue))
	default:
		return err
	}
}
