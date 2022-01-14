// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package jobs

import (
	"time"

	"github.com/cockroachdb/errors"
	"github.com/gorhill/cronexpr"
)

// CronParseNext is a helper that parses the next time for the given expression
// but captures any panic that may occur in the underlying library.
func CronParseNext(
	e *cronexpr.Expression, fromTime time.Time, cron string,
) (t time.Time, err error) {
	defer func() {
		if recover() != nil {
			t = time.Time{}
			err = errors.Newf("failed to parse cron expression: %q", cron)
		}
	}()

	return e.Next(fromTime), nil
}
