// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cronexpr

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	unsafe_cronexpr "github.com/gorhill/cronexpr"
)

// cronexpr third party package is thread hostile. So, protect access to it via mutex.
// TODO(yevgeniy): Add a linter to disallow direct uses of gorhill/cronexpr
// TODO(yevgeniy): Consider replacing cronexpr since this package is not maintained.
var cronMu syncutil.Mutex

// Expression is a wrapper around unsafe_cronexpr.Expression
type Expression interface {
	Next(fromTime time.Time) time.Time
}

type wrappedExpression struct {
	*unsafe_cronexpr.Expression
}

// Parse parses crontab line string and returns cron expression.
func Parse(cronLine string) (Expression, error) {
	cronMu.Lock()
	defer cronMu.Unlock()

	e, err := unsafe_cronexpr.Parse(cronLine)
	if err != nil {
		return nil, err
	}
	return &wrappedExpression{e}, nil
}
