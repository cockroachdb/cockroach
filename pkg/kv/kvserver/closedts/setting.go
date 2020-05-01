// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package closedts

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

// TargetDuration is the follower reads closed timestamp update target duration.
var TargetDuration = settings.RegisterNonNegativeDurationSetting(
	"kv.closed_timestamp.target_duration",
	"if nonzero, attempt to provide closed timestamp notifications for timestamps trailing cluster time by approximately this duration",
	3*time.Second,
)

// CloseFraction is the fraction of TargetDuration determining how often closed
// timestamp updates are to be attempted.
var CloseFraction = settings.RegisterValidatedFloatSetting(
	"kv.closed_timestamp.close_fraction",
	"fraction of closed timestamp target duration specifying how frequently the closed timestamp is advanced",
	0.2,
	func(v float64) error {
		if v <= 0 || v > 1 {
			return errors.New("value not between zero and one")
		}
		return nil
	})
