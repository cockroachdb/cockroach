// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package closedts

import (
	"errors"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
)

// TargetDuration is the follower reads closed timestamp update target duration.
var TargetDuration = settings.RegisterNonNegativeDurationSetting(
	"kv.closed_timestamp.target_duration",
	"if nonzero, attempt to provide closed timestamp notifications for timestamps trailing cluster time by approximately this duration",
	30*time.Second,
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
