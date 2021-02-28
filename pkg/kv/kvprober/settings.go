// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvprober

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
)

var readEnabled = settings.RegisterBoolSetting(
	"kv.prober.read.enabled",
	"whether the KV read prober is enabled",
	false)

// TODO(josh): Another option is for the cluster setting to be a QPS target
// for the cluster as a whole.
var readInterval = settings.RegisterDurationSetting(
	"kv.prober.read.interval",
	"how often each node sends a read probe to the KV layer on average (jitter is added); "+
		"note that a very slow read can block kvprober from sending additional probes; "+
		"kv.prober.read.timeout controls the max time kvprober can be blocked",
	1*time.Minute, func(duration time.Duration) error {
		if duration <= 0 {
			return errors.New("param must be >0")
		}
		return nil
	})

var readTimeout = settings.RegisterDurationSetting(
	"kv.prober.read.timeout",
	// Slow enough response times are not different than errors from the
	// perspective of the user.
	"if this much time elapses without success, a KV read probe will be treated as an error; "+
		"note that a very slow read can block kvprober from sending additional probes"+
		"this setting controls the max time kvprober can be blocked",
	2*time.Second, func(duration time.Duration) error {
		if duration <= 0 {
			return errors.New("param must be >0")
		}
		return nil
	})

var scanMeta2Timeout = settings.RegisterDurationSetting(
	"kv.prober.planner.scan_meta2.timeout",
	"timeout on scanning meta2 via db.Scan with max rows set to "+
		"kv.prober.planner.num_steps_to_plan_at_once",
	2*time.Second, func(duration time.Duration) error {
		if duration <= 0 {
			return errors.New("param must be >0")
		}
		return nil
	})

var numStepsToPlanAtOnce = settings.RegisterIntSetting(
	"kv.prober.planner.num_steps_to_plan_at_once",
	"the number of Steps to plan at once, where a Step is a decision on "+
		"what range to probe; the order of the Steps is randomized within "+
		"each planning run, so setting this to a small number will lead to "+
		"close-to-lexical probing; already made plans are held in memory, so "+
		"large values are advised against",
	100, func(i int64) error {
		if i <= 0 {
			return errors.New("param must be >0")
		}
		return nil
	})
