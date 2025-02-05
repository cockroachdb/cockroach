// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeeds

import "time"

// defaultEnvValuesInt if the environment variable is not set
var defaultEnvValuesInt = map[string]int{
	maxChangefeeds:          20,
	maxPctChangeFeedsScanOn: 20, // max 2 CF job out of 10 will be created randomly with initial scan value "yes" or "only"
}

// pollForStatusInterval is the interval to wait before polling for status again
const pollForStatusInterval = 10 * time.Second
const pollForStatusTimeout = 10 * time.Minute
