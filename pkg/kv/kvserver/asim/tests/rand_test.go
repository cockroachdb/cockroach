// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"testing"
	"time"
)

const (
	defaultNumIterations = 5
	defaultSeed          = 42
	defaultDuration      = 30 * time.Minute
	defaultVerbosity     = false
)

func defaultSettings(randOptions map[string]bool) testSettings {
	return newTestSettings(defaultNumIterations, defaultDuration, defaultVerbosity, defaultSeed, randOptions)
}

// TestRandomized is a randomized testing framework which validates an allocator
// simulator by creating randomized configurations, exposing potential edge
// cases and bugs.
//
// Inputs:
//
// numIterations - Number of test iterations.
// duration - Duration of each test iteration.
// randSeed - Random number generation seed.
// verbose - Boolean to enable detailed failure output.
// randOptions - A map with (key: option, value: boolean indicating if the key
// option should be randomized).
// TODO(wenyihu6): change input structure to datadriven + print more useful info
// for output + add more tests to cover cases that are not tested by default
func TestRandomized(t *testing.T) {
	randOptions := map[string]bool{
		"cluster":         false,
		"ranges":          false,
		"load":            false,
		"static_settings": false,
		"static_events":   false,
		"assertions":      false,
	}
	settings := defaultSettings(randOptions)
	f := newRandTestingFramework(settings)
	f.runRandTestRepeated(t)
}
