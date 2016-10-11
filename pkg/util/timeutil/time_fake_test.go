// Copyright 2016 The Cockroach Authors.
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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package timeutil

import (
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util/envutil"
	_ "github.com/cockroachdb/cockroach/util/log" // for flags
)

func TestOffset(t *testing.T) {
	for _, expectedOffset := range []time.Duration{-time.Hour, time.Hour} {
		if err := os.Setenv("COCKROACH_SIMULATED_OFFSET", expectedOffset.String()); err != nil {
			t.Fatal(err)
		}

		envutil.ClearEnvCache()

		initFakeTime()

		lowerBound := time.Now().Add(expectedOffset)
		offsetTime := Now()
		upperBound := time.Now().Add(expectedOffset)

		if offsetTime.Before(lowerBound) || offsetTime.After(upperBound) {
			t.Errorf("expected offset time %s to be in the interval\n[%s,%s]", offsetTime, lowerBound, upperBound)
		}
	}
}
