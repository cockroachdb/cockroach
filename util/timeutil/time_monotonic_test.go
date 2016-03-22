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
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package timeutil

import (
	"os"
	"testing"
	"time"
)

func TestMonotonicityCheck(t *testing.T) {
	if err := os.Setenv(monotonicCheckEnableEnvKey, "1"); err != nil {
		t.Fatal(err)
	}

	initMonotonicityCheck()

	firstTime := Now()

	if err := os.Setenv(offsetEnvKey, "-10m"); err != nil {
		t.Fatal(err)
	}
	initFakeTime()
	secondTime := Now()
	if mu.monotonicityErrorsCount != 1 {
		t.Fatalf("clock backward jump was not detected by the monotonicity checker (from %s to %s)", firstTime, secondTime)
	}

	SetMonotonicityCheckThreshold(time.Hour)
	if err := os.Setenv(offsetEnvKey, "-20m"); err != nil {
		t.Fatal(err)
	}
	initFakeTime()
	thirdTime := Now()

	if mu.monotonicityErrorsCount != 1 {
		t.Fatalf("clock backward jump below threshold was incorrectly detected by the monotonicity checker (from %s to %s)", secondTime, thirdTime)
	}

}
