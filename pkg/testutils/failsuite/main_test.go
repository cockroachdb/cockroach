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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package failsuite

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

func TestMain(m *testing.M) {
	if !envutil.EnvOrDefaultBool("COCKROACH_FAILSUITE", false) {
		return
	}
	os.Exit(m.Run())
}

func TestThatPasses(t *testing.T) {
	fmt.Println("output of", t.Name())
}

func TestThatFails(t *testing.T) {
	t.Error("an error occurs")
	t.Fatal("then the test fatals")
}

func TestThatGetsSkipped(t *testing.T) {
	t.Log("logging something")
	t.Skip("skipped")
}

func TestParallelOneWithDataRaceWithoutFailures(t *testing.T) {
	testParallelImpl(t, false)
}
func TestParallelWithDataRaceAndFailures(t *testing.T) {
	testParallelImpl(t, true)
}

func testParallelImpl(t *testing.T, withFailures bool) {
	var dataracingGlobal bool
	sleep := func() {
		time.Sleep(time.Duration(rand.Intn(1E9)))
	}
	// Parallel tests are interesting because they interleave.
	// Make the parent test parallel as well, to interleave it
	// with another copy of itself.
	fmt.Println("output before calling t.Parallel()")
	t.Parallel()
	for i := 0; i < 10; i++ {
		i := i
		fails := withFailures && (i%3) == 0
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			t.Parallel()
			// Add a data race. Go pretty much catches this every time.
			dataracingGlobal = !dataracingGlobal
			sleep()
			// NB: access to stdout isn't actually handled properly with parallel tests.
			// In effect, this output will just be put into whatever test happens to
			// announce that it's running most recently. So we can't do anything useful
			// here and avoid doing this in the first place. fmt.Printf("subtest %d
			// outputs this\n", i)
			if fails {
				t.Error("subtest failed")
			} else {
				t.Log("something gets logged")
			}
		})
	}

}
