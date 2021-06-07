// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rttanalysis

import (
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
)

// TestBenchmarkExpectation runs all of the benchmarks and
// one iteration and validates that the number of RPCs meets
// the expectation.
//
// It takes a long time and thus is skipped under stress, race
// and short.
func TestBenchmarkExpectation(t *testing.T) {
	skip.UnderStress(t)
	skip.UnderRace(t)
	skip.UnderShort(t)

	expecations := readExpectationsFile(t)

	benchmarks := getBenchmarks(t)
	if *rewriteFlag != "" {
		rewriteBenchmarkExpecations(t, benchmarks)
		return
	}

	defer func() {
		if t.Failed() {
			t.Log("see the --rewrite flag to re-run the benchmarks and adjust the expectations")
		}
	}()

	var g sync.WaitGroup
	run := func(b string) {
		tf := func(t *testing.T) {
			flags := []string{
				"--test.run=^$",
				"--test.bench=" + b,
				"--test.benchtime=1x",
			}
			if testing.Verbose() {
				flags = append(flags, "--test.v")
			}
			results := runBenchmarks(t, flags...)

			for _, r := range results {
				exp, ok := expecations.find(r.name)
				if !ok {
					t.Logf("no expectation for benchmark %s, got %d", r.name, r.result)
					continue
				}
				if !exp.matches(r.result) {
					t.Errorf("fail: expected %s to perform KV lookups in [%d, %d], got %d",
						r.name, exp.min, exp.max, r.result)
				} else {
					t.Logf("success: expected %s to perform KV lookups in [%d, %d], got %d",
						r.name, exp.min, exp.max, r.result)
				}
			}
		}
		g.Add(1)
		go func() {
			defer g.Done()
			t.Run(b, tf)
		}()
	}
	for _, b := range benchmarks {
		run(b)
	}
	g.Wait()
}
