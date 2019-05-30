// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
)

func Example_text_formatter() {
	testFormatter(&textFormatter{})

	// output:
	// _elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)
	//     1.0s        0            1.0            1.0    503.3    503.3    503.3    503.3 read
	//     2.0s        0            0.5            1.0    335.5    335.5    335.5    335.5 read
	//
	// _elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__total
	//     3.0s        0              2            0.7    411.0    335.5    503.3    503.3    503.3  read
	//
	// _elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__result
	//     4.0s        0              2            0.5    411.0    335.5    503.3    503.3    503.3  woo
}

func Example_json_formatter() {
	testFormatter(&jsonFormatter{})

	// output:
	// {Time:"0001-01-01T00:00:01Z",Errors:0,OpsPerSec:1.0,AvgLatMillis:1.0,P50LatMillis:503.3,P95LatMillis:503.3,P99LatMillis:503.3,PMaxLatMillis:503.3,OpName:"read"}
	// {Time:"0001-01-01T00:00:02Z",Errors:0,OpsPerSec:0.5,AvgLatMillis:1.0,P50LatMillis:335.5,P95LatMillis:335.5,P99LatMillis:335.5,PMaxLatMillis:335.5,OpName:"read"}
}

func testFormatter(formatter outputFormat) {
	reg := histogram.NewRegistry(time.Second)

	start := time.Time{}

	reg.GetHandle().Get("read").Record(time.Second / 2)
	reg.Tick(func(t histogram.Tick) {
		// Make output deterministic.
		t.Elapsed = time.Second
		t.Now = start.Add(t.Elapsed)

		formatter.outputTick(time.Second, t)
	})

	reg.GetHandle().Get("read").Record(time.Second / 3)
	reg.Tick(func(t histogram.Tick) {
		// ditto.
		t.Elapsed = 2 * time.Second
		t.Now = start.Add(t.Elapsed)

		formatter.outputTick(2*time.Second, t)
	})

	resultTick := histogram.Tick{Name: "woo"}
	reg.Tick(func(t histogram.Tick) {
		// ditto.
		t.Elapsed = 3 * time.Second
		t.Now = start.Add(t.Elapsed)

		formatter.outputTotal(3*time.Second, t)
		resultTick.Now = t.Now
		resultTick.Cumulative = t.Cumulative
	})
	formatter.outputResult(4*time.Second, resultTick)
}
