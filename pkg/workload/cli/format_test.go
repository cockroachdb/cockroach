// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/stretchr/testify/require"
)

func Example_text_formatter() {
	testFormatter(&textFormatter{})

	// output:
	// _elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)
	//     0.5s        0            2.0            2.0    503.3    503.3    503.3    503.3 read
	//     1.5s        0            0.7            1.3    335.5    335.5    335.5    335.5 read
	//
	// _elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__total
	//     2.0s        0              2            1.0    411.0    335.5    503.3    503.3    503.3  read
	//
	// _elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)__result
	//     4.0s        0              2            0.5    411.0    335.5    503.3    503.3    503.3  woo
}

func Example_json_formatter() {
	testFormatter(&jsonFormatter{w: os.Stdout})

	// output:
	// {"time":"0001-01-01T00:00:00.5Z","errs":0,"avgt":2.0,"avgl":2.0,"p50l":503.3,"p95l":503.3,"p99l":503.3,"maxl":503.3,"type":"read"}
	// {"time":"0001-01-01T00:00:01.5Z","errs":0,"avgt":0.7,"avgl":1.3,"p50l":335.5,"p95l":335.5,"p99l":335.5,"maxl":335.5,"type":"read"}
}

func testFormatter(formatter outputFormat) {
	reg := histogram.NewRegistry(
		time.Second,
		histogram.MockWorkloadName,
	)

	start := time.Time{}

	reg.GetHandle().Get("read").Record(time.Second / 2)
	reg.Tick(func(t histogram.Tick) {
		// Make output deterministic.
		t.Elapsed = time.Second / 2
		t.Now = start.Add(t.Elapsed)

		formatter.outputTick(t.Elapsed, t)
	})

	reg.GetHandle().Get("read").Record(time.Second / 3)
	reg.Tick(func(t histogram.Tick) {
		// ditto.
		t.Elapsed = 3 * time.Second / 2
		t.Now = start.Add(t.Elapsed)

		formatter.outputTick(t.Elapsed, t)
	})

	resultTick := histogram.Tick{Name: "woo"}
	reg.Tick(func(t histogram.Tick) {
		// ditto.
		t.Elapsed = 2 * time.Second
		t.Now = start.Add(t.Elapsed)

		formatter.outputTotal(t.Elapsed, t)
		resultTick.Now = t.Now
		resultTick.Cumulative = t.Cumulative
	})
	formatter.outputResult(4*time.Second, resultTick)
}

// TestJSONStructure ensures that the JSON output is parsable.
func TestJSONStructure(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var buf bytes.Buffer
	f := jsonFormatter{w: &buf}
	reg := histogram.NewRegistry(
		time.Second,
		histogram.MockWorkloadName,
	)

	start := time.Time{}

	reg.GetHandle().Get("read").Record(time.Second / 2)
	reg.Tick(func(t histogram.Tick) {
		// Make output deterministic.
		t.Elapsed = time.Second
		t.Now = start.Add(t.Elapsed)

		f.outputTick(time.Second, t)
	})

	const expected = `
{
"time":"0001-01-01T00:00:01Z",
"avgl":1,
"avgt":1,
"errs":0,
"maxl":503.3,
"p50l":503.3,
"p95l":503.3,
"p99l":503.3,
"type":"read"
}`
	require.JSONEq(t, expected, buf.String())
}

// TestParseJson ensures that all the fields in the JSON output can be parsed.
func TestParseJSON(t *testing.T) {
	// A line taken directly from a test run output.
	jsonStr := `{"time":"2024-08-30T19:40:31.913393Z","errs":1,"avgt":4531.0,"avgl":1120.2,"p50l":2.4,"p95l":19.9,"p99l":37.7,"maxl":62.9,"type":"read"}`
	tick, err := fromJson(jsonStr)
	require.NoError(t, err)

	expectedTick := Tick{
		Time:       time.Date(2024, 8, 30, 19, 40, 31, 913393000, time.UTC),
		Errs:       1,
		Throughput: 4531.0,
		P50:        2400 * time.Microsecond,
		P95:        19900 * time.Microsecond,
		P99:        37700 * time.Microsecond,
		PMax:       62900 * time.Microsecond,
		Type:       "read",
	}
	require.Equal(t, expectedTick, tick)
}

// TestParseOutput ensures that the valid json can be extracted from a file.
// The output is an snippet from a test run output with associated non-JSON
// garbage in the file.
func TestParseOutput(t *testing.T) {
	output := `
	W240830 19:40:30.523513 3604 workload/pgx_helpers.go:240  [-] 4953  error preparing statement. name=kv-2 sql=SELECT k, v FROM kv AS OF SYSTEM TIME follower_read_timestamp() WHERE k IN ($1) ERROR: database "kv" does not exist (SQLSTATE 3D000)
{"time":"2024-08-30T19:40:30.915598Z","errs":0,"avgt":242.1,"avgl":269.9,"p50l":13.6,"p95l":3758.1,"p99l":3892.3,"maxl":3892.3,"type":"follower-read"}
{"time":"2024-08-30T19:40:30.915598Z","errs":0,"avgt":223.9,"avgl":249.7,"p50l":31.5,"p95l":3758.1,"p99l":3892.3,"maxl":3892.3,"type":"read"}
Number of reads that didn't return any results: 40661.
Write sequence could be resumed by passing --write-seq=R133867 to the next run.`
	ticks := ParseOutput(strings.NewReader(output))
	fmt.Println(ticks)
	require.Equal(t, 2, len(ticks))
	require.Equal(t, ticks[0].Type, "follower-read")
	require.Equal(t, ticks[1].Type, "read")
}
