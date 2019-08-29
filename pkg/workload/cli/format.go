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
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
)

// outputFormat is the interface used to output results incrementally
// during a workload run.
type outputFormat interface {
	// rampDone is called once when the ramp-up period completes, if
	// configured.
	rampDone()
	// outputError is called when an error is encountered.
	outputError(err error)
	// outputTick is called when the main loop considers it useful
	// to emit one row of results.
	outputTick(startElapsed time.Duration, t histogram.Tick)
	// outputTotal is called at the end, using the main histogram
	// collector.
	outputTotal(startElapsed time.Duration, t histogram.Tick)
	// outputResult is called at the end, using the result histogram
	// collector.
	outputResult(startElapsed time.Duration, t histogram.Tick)
}

// textFormatter produces output meant for quick parsing by humans. The
// data is printed as fixed-width columns. Summary rows
// are printed at the end.
type textFormatter struct {
	i      int
	numErr int
}

func (f *textFormatter) rampDone() {
	f.i = 0
}

func (f *textFormatter) outputError(_ error) {
	f.numErr++
}

func (f *textFormatter) outputTick(startElapsed time.Duration, t histogram.Tick) {
	if f.i%20 == 0 {
		fmt.Println("_elapsed___errors__ops/sec(inst)___ops/sec(cum)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)")
	}
	f.i++
	fmt.Printf("%7.1fs %8d %14.1f %14.1f %8.1f %8.1f %8.1f %8.1f %s\n",
		startElapsed.Seconds(),
		f.numErr,
		float64(t.Hist.TotalCount())/t.Elapsed.Seconds(),
		float64(t.Cumulative.TotalCount())/startElapsed.Seconds(),
		time.Duration(t.Hist.ValueAtQuantile(50)).Seconds()*1000,
		time.Duration(t.Hist.ValueAtQuantile(95)).Seconds()*1000,
		time.Duration(t.Hist.ValueAtQuantile(99)).Seconds()*1000,
		time.Duration(t.Hist.ValueAtQuantile(100)).Seconds()*1000,
		t.Name,
	)
}

const totalHeader = "\n_elapsed___errors_____ops(total)___ops/sec(cum)__avg(ms)__p50(ms)__p95(ms)__p99(ms)_pMax(ms)"

func (f *textFormatter) outputTotal(startElapsed time.Duration, t histogram.Tick) {
	f.outputFinal(startElapsed, t, "__total")
}

func (f *textFormatter) outputResult(startElapsed time.Duration, t histogram.Tick) {
	f.outputFinal(startElapsed, t, "__result")
}

func (f *textFormatter) outputFinal(
	startElapsed time.Duration, t histogram.Tick, titleSuffix string,
) {
	fmt.Println(totalHeader + titleSuffix)
	if t.Cumulative == nil {
		return
	}
	if t.Cumulative.TotalCount() == 0 {
		return
	}
	fmt.Printf("%7.1fs %8d %14d %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f  %s\n",
		startElapsed.Seconds(),
		f.numErr,
		t.Cumulative.TotalCount(),
		float64(t.Cumulative.TotalCount())/startElapsed.Seconds(),
		time.Duration(t.Cumulative.Mean()).Seconds()*1000,
		time.Duration(t.Cumulative.ValueAtQuantile(50)).Seconds()*1000,
		time.Duration(t.Cumulative.ValueAtQuantile(95)).Seconds()*1000,
		time.Duration(t.Cumulative.ValueAtQuantile(99)).Seconds()*1000,
		time.Duration(t.Cumulative.ValueAtQuantile(100)).Seconds()*1000,
		t.Name,
	)
}

// jsonFormatter produces output that is machine-readable. The time is
// printed using absolute timestamps. No summary row is printed at the
// end.
type jsonFormatter struct {
	w      io.Writer
	numErr int
}

func (f *jsonFormatter) rampDone() {}

func (f *jsonFormatter) outputError(_ error) {
	f.numErr++
}

func (f *jsonFormatter) outputTick(startElapsed time.Duration, t histogram.Tick) {
	// Note: we use fmt.Printf here instead of json.Marshal to ensure
	// that float values do not get printed with a uselessly large
	// number of decimals.
	fmt.Fprintf(f.w, `{"time":"%s",`+
		`"errs":%d,`+
		`"avgt":%.1f,`+
		`"avgl":%.1f,`+
		`"p50l":%.1f,`+
		`"p95l":%.1f,`+
		`"p99l":%.1f,`+
		`"maxl":%.1f,`+
		`"type":"%s"`+
		"}\n",
		t.Now.UTC().Format(time.RFC3339Nano),
		f.numErr,
		float64(t.Hist.TotalCount())/t.Elapsed.Seconds(),
		float64(t.Cumulative.TotalCount())/startElapsed.Seconds(),
		time.Duration(t.Hist.ValueAtQuantile(50)).Seconds()*1000,
		time.Duration(t.Hist.ValueAtQuantile(95)).Seconds()*1000,
		time.Duration(t.Hist.ValueAtQuantile(99)).Seconds()*1000,
		time.Duration(t.Hist.ValueAtQuantile(100)).Seconds()*1000,
		t.Name,
	)
}

func (f *jsonFormatter) outputTotal(startElapsed time.Duration, t histogram.Tick) {}

func (f *jsonFormatter) outputResult(startElapsed time.Duration, t histogram.Tick) {}
