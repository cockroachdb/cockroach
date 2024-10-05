// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rttanalysis

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/log"
)

type testingB interface {
	testing.TB
	N() int
	ResetTimer()
	StopTimer()
	StartTimer()
	ReportMetric(float64, string)
	Run(string, func(testingB))

	// logScope is used to wrap log.Scope and make it available only in
	// appropriate contexts.
	logScope() (getDirectory func() string, close func())
	isBenchmark() bool
}

type bShim struct {
	*testing.B
}

func (b bShim) isBenchmark() bool { return true }

var _ testingB = bShim{}

func (b bShim) logScope() (getDirectory func() string, close func()) {
	sc := log.Scope(b)
	return sc.GetDirectory, func() { sc.Close(b) }
}
func (b bShim) N() int { return b.B.N }
func (b bShim) Run(name string, f func(b testingB)) {
	b.B.Run(name, func(b *testing.B) {
		f(bShim{b})
	})
}

// tShim is used by the expectation test's testing.T to appear as though
// it is a testing.B and can be used capture the output. The object also
// suppresses creation of a new log.Scope in order to permit parallel
// execution.
type tShim struct {
	*testing.T
	scope   *log.TestLogScope
	results *resultSet
}

func (ts tShim) isBenchmark() bool { return false }

var _ testingB = tShim{}

func (ts tShim) logScope() (getDirectory func() string, close func()) {
	return ts.scope.GetDirectory, func() {}
}
func (ts tShim) GetDirectory() string {
	return ts.scope.GetDirectory()
}
func (ts tShim) N() int      { return 2 }
func (ts tShim) ResetTimer() {}
func (ts tShim) StopTimer()  {}
func (ts tShim) StartTimer() {}
func (ts tShim) ReportMetric(f float64, s string) {
	if s == roundTripsMetric {
		ts.results.add(benchmarkResult{
			name:   ts.Name(),
			result: int(f),
		})
	}
}
func (ts tShim) Name() string {
	// Trim the name of the outermost test off the beginning of the name.
	tn := ts.T.Name()
	return tn[strings.Index(tn, "/")+1:]
}
func (ts tShim) Run(s string, f func(testingB)) {
	ts.T.Run(s, func(t *testing.T) {
		f(tShim{results: ts.results, T: t, scope: ts.scope})
	})
}
