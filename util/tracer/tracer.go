// Copyright 2015 The Cockroach Authors.
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
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package tracer

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/caller"
)

// A Traceable object has a Trace identifier attached to it.
type Traceable interface {
	// TraceID is the unique ID for the tracee.
	TraceID() string
	// TraceName is a short and "sufficiently unique" human-readable
	// representation of the tracee.
	TraceName() string
}

// A TraceItem is an entry in a Trace.
type TraceItem struct {
	depth     int32
	Origin    string
	Name      string
	Timestamp time.Time
	Duration  time.Duration
	File      string
	Line      int
	Func      string
	HLC       time.Time // TODO(tschottdorf) HLC timestamp
}

// A Trace is created by a Tracer and records the path of a request within (a
// connected part of) the system. It contains the ID of the traced object and a
// slice of trace entries. In typical usage the Epoch() and Event() methods are
// called at various stages to record the path the associated request takes
// through the system; when the request goes out of scope, a call to Finalize
// marks the end of the Trace, at which point it publishes itself to an
// associated `util.Feed`. A request may create multiple Traces as it passes
// through different parts of a distributed systems.
// A Trace is not safe for concurrent access.
//
// TODO(tschottdorf): not allowing concurrent access is the right thing to do
// semantically, but we pass a Trace along with a context.Context, which
// explicitly encourages sharing of values. Might want to add that just for
// that reason, but for now it's convenient to let the race detector check what
// we do with the Trace.
type Trace struct {
	// IDs is the unique identifier for the request in this trace.
	ID string
	// Name is a human-readable identifier for the request in this trace.
	Name string
	// Content is a trace, containing call sites and timings in the order in
	// which they happened.
	Content []TraceItem
	tracer  *Tracer // origin tracer for clock, publishing...
	depth   int32
}

// Event adds an Epoch with zero duration to the Trace.
func (t *Trace) Event(name string) {
	if t == nil {
		return
	}
	t.epoch(name)()
	t.Content[len(t.Content)-1].Duration = 0
}

// Epoch begins a phase in the life of the Trace, starting the measurement of
// its duration. The returned function needs to be called when the measurement
// is complete; failure to do so results in a panic() when Finalize() is
// called. The suggested pattern of usage is, where possible,
// `defer trace.Epoch("<name>")()`.
func (t *Trace) Epoch(name string) func() {
	if t == nil {
		return func() {}
	}
	return t.epoch(name)
}

func (t *Trace) epoch(name string) func() {
	if t.depth < 0 {
		panic("use of finalized Trace:\n" + t.String())
	}
	t.depth++
	pos := t.add(name)
	called := false
	return func() {
		if called {
			panic("epoch terminated twice")
		}
		called = true
		t.Content[pos].Duration = t.tracer.now().Sub(t.Content[pos].Timestamp)
		t.depth--
	}
}

// Finalize submits the Trace to the underlying feed. If there is an open
// Epoch, a panic occurs.
func (t *Trace) Finalize() {
	if t == nil {
		return
	}
	if t.depth != 0 {
		panic("attempt to finalize unbalanced trace:\n" + t.String())
	}
	t.depth = math.MinInt32
	t.tracer.feed.Publish(t) // by reference
}

func (t *Trace) add(name string) int {
	// Must be called with two callers to the client.
	// (Client->Event|Epoch->epoch->add)
	file, line, fun := caller.Lookup(3)
	t.Content = append(t.Content, TraceItem{
		depth:     t.depth,
		Origin:    t.tracer.origin,
		File:      file,
		Line:      line,
		Func:      fun,
		Timestamp: t.tracer.now(),
		Name:      name,
	})
	return len(t.Content) - 1
}

// Fork creates a new Trace, equal to (but autonomous from) that which created
// the original Trace.
func (t *Trace) Fork() *Trace {
	if t == nil {
		return nil
	}
	return t.tracer.newTrace(t.ID, t.Name)
}

// String implements fmt.Stringer. It prints a human-readable breakdown of the
// Trace.
func (t Trace) String() string {
	const tab = "\t"
	buf := bytes.NewBuffer(nil)
	w := tabwriter.NewWriter(buf, 1, 1, 0, ' ', 0)
	fmt.Fprintln(w, "Name", tab, "Origin", tab, "Ts", tab, "Dur", tab, "Desc", tab, "File")

	const traceTimeFormat = "15:04:05.000000"
	for _, c := range t.Content {
		var namePrefix string
		if c.depth > 1 {
			namePrefix = strings.Repeat("·", int(c.depth-1))
		}
		fmt.Fprintln(w, t.Name, tab, c.Origin, tab,
			c.Timestamp.Format(traceTimeFormat), tab, c.Duration, tab,
			namePrefix+c.Name, tab, c.File+":"+strconv.Itoa(c.Line))
	}

	_ = w.Flush()
	return buf.String()
}

// A Tracer is used to follow requests across the system (or across systems).
// Requests must implement the Traceable interface and can be traced by invoking
// NewTrace(), which returns a Trace object initialized to publish itself to a
// util.Feed registered by the Tracer on completion.
type Tracer struct {
	origin string // owner of this Tracer, i.e. Host ID
	feed   *util.Feed
	now    func() time.Time
}

// NewTracer returns a new Tracer whose created Traces publish to the given feed.
// The origin is an identifier of the system, for instance a host ID.
func NewTracer(f *util.Feed, origin string) *Tracer {
	return &Tracer{
		origin: origin,
		now:    time.Now,
		feed:   f,
	}
}

var dummyTracer = &Tracer{
	now: func() time.Time { return time.Time{} },
}

// NewTrace creates a Trace for the given Traceable.
func (t *Tracer) NewTrace(tracee Traceable) *Trace {
	if t == nil {
		t = dummyTracer
	}
	return t.newTrace(tracee.TraceID(), tracee.TraceName())
}

func (t *Tracer) newTrace(id, name string) *Trace {
	return &Trace{
		ID:     id,
		Name:   name,
		tracer: t,
	}

}
