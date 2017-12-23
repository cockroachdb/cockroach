// Copyright 2017 The Cockroach Authors.
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

package fsm

import (
	"bytes"
	"fmt"
	"io"
	"sort"
	"strings"
)

type debugInfo struct {
	t                Transitions
	sortedStateNames []string
	sortedEventNames []string
	stateNameMap     map[string]State
	eventNameMap     map[string]Event
}

// eventAppliedToState returns the State resulting from applying the specified
// Event to the specified State and the bool true, or false if there is no
// associated transition.
func (di debugInfo) eventAppliedToState(sName, eName string) (State, bool) {
	sm := di.t.expanded[di.stateNameMap[sName]]
	tr, ok := sm[di.eventNameMap[eName]]
	return tr.Next, ok
}

func nameStr(i interface{}, strip string) string {
	s := fmt.Sprintf("%#v", i)
	parts := strings.Split(s, ".")
	last := parts[len(parts)-1]
	return strings.Replace(last, strip, "", -1)
}
func stateName(s State) string { return nameStr(s, "state") }
func eventName(e Event) string { return nameStr(e, "event") }

func makeDebugInfo(t Transitions) debugInfo {
	di := debugInfo{
		t:            t,
		stateNameMap: make(map[string]State),
		eventNameMap: make(map[string]Event),
	}
	maybeAddState := func(s State) {
		sName := stateName(s)
		if _, ok := di.stateNameMap[sName]; !ok {
			di.sortedStateNames = append(di.sortedStateNames, sName)
			di.stateNameMap[sName] = s
		}
	}
	maybeAddEvent := func(e Event) {
		eName := eventName(e)
		if _, ok := di.eventNameMap[eName]; !ok {
			di.sortedEventNames = append(di.sortedEventNames, eName)
			di.eventNameMap[eName] = e
		}
	}

	for s, sm := range di.t.expanded {
		maybeAddState(s)
		for e, tr := range sm {
			maybeAddEvent(e)
			maybeAddState(tr.Next)
		}
	}

	sort.Strings(di.sortedStateNames)
	sort.Strings(di.sortedEventNames)
	return di
}

// panicWriter wraps an io.Writer, panicing if a call to Write ever fails.
type panicWriter struct {
	w io.Writer
}

// Write implements the io.Writer interface.
func (pw *panicWriter) Write(p []byte) (n int, err error) {
	if n, err = pw.w.Write(p); err != nil {
		panic(err)
	}
	return n, nil
}

func genReport(w io.Writer, t Transitions) {
	w = &panicWriter{w: w}
	di := makeDebugInfo(t)
	var present, missing bytes.Buffer
	for _, sName := range di.sortedStateNames {
		defer present.Reset()
		defer missing.Reset()

		for _, eName := range di.sortedEventNames {
			handledBuf := &missing
			if _, ok := di.eventAppliedToState(sName, eName); ok {
				handledBuf = &present
			}
			fmt.Fprintf(handledBuf, "\t\t%s\n", eName)
		}

		fmt.Fprintf(w, "%s\n", sName)
		fmt.Fprintf(w, "\thandled events:\n")
		_, _ = io.Copy(w, &present)
		fmt.Fprintf(w, "\tmissing events:\n")
		_, _ = io.Copy(w, &missing)
	}
}

func genDot(w io.Writer, t Transitions, start State) {
	dw := dotWriter{w: &panicWriter{w: w}, di: makeDebugInfo(t)}
	dw.Write(start)
}

// dotWriter writes a graph representation of the debugInfo in the DOT language.
type dotWriter struct {
	w  io.Writer
	di debugInfo
}

func (dw dotWriter) Write(start State) {
	dw.writeHeader(start)
	dw.writeEdges()
	dw.writeFooter()
}

func (dw dotWriter) writeHeader(start State) {
	fmt.Fprintf(dw.w, "digraph finite_state_machine {\n")
	fmt.Fprintf(dw.w, "\trankdir=LR;\n\n")
	if start != nil {
		startName := stateName(start)
		fmt.Fprintf(dw.w, "\tnode [shape = doublecircle]; %q;\n", startName)
		fmt.Fprintf(dw.w, "\tnode [shape = point ]; qi\n")
		fmt.Fprintf(dw.w, "\tqi -> %q;\n\n", startName)
	}
	fmt.Fprintf(dw.w, "\tnode [shape = circle];\n")
}

func (dw dotWriter) writeEdges() {
	di := dw.di
	for _, sName := range di.sortedStateNames {
		for _, eName := range di.sortedEventNames {
			if next, ok := di.eventAppliedToState(sName, eName); ok {
				fmt.Fprintf(dw.w, "\t%q -> %q [label = %q]\n",
					sName, stateName(next), eName)
			}
		}
	}
}

func (dw dotWriter) writeFooter() {
	fmt.Fprintf(dw.w, "}\n")
}

// WriteReport writes a report of the Transitions graph, reporting on which
// Events each State handles and which Events each state does not.
func (t Transitions) WriteReport(w io.Writer) {
	genReport(w, t)
}

// WriteDotGraph writes a representation of the Transitions graph in the
// graphviz dot format. It accepts a starting State that will be expressed as
// such in the graph, if provided.
func (t Transitions) WriteDotGraph(w io.Writer, start State) {
	genDot(w, t, start)
}
