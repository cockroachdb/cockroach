// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	reachableStates  map[string]struct{}
}

// eventAppliedToState returns the Transition resulting from applying the
// specified Event to the specified State and the bool true, or false if there
// is no associated transition.
func (di debugInfo) eventAppliedToState(sName, eName string) (Transition, bool) {
	sm := di.t.expanded[di.stateNameMap[sName]]
	tr, ok := sm[di.eventNameMap[eName]]
	return tr, ok
}

func (di debugInfo) reachable(sName string) bool {
	_, ok := di.reachableStates[sName]
	return ok
}

func typeName(i interface{}) string {
	s := fmt.Sprintf("%#v", i)
	parts := strings.Split(s, ".")
	return parts[len(parts)-1]
}
func trimState(s string) string { return strings.TrimPrefix(s, "state") }
func trimEvent(s string) string { return strings.TrimPrefix(s, "event") }
func stateName(s State) string  { return trimState(typeName(s)) }
func eventName(e Event) string  { return trimEvent(typeName(e)) }

func makeDebugInfo(t Transitions) debugInfo {
	di := debugInfo{
		t:               t,
		stateNameMap:    make(map[string]State),
		eventNameMap:    make(map[string]Event),
		reachableStates: make(map[string]struct{}),
	}
	maybeAddState := func(s State, markReachable bool) {
		sName := stateName(s)
		if _, ok := di.stateNameMap[sName]; !ok {
			di.sortedStateNames = append(di.sortedStateNames, sName)
			di.stateNameMap[sName] = s
		}
		if markReachable {
			di.reachableStates[sName] = struct{}{}
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
		maybeAddState(s, false)
		for e, tr := range sm {
			maybeAddEvent(e)

			// markReachable if this isn't a self-loop.
			markReachable := s != tr.Next
			maybeAddState(tr.Next, markReachable)
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
		if !di.reachable(sName) {
			fmt.Fprintf(w, "\tunreachable!\n")
		}
		fmt.Fprintf(w, "\thandled events:\n")
		_, _ = io.Copy(w, &present)
		fmt.Fprintf(w, "\tmissing events:\n")
		_, _ = io.Copy(w, &missing)
	}
}

func genDot(w io.Writer, t Transitions, start string) {
	dw := dotWriter{w: &panicWriter{w: w}, di: makeDebugInfo(t)}
	dw.Write(start)
}

// dotWriter writes a graph representation of the debugInfo in the DOT language.
type dotWriter struct {
	w  io.Writer
	di debugInfo
}

func (dw dotWriter) Write(start string) {
	dw.writeHeader(start)
	dw.writeEdges(start)
	dw.writeFooter()
}

func (dw dotWriter) writeHeader(start string) {
	fmt.Fprintf(dw.w, "digraph finite_state_machine {\n")
	fmt.Fprintf(dw.w, "\trankdir=LR;\n\n")
	if start != "" {
		if _, ok := dw.di.stateNameMap[start]; !ok {
			panic(fmt.Sprintf("unknown state %q", start))
		}
		fmt.Fprintf(dw.w, "\tnode [shape = doublecircle]; %q;\n", start)
		fmt.Fprintf(dw.w, "\tnode [shape = point ]; qi\n")
		fmt.Fprintf(dw.w, "\tqi -> %q;\n\n", start)
	}
	fmt.Fprintf(dw.w, "\tnode [shape = circle];\n")
}

func (dw dotWriter) writeEdges(start string) {
	di := dw.di
	for _, sName := range di.sortedStateNames {
		if start != "" && start != sName {
			if !di.reachable(sName) {
				// If the state isn't reachable and it's not the starting state,
				// don't include it in the graph.
				continue
			}
		}
		for _, eName := range di.sortedEventNames {
			if tr, ok := di.eventAppliedToState(sName, eName); ok {
				var label string
				if tr.Description == "" {
					label = fmt.Sprintf("%q", eName)
				} else {
					// We'll use an HTML label with the description on the 2nd line.
					label = fmt.Sprintf("<%s<BR/><I>%s</I>>", eName, tr.Description)
				}
				fmt.Fprintf(dw.w, "\t%q -> %q [label = %s]\n",
					sName, stateName(tr.Next), label)
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

// WriteDotGraph writes a representaWriteDotGraphStringtion of the Transitions graph in the
// graphviz dot format. It accepts a starting State that will be expressed as
// such in the graph, if provided.
func (t Transitions) WriteDotGraph(w io.Writer, start State) {
	genDot(w, t, stateName(start))
}

// WriteDotGraphString is like WriteDotGraph, but takes the string
// representation of the start State.
func (t Transitions) WriteDotGraphString(w io.Writer, start string) {
	start = trimState(start)
	if !strings.Contains(start, "{") {
		start += "{}"
	}
	genDot(w, t, start)
}

// Silence unused warning for Transitions.WriteDotGraphString. The method
// is used by write_reports.go.tmpl.
var _ = (Transitions).WriteDotGraphString
