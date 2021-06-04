// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package layered

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type event struct {
	ts       time.Time
	worker   string
	activity string
}

type UMLRecorder struct {
	mu  syncutil.Mutex
	evs []event
}

func (r *UMLRecorder) Record(ts time.Time, worker string, activity string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, event{
		ts:       ts,
		worker:   worker,
		activity: activity,
	})
}

func (r *UMLRecorder) Idle(ts time.Time, worker string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, event{
		ts:       ts,
		worker:   worker,
		activity: "{-}",
	})
}

func (r *UMLRecorder) Hidden(ts time.Time, worker string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.evs = append(r.evs, event{
		ts:       ts,
		worker:   worker,
		activity: "{hidden}",
	})
}

func (r UMLRecorder) String() string {
	sort.SliceStable(r.evs, func(i, j int) bool {
		ei, ej := r.evs[i], r.evs[j]
		if ei.ts == ej.ts {
			if ei.worker != ej.worker {
				return ei.worker < ej.worker
			}
			// Especially in unit tests (where time ticks are discrete)
			// we sometimes emit multiple states for a step. It turns
			// out that plantUML will disregard all but the first state,
			// so we make sure that {-} sorts last. This makes sure that
			// if one activity ends and another one begins, the "later"
			// state (which is not {-}) wins.
			return ei.activity < ej.activity
		}
		return ei.ts.Before(ej.ts)
	})

	var buf strings.Builder
	fmt.Fprintln(&buf, `@startuml`)

	umlWorkers := map[string]string{} // worker name -> UML worker name
	{
		var n int
		for _, ev := range r.evs {
			if _, ok := umlWorkers[ev.worker]; ok {
				continue
			}
			n++
			umlWorkers[ev.worker] = fmt.Sprintf("W%d", n)
			fmt.Fprintf(&buf, `concise "%s" as %s`, ev.worker, umlWorkers[ev.worker])
			fmt.Fprintln(&buf)
		}
	}

	for i := range r.evs {
		if i == 0 || r.evs[i-1].ts != r.evs[i].ts {
			d := r.evs[i].ts.Sub(r.evs[0].ts)
			fmt.Fprintf(&buf, "@%s\n", (time.Time{}).Add(d).UTC().Format("15:04:05"))
		}
		w := umlWorkers[r.evs[i].worker]
		act := r.evs[i].activity
		if act != "" {
			fmt.Fprintf(&buf, "%s is %q\n", w, act)
		} else {
			fmt.Fprintf(&buf, "%s is {-}\n", w)
		}
	}
	fmt.Fprintln(&buf, "@enduml")
	return buf.String()
}
