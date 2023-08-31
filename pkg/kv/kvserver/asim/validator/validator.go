// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package validator

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/event"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/scheduled"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

// Validate checks for any invalid events. Currently, it only checks
// SetSpanConfigEvent for the presence of unsatisfiable configurations. But it
// can be extended to validate the initial state and other events as well.
func Validate(initialState state.State, events scheduled.EventExecutor) string {
	buf := strings.Builder{}
	buf.WriteString("validation result:\n")
	failed := false

	// Since all constraint checks utilize the same cluster info, we process the
	// cluster info once and reuse it.
	zoneToRegion, zone, region, total := processClusterInfo(initialState.ClusterInfo().Regions)
	for _, se := range events.ScheduledEvents() {
		if e, ok := se.TargetEvent.(event.SetSpanConfigEvent); ok {
			// Create a new mockAllocator for every constraint satisfiability check as
			// isSatisfiable directly modifies mockAllocator fields.
			ma := newMockAllocator(zoneToRegion, zone, region, total)
			if success, reason := ma.isSatisfiable(e.Config); !success {
				failed = true
				buf.WriteString(fmt.Sprintf("\tevent scheduled at %s is expected to lead to failure\n", se.At.Format("2006-01-02 15:04:05")))
				buf.WriteString(fmt.Sprintf("\t\tunsatisfiable: %s\n", reason))
			}
		}
	}
	if !failed {
		buf.WriteString("\tvalid\n")
	}
	return buf.String()
}
