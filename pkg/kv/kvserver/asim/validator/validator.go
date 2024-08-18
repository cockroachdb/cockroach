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

type Validator struct {
	zoneToRegion map[string]string
	zone         map[string]int
	region       map[string]int
	total        int
}

func NewValidator(regions []state.Region) Validator {
	// Since all constraint checks utilize the same cluster info, we process the
	// cluster info once and reuse it.
	zoneToRegion, zone, region, total := processClusterInfo(regions)
	return Validator{zoneToRegion, zone, region, total}
}

func (v Validator) ValidateEvent(se scheduled.ScheduledEvent) (satisfiable bool, err error) {
	if e, ok := se.TargetEvent.(event.SetSpanConfigEvent); ok {
		// Create a new mockAllocator for every constraint satisfiability check as
		// isSatisfiable directly modifies mockAllocator fields.
		ma := v.newMockAllocator()
		return ma.isSatisfiable(e.Config)
	}
	return true, nil

}

// Validate checks for any invalid events. Currently, it only checks
// SetSpanConfigEvent for the presence of unsatisfiable configurations. But it
// can be extended to validate the initial state and other events as well.
func (v Validator) Validate(events scheduled.ScheduledEventList) string {
	buf := strings.Builder{}
	buf.WriteString("validation result:\n")
	for _, se := range events {
		satisfiable, err := v.ValidateEvent(se)
		if satisfiable {
			buf.WriteString("\tsatisfiable\n")
		} else {
			buf.WriteString(fmt.Sprintf("\tunsatisfiable: %s\n", err))
		}
	}
	return buf.String()
}
