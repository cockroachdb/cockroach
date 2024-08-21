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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type Validator struct {
	zoneToRegion map[string]string
	zone         map[string]int
	region       map[string]int
	total        int
}

type ValidationResult struct {
	Satisfiable    bool
	Configurations string
	Reason         string
}

func (v ValidationResult) String() string {
	buf := strings.Builder{}
	if v.Satisfiable {
		buf.WriteString(fmt.Sprintf("satisfiable:\n%v",
			v.Configurations))
	} else {
		buf.WriteString(fmt.Sprintf("unsatisfiable:\n%v\n%v",
			v.Configurations, v.Reason))
	}
	return buf.String()
}

func NewValidator(regions []state.Region) Validator {
	// Since all constraint checks utilize the same cluster info, we process the
	// cluster info once and reuse it.
	zoneToRegion, zone, region, total := processClusterInfo(regions)
	return Validator{zoneToRegion, zone, region, total}
}

func (v Validator) ValidateEvent(config roachpb.SpanConfig) (res ValidationResult) {
	// Create a new mockAllocator for every constraint satisfiability check as
	// isSatisfiable directly modifies mockAllocator fields.
	ma := v.newMockAllocator()
	satisfiable, err := ma.isSatisfiable(config)
	return ValidationResult{
		Satisfiable:    satisfiable,
		Configurations: fmt.Sprint(ma.String()),
		Reason:         err,
	}
}

//// Validate checks for any invalid events. Currently, it only checks
//// SetSpanConfigEvent for the presence of unsatisfiable configurations. But it
//// can be extended to validate the initial state and other events as well.
//func (v Validator) Validate(events scheduled.ScheduledEventList) string {
//	buf := strings.Builder{}
//	buf.WriteString("validation result:\n")
//	for _, se := range events {
//		if e, ok := se.TargetEvent.(event.SetSpanConfigEvent); ok {
//			buf.WriteString(fmt.Sprintf("\t%v\n", v.ValidateEvent(e)))
//		}
//	}
//	return buf.String()
//}
