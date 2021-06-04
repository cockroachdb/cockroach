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
	"strings"
)

type Event interface {
	fmt.Stringer
}

type EvSeqExhausted struct {
	Seq string
}

func (e *EvSeqExhausted) String() string {
	return fmt.Sprintf("%s is exhausted", e.Seq)
}

type EvStepStart struct {
	Worker int
	Seq, S string
}

func (e *EvStepStart) String() string {
	return fmt.Sprintf("%s.%s started", e.Seq, e.S)
}

type EvStepDone struct {
	Worker         int
	Seq, S         string
	SequenceFailed bool
	Result         interface{}
}

func (e *EvStepDone) String() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%s.%s ", e.Seq, e.S)
	// NB: a nil result should never have !SequenceFailed but
	// this code shouldn't need to rely on that.
	if e.Result == nil && !e.SequenceFailed {
		fmt.Fprintf(&buf, "succeded")
	} else {
		fmt.Fprintf(&buf, "failed: %v", e.Result)
	}
	return buf.String()
}

type EvUnstructured struct {
	Worker  int
	Message string
}

func (e *EvUnstructured) String() string {
	return e.Message
}
