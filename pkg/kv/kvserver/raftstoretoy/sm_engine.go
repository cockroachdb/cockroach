// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// SMEngine ("state machine engine") represents the engine storing the state
// machines. Writes to it become durable only after an arbitrary delay.
type SMEngine interface {
	Apply(id roachpb.RangeID, operation WAGOperation) error
}
