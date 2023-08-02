// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package event

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
)

type DelayedEventList []DelayedEvent

// Len implements sort.Interface.
func (del DelayedEventList) Len() int { return len(del) }

// Less implements sort.Interface.
func (del DelayedEventList) Less(i, j int) bool {
	if del[i].At == del[j].At {
		return i < j
	}
	return del[i].At.Before(del[j].At)
}

// Swap implements sort.Interface.
func (del DelayedEventList) Swap(i, j int) {
	del[i], del[j] = del[j], del[i]
}

type DelayedEvent struct {
	At      time.Time
	EventFn func(context.Context, time.Time, state.State)
}
