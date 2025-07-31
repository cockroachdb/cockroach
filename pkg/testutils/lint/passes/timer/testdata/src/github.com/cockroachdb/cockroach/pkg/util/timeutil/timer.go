// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package timeutil

import "time"

func NewTimer() *Timer {
	return &Timer{}
}

type Timer struct {
	C    <-chan time.Time
	Read bool
}

func (t *Timer) Reset(d time.Duration) {
}
