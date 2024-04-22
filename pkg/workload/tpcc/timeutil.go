// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tpcc

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"golang.org/x/exp/rand"
)

type tpccTime struct {
	syncutil.Mutex
	fakeTime time.Time
	stepMax  time.Duration
	rng      *rand.Rand
}

func (t *tpccTime) Now() time.Time {
	if t.fakeTime.IsZero() {
		return timeutil.Now()
	}

	t.Lock()
	defer t.Unlock()
	t.fakeTime = t.fakeTime.Add(
		time.Duration(t.rng.Float64()*t.stepMax.Seconds()) * time.Second,
	)
	return t.fakeTime
}

func newTpccTime(fakeStartTime time.Time, stepMax time.Duration, seed uint64) *tpccTime {
	rng := rand.New(new(rand.LockedSource))
	rng.Seed(seed)

	return &tpccTime{
		fakeTime: fakeStartTime,
		stepMax:  stepMax,
		rng:      rng,
	}
}
