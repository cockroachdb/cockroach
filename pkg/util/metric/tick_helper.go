// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metric

import "time"

var _ periodic = &tickHelper{}

// tickHelper is used by metrics that are at heart cumulative, but wish to also
// maintain a windowed version to work around limitations of our internal
// timeseries database.
type tickHelper struct {
	nextT        time.Time
	tickInterval time.Duration

	onTick func()
}

func (s *tickHelper) nextTick() time.Time {
	return s.nextT
}

func (s *tickHelper) tick() {
	s.nextT = s.nextT.Add(s.tickInterval)
	s.onTick()
}
