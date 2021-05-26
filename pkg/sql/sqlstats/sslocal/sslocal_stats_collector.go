// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sslocal

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sessionphase"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
)

type statsCollector struct {
	sqlstats.Writer

	// phaseTimes tracks session-level phase times.
	phaseTimes *sessionphase.Times

	// previousPhaseTimes tracks the session-level phase times for the previous
	// query. This enables the `SHOW LAST QUERY STATISTICS` observer statement.
	previousPhaseTimes *sessionphase.Times
}

var _ sqlstats.StatsCollector = &statsCollector{}

// NewStatsCollector returns an instance of sqlstats.StatsCollector.
func NewStatsCollector(
	writer sqlstats.Writer, phaseTime *sessionphase.Times,
) sqlstats.StatsCollector {
	return &statsCollector{
		Writer:     writer,
		phaseTimes: phaseTime.Clone(),
	}
}

// PhaseTimes implements sqlstats.StatsCollector interface.
func (s *statsCollector) PhaseTimes() *sessionphase.Times {
	return s.phaseTimes
}

// PreviousPhaseTimes implements sqlstats.StatsCollector interface.
func (s *statsCollector) PreviousPhaseTimes() *sessionphase.Times {
	return s.previousPhaseTimes
}

// Reset implements sqlstats.StatsCollector interface.
func (s *statsCollector) Reset(writer sqlstats.Writer, phaseTime *sessionphase.Times) {
	previousPhaseTime := s.phaseTimes
	*s = statsCollector{
		Writer:             writer,
		previousPhaseTimes: previousPhaseTime,
		phaseTimes:         phaseTime.Clone(),
	}
}
