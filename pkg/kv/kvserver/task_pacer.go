// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import "time"

// taskPacerConfig is the interface for the configuration of a taskPacer.
type taskPacerConfig interface {
	// getRefresh returns the interval between tasks.
	getRefresh() time.Duration

	// getSmear returns the interval between batches inside a task.
	getSmear() time.Duration
}

// taskPacer controls the pacing of tasks to prevent overloading the system.
type taskPacer struct {
	// taskStartTime is the time at which the task started.
	taskStartTime time.Time
	// conf is the configuration for the taskPacer.
	conf taskPacerConfig
}

func NewTaskPacer(conf taskPacerConfig) *taskPacer {
	return &taskPacer{
		conf: conf,
	}
}

func (tp *taskPacer) StartTask(now time.Time) {
	tp.taskStartTime = now
}

// Pace returns the amount of work that should be done and the time by which it
// should be done.
// See the test TestTaskPacer for examples of how to use this method.
func (tp *taskPacer) Pace(now time.Time, workLeft int) (todo int, by time.Time) {
	deadline := tp.GetDeadline()
	timeLeft := deadline.Sub(now)
	quantum := tp.conf.getSmear()
	if workLeft <= 0 || timeLeft <= 0 { // ran out of work or time
		return workLeft, now
	} else if quantum <= 0 { // smearing is disabled
		return workLeft, deadline
	} else if timeLeft <= quantum { // time is running out
		return workLeft, deadline
	}
	// Otherwise, we have workLeft >= 1, and at least a full quantum of time.
	// Assume we can complete work at uniform speed.
	todo = int(float64(workLeft) * quantum.Seconds() / timeLeft.Seconds())
	by = now.Add(quantum)
	if todo > workLeft { // should never happen, but just in case float64 has quirks
		return workLeft, by
	} else if todo == 0 {
		return 1, by // always do some work
	}
	return todo, by
}

// GetDeadline returns the time at which the current batch of work should be
// done.
func (tp *taskPacer) GetDeadline() time.Time {
	return tp.taskStartTime.Add(tp.conf.getRefresh())
}
