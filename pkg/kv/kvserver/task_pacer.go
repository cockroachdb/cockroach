package kvserver

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

type TaskPacerConfig interface {
	getRefresh() time.Duration
	getSmear() time.Duration
}

// TaskPacer controls the pacing of tasks to prevent overloading the system.
type TaskPacer struct {
	taskStartTime time.Time
	startBatchAt  time.Time
	conf          TaskPacerConfig
}

func NewTaskPacer(conf TaskPacerConfig, startTime time.Time) *TaskPacer {
	return &TaskPacer{
		taskStartTime: startTime,
		startBatchAt:  startTime,
		conf:          conf,
	}
}

func (tp *TaskPacer) getDeadline() time.Time {
	return tp.taskStartTime.Add(tp.conf.getRefresh())
}

// How to use:
// 1) NewTaskPacer()
// 2) Start(time now) -> Marks this batch of work as started. Sets StartAt as now
// 3) GetWork() -> sleeps until startAt, and Returns the amount of work to do and the time by which it should be done
func (tp *TaskPacer) GetWork(now time.Time, workLeft int) (todo int, by time.Time) {
	deadline := tp.getDeadline()
	timeLeft := deadline.Sub(now)
	quantum := tp.conf.getSmear()
	if workLeft <= 0 || timeLeft <= 0 { // ran out of work or time
		return workLeft, now
	} else if timeLeft <= quantum || quantum == 0 { // time is running out
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

func (tp *TaskPacer) Wait(until time.Time) error {
	var timer timeutil.Timer
	defer timer.Stop()

	now := timeutil.Now()
	if !now.Before(until) {
		return nil
	}

	timer.Reset(until.Sub(now))

	select {
	case <-timer.C:
		timer.Read = true
		return nil
	}
}
