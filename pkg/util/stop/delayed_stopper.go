package stop

import (
	"container/heap"
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// DelayedStopper extends Stopper to manage queuing and cancellation of
// delayed tasks.
type DelayedStopper struct {
	*Stopper
	tasks      taskQueue
	taskChan   chan *DelayedTask
	cancelChan chan *DelayedTask
}

// NewDelayedStopper creates a new DelayedStopper from a Stopper.
func NewDelayedStopper(stopper *Stopper) *DelayedStopper {
	ds := &DelayedStopper{
		Stopper: stopper,

		taskChan:   make(chan *DelayedTask),
		cancelChan: make(chan *DelayedTask),
	}
	_ = ds.RunAsyncTask(context.Background(), "delayed stopper", ds.run)
	return ds
}

// DelayedTask is returned from RunDelayedAsyncTask.
// It exposes a Cancel method which can be used to cancel the task before it
// has been started.
type DelayedTask struct {
	ds        *DelayedStopper
	ctx       context.Context
	taskName  string
	f         func(context.Context)
	startTime time.Time

	idx int // idx is the index into a taskQueue
}

// Cancel attempts to prevent execution of the DelayedTask before it has been
// started. Calls to Cancel after the task has been run are a no-op.
func (t *DelayedTask) Cancel() {
	if t.ds == nil {
		return
	}
	select {
	case t.ds.cancelChan <- t:
	case <-t.ds.ShouldQuiesce():
	}
}

// RunDelayedAsyncTask queues a task for executaion after the provided delay.
// An error is returned if the context is cancelled or the DelayedStopper is
// shutting down before the task can be queued. Otherwise the returned object
// can be used to cancel the task while its queued. The method task an optional
// DelayedTask pointer to allow clients to avoid an allocation by storing the
// DelayedTask struct as a member of another struct.
func (ds *DelayedStopper) RunDelayedAsyncTask(
	ctx context.Context,
	taskName string,
	f func(context.Context),
	delay time.Duration,
	t *DelayedTask,
) (*DelayedTask, error) {
	if t == nil {
		t = &DelayedTask{}
	}
	initTask(ctx, t, ds, taskName, f, delay)
	select {
	case <-ds.ShouldQuiesce():
		return nil, ErrUnavailable
	case ds.taskChan <- t:
		return t, nil
	}
}

func initTask(
	ctx context.Context,
	t *DelayedTask,
	ds *DelayedStopper,
	taskName string,
	f func(context.Context),
	delay time.Duration,
) {
	*t = DelayedTask{
		ds:        ds,
		idx:       -1,
		ctx:       ctx,
		taskName:  taskName,
		f:         f,
		startTime: timeutil.Now().Add(delay),
	}
}

func (ds *DelayedStopper) run(ctx context.Context) {
	var (
		startTime     time.Time
		timer         = timeutil.NewTimer()
		maybeSetTimer = func() {
			var nextStartTime time.Time
			if next := ds.tasks.peekFront(); next != nil {
				nextStartTime = next.startTime
			}
			if !startTime.Equal(nextStartTime) {
				startTime = nextStartTime
				if !startTime.IsZero() {
					timer.Reset(time.Until(startTime))
				} else {
					// Clear the current timer due to a sole batch already sent before
					// the timer fired.
					timer.Stop()
					timer = timeutil.NewTimer()
				}
			}
		}
	)
	for {
		select {
		case t := <-ds.taskChan:
			heap.Push(&ds.tasks, t)
		case t := <-ds.cancelChan:
			if t.idx != -1 {
				ds.tasks.remove(t)
			}
		case <-timer.C:
			timer.Read = true
			t := ds.tasks.popFront()
			_ = ds.RunAsyncTask(t.ctx, t.taskName, t.f)
		case <-ds.ShouldQuiesce():
			return
		}
		maybeSetTimer()
	}
}

type taskQueue []*DelayedTask

func (q *taskQueue) remove(t *DelayedTask) {
	heap.Remove(q, t.idx)
}

func (q *taskQueue) peekFront() *DelayedTask {
	if len(*q) == 0 {
		return nil
	}
	return (*q)[0]
}

func (q *taskQueue) popFront() *DelayedTask {
	if len(*q) == 0 {
		return nil
	}
	return heap.Pop(q).(*DelayedTask)
}

func (q *taskQueue) Len() int {
	return len(*q)
}

func (q *taskQueue) Less(i, j int) bool {
	return (*q)[i].startTime.Before((*q)[j].startTime)
}

func (q *taskQueue) Swap(i, j int) {
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
	(*q)[i].idx, (*q)[j].idx = i, j
}

func (q *taskQueue) Push(v interface{}) {
	t := v.(*DelayedTask)
	t.idx = len(*q)
	*q = append(*q, t)
}

func (q *taskQueue) Pop() interface{} {
	t := (*q)[len(*q)-1]
	t.idx = -1
	(*q) = (*q)[:len(*q)-1]
	return t
}
