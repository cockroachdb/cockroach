package closetest

import (
	"fmt"
	"path"
	"runtime"
	"strings"
	"sync"

	"github.com/cockroachdb/errors"
)

type AllocationTracker struct {
	name string
	mu struct {
		sync.Mutex
		total int
		allocations map[int]*Allocation
	}
}

func NewTracker(name string) *AllocationTracker {
	tracker := &AllocationTracker{
		name: name,
	}
	tracker.mu.allocations = map[int]*Allocation{}
	return tracker
}

func (a *AllocationTracker) CheckLeaks() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(a.mu.allocations) == 0 {
		return nil
	}

	leakingStacks := map[stack]int {}
	for _, leakedAllocation := range a.mu.allocations {
		leakingStacks[leakedAllocation.stack] = leakingStacks[leakedAllocation.stack] + 1
	}
	
	var builder strings.Builder
	for leakedStack, count := range leakingStacks {
		fmt.Fprintf(&builder, "found %d copies of stack: \n", count)

		frames := runtime.CallersFrames(leakedStack.callers[:leakedStack.size])
		for index := 0; ; index++ {
			frame, more := frames.Next()
			fmt.Fprintf(&builder, "%d: %s %s:%d\n", index, path.Base(frame.Function), path.Base(frame.File), frame.Line)
			if !more {
				break
			}
		}
	}
	
	return errors.Newf(
		"leaked %d of %d %s instances from %d unique allocation stacks:\n%s",
		len(a.mu.allocations), a.mu.total, a.name, len(leakingStacks), builder.String())
}

func (a *AllocationTracker) TrackAllocation(ignoreCallers int) *Allocation {
	a.mu.Lock()
	defer a.mu.Unlock()

	id := a.mu.total
	a.mu.total += 1

	allocation := &Allocation {
		tracker: a,
		id: id,
	}
	allocation.stack.size = runtime.Callers(ignoreCallers + 1, allocation.stack.callers[:])

	a.mu.allocations[id] = allocation

	return allocation
}

type stack struct {
	callers [32]uintptr
	size    int
}

type Allocation struct {
	tracker *AllocationTracker
	stack
	id      int
}

func (a *Allocation) Close() {
	a.tracker.mu.Lock()
	defer a.tracker.mu.Unlock()
	delete(a.tracker.mu.allocations, a.id)
}
