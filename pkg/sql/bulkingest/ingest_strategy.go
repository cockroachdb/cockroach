package bulkingest

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/taskset"
)

// IngestStrategy is used to assign ingest SSTs to an ingest processor.  The
// strategy has an input channel for task completion and an output channel for
// task assignment.
//
// There are several inputs to this strategy:
// - The set of SSTs to ingest and the nodes containint the SST.
// - The set of ranges backing the SSTs and the nodes containing the ranges.
//
// There are two actions available to the strategy:
//   - Assign an SST to a processor. This will cause the processor to read the SST
//     from whichver node is storing it and write it to the range. Processors will
//     attempt to move the lease holder to the local node if possible.
//   - Move a replica of an empty range. This can be used to move replicas to nodes
//     that have no SSTs that can be assigned to ingest.
type ingestStrategy struct {
	taskAssignmentCh chan taskAssignment

	// taskCompletionCh is closed
	taskCompletionCh chan taskCompletion

	// workers contains the available worker nodes for ingestion
	workers []workerKeys

	// TODO(jeffswenson): We need access to range information in order to know
	// which nodes can be assigned to an ingest. We can use AdminRelocateRangeRequest
	// to mover anges around.
	//
	// TODO(jeffswenson): Our use of AdminRelocateRangeRequest makes it easy to
	// violate span constraints. We need to ensure that the spans are not
	// violated. If we want to productionize this, we will need to work with the
	// KV team to do it safely.
	ingestSpans []roachpb.Span

	next taskset.TaskId
	// inflight is the number of tasks that are currently being processed. After
	// closing the task assignment channel we can close the task completion
	// channel once this value reaches 0.
	inflight int
}

type workerKeys struct {
	routingKey    string
	sqlInstanceID base.SQLInstanceID
}

func newIngestStrategy(spans []roachpb.Span, workers []workerKeys) *ingestStrategy {
	return &ingestStrategy{
		ingestSpans:      spans,
		workers:          workers,
		taskAssignmentCh: make(chan taskAssignment),
		taskCompletionCh: make(chan taskCompletion),
	}
}

func (s *ingestStrategy) run(ctx context.Context) error {
	// TODO(jeffswenson): this is a stub implementation of strategy. It will
	// work correctly for initial testing because in 1 and 3 node clusters,
	// every range has a replica on every node. But on larger clusters it will
	// need to account for range placement.
	defer close(s.taskAssignmentCh)
	defer close(s.taskCompletionCh)

	// Assign the initial set of tasks to workers.
	for i := 0; i < min(len(s.workers), len(s.ingestSpans)); i++ {
		s.assignTask(s.workers[i].sqlInstanceID)
	}

	// Process task completions and assign new tasks
	for {
		completion := <-s.taskCompletionCh
		s.inflight--

		if s.doneAssigningTasks(ctx) {
			break
		}

		s.assignTask(completion.SqlInstanceID)
	}

	// Read in flight task completions
	for s.inflight != 0 {
		<-s.taskCompletionCh
		s.inflight--
	}

	return nil
}

func (s *ingestStrategy) assignTask(sqlInstanceID base.SQLInstanceID) {
	s.taskAssignmentCh <- taskAssignment{
		SqlInstanceID: sqlInstanceID,
		TaskID:        s.next,
	}
	s.next++
	s.inflight++
}

func (s *ingestStrategy) doneAssigningTasks(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	return int(s.next) >= len(s.ingestSpans)
}

func (s *ingestStrategy) assignNextTask(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	if int(s.next) >= len(s.ingestSpans) {
		return false
	}

	worker := s.workers[s.next]
	assignment := taskAssignment{
		SqlInstanceID: worker.sqlInstanceID,
		TaskID:        s.next,
	}
	s.next++
	s.inflight++
	s.taskAssignmentCh <- assignment

	return true
}

type taskAssignment struct {
	SqlInstanceID base.SQLInstanceID
	TaskID        taskset.TaskId
}

type taskCompletion struct {
	SqlInstanceID base.SQLInstanceID
	TaskID        taskset.TaskId
}
