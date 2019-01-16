package toystore

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type Store struct {
	// Extremely simplified.
	mu struct {
		syncutil.RWMutex
		// NB: if we had separate "states" for preemptive snapshots, etc, we
		// would store a (simulated) enum in this struct.
		replicas map[int64]*GuardedReplica
	}
}

type Request struct {
	RangeID int64

	// Remainder omitted.
}

type Response struct{} // contents omitted

func (s *Store) Send(ctx context.Context, req Request) (Response, error) {
	// Lots of detail omitted.

	s.mu.RLock()
	guard := s.mu.replicas[req.RangeID]
	s.mu.RUnlock()

	return guard.Send(ctx, req)
}
