package pgxpool

import (
	"time"

	"github.com/jackc/puddle"
)

// Stat is a snapshot of Pool statistics.
type Stat struct {
	s *puddle.Stat
}

// AcquireCount returns the cumulative count of successful acquires from the pool.
func (s *Stat) AcquireCount() int64 {
	return s.s.AcquireCount()
}

// AcquireDuration returns the total duration of all successful acquires from
// the pool.
func (s *Stat) AcquireDuration() time.Duration {
	return s.s.AcquireDuration()
}

// AcquiredConns returns the number of currently acquired connections in the pool.
func (s *Stat) AcquiredConns() int32 {
	return s.s.AcquiredResources()
}

// CanceledAcquireCount returns the cumulative count of acquires from the pool
// that were canceled by a context.
func (s *Stat) CanceledAcquireCount() int64 {
	return s.s.CanceledAcquireCount()
}

// ConstructingConns returns the number of conns with construction in progress in
// the pool.
func (s *Stat) ConstructingConns() int32 {
	return s.s.ConstructingResources()
}

// EmptyAcquireCount returns the cumulative count of successful acquires from the pool
// that waited for a resource to be released or constructed because the pool was
// empty.
func (s *Stat) EmptyAcquireCount() int64 {
	return s.s.EmptyAcquireCount()
}

// IdleConns returns the number of currently idle conns in the pool.
func (s *Stat) IdleConns() int32 {
	return s.s.IdleResources()
}

// MaxConns returns the maximum size of the pool.
func (s *Stat) MaxConns() int32 {
	return s.s.MaxResources()
}

// TotalConns returns the total number of resources currently in the pool.
// The value is the sum of ConstructingConns, AcquiredConns, and
// IdleConns.
func (s *Stat) TotalConns() int32 {
	return s.s.TotalResources()
}
