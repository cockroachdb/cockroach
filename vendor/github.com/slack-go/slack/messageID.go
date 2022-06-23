package slack

import "sync"

// IDGenerator provides an interface for generating integer ID values.
type IDGenerator interface {
	Next() int
}

// NewSafeID returns a new instance of an IDGenerator which is safe for
// concurrent use by multiple goroutines.
func NewSafeID(startID int) IDGenerator {
	return &safeID{
		nextID: startID,
		mutex:  &sync.Mutex{},
	}
}

type safeID struct {
	nextID int
	mutex  *sync.Mutex
}

func (s *safeID) Next() int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	id := s.nextID
	s.nextID++
	return id
}
