package queuefeed

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

type assignmentSimulator struct {
	t        *testing.T
	sessions []Session
	cache    partitionCache
}

func newAssignmentSimulator(t *testing.T, partitionCount int) *assignmentSimulator {
	partitions := make([]Partition, partitionCount)
	for i := range partitions {
		partitions[i] = Partition{
			ID:        int64(i + 1),
			Session:   Session{}, // Unassigned
			Successor: Session{},
		}
	}
	sim := &assignmentSimulator{t: t}
	sim.cache.Init(partitions)
	return sim
}

// refreshAssignment returns true if refreshing the assignment took any action.
func (a *assignmentSimulator) refreshAssignment(session Session) bool {
	tryRelease, tryClaim, trySecede := a.cache.planAssignment(session, true, a.cache)

	updates := make(map[int64]Partition)

	// This is simulating the production implementation. The prod implementation
	// would use a txn to apply these changes to the DB, then update the cache
	// with the latest version of the rows.
	for _, partition := range tryRelease {
		updates[partition.ID] = Partition{
			ID:        partition.ID,
			Session:   partition.Successor,
			Successor: Session{},
			Span:      partition.Span,
		}
	}
	if !tryClaim.Empty() {
		updates[tryClaim.ID] = Partition{
			ID:        tryClaim.ID,
			Session:   session,
			Successor: Session{},
			Span:      tryClaim.Span,
		}
	}
	if !trySecede.Empty() {
		updates[trySecede.ID] = Partition{
			ID:        trySecede.ID,
			Session:   trySecede.Session,
			Successor: session,
			Span:      trySecede.Span,
		}
	}

	a.cache.Update(updates)

	return len(updates) != 0
}

func (a *assignmentSimulator) createSession() Session {
	session := Session{
		ConnectionID: uuid.MakeV4(),
		LivenessID:   sqlliveness.SessionID(uuid.MakeV4().String()),
	}

	a.sessions = append(a.sessions, session)

	// This is simulating the production implementation. The prod implementation
	// would use a txn to apply these changes to the DB, then update the cache
	// with the latest version of the rows.
	tryClaim, trySecede := a.cache.planRegister(session, a.cache)

	updates := make(map[int64]Partition)
	if !tryClaim.Empty() {
		tryClaim.Session = session
		updates[tryClaim.ID] = tryClaim
	}
	if !trySecede.Empty() {
		trySecede.Successor = session
		updates[trySecede.ID] = trySecede
	}
	a.cache.Update(updates)

	return session
}

func (a *assignmentSimulator) removeSession(session Session) {
	// Remove session from sessions list
	for i, s := range a.sessions {
		if s == session {
			a.sessions = append(a.sessions[:i], a.sessions[i+1:]...)
			break
		}
	}

	assignment := a.cache.constructAssignment(session)
	updates := make(map[int64]Partition)

	for _, partition := range assignment.Partitions {
		// For each partition assigned to this session, make the successor (if any)
		// the owner.
		updates[partition.ID] = Partition{
			ID:        partition.ID,
			Session:   partition.Successor,
			Successor: Session{}, // Clear successor
			Span:      partition.Span,
		}
	}
	// Any partitions where this session is the successor should have
	// the successor cleared.
	for id := range a.cache.partitions {
		if a.cache.partitions[id].Successor != session {
			continue
		}
		partition := a.cache.partitions[id]
		updates[partition.ID] = Partition{
			ID:        partition.ID,
			Session:   partition.Session, // Keep current owner
			Successor: Session{},         // Clear successor
			Span:      partition.Span,
		}
	}

	a.cache.Update(updates)
}

func (a *assignmentSimulator) runToStable() {
	maxIterations := 100000 // Prevent infinite loops

	for i := 0; i < maxIterations; i++ {
		actionTaken := false

		// Process each session
		for _, session := range a.sessions {
			if a.refreshAssignment(session) {
				actionTaken = true
			}
		}

		// If no action was taken in this round, we're stable
		if !actionTaken {
			return
		}
	}

	a.t.Fatalf("runToStable exceeded maximum iterations (%d sessions, %d partitions): %s ", len(a.sessions), len(a.cache.partitions), a.cache.DebugString())
}

func TestPartitionCacheSimple(t *testing.T) {
	sim := newAssignmentSimulator(t, 2)

	// Create two sessions.
	session1 := sim.createSession()
	sim.runToStable()
	session2 := sim.createSession()
	sim.runToStable()

	// Each session should have one partition.
	assignment1 := sim.cache.constructAssignment(session1)
	assignment2 := sim.cache.constructAssignment(session2)
	require.Len(t, assignment1.Partitions, 1)
	require.Len(t, assignment2.Partitions, 1)

	// After removing one session, the other session should have both partitions.
	sim.removeSession(session1)
	sim.runToStable()
	assignment2 = sim.cache.constructAssignment(session2)
	require.Len(t, assignment2.Partitions, 2)
}

func TestPartitionCacheRandom(t *testing.T) {
	partitions := rand.Intn(1000) + 1
	sessions := make([]Session, rand.Intn(100)+1)

	sim := newAssignmentSimulator(t, partitions)

	for i := range sessions {
		if rand.Int()%2 == 0 {
			sim.runToStable()
		}
		sessions[i] = sim.createSession()
	}
	sim.runToStable()

	t.Logf("%d partitions, %d sessions", partitions, len(sessions))
	t.Log(sim.cache.DebugString())

	// Verify all partitions are assigned
	for _, partition := range sim.cache.partitions {
		require.False(t, partition.Session.Empty())
	}

	// Verify that no session has more than ceil(partitions / len(sessions))
	// partitions.
	maxPerSession := (partitions + len(sessions) - 1) / len(sessions)
	for _, session := range sessions {
		assignment := sim.cache.constructAssignment(session)
		require.LessOrEqual(t, len(assignment.Partitions), maxPerSession)
	}
}
