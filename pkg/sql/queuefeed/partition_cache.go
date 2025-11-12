package queuefeed

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
)

type partitionCache struct {
	// partitions is a stale cache of the current state of the partition's
	// table. Any assignment decisions and updates should be made using
	// transactions.
	partitions map[int64]Partition

	// assignmentIndex is a map of sessions to assigned partitions.
	assignmentIndex map[Session]map[int64]struct{}

	sessions map[Session]struct{}
}

func (p *partitionCache) DebugString() string {
	var result strings.Builder

	result.WriteString("PartitionCache Debug:\n")
	result.WriteString("===================\n\n")

	// Print partitions
	result.WriteString("Partitions:\n")
	if len(p.partitions) == 0 {
		result.WriteString("  (none)\n")
	} else {
		for id, partition := range p.partitions {
			result.WriteString(fmt.Sprintf("  ID: %d", id))
			if !partition.Session.Empty() {
				result.WriteString(fmt.Sprintf(" | Session: %s", partition.Session.ConnectionID.String()[:8]))
			} else {
				result.WriteString(" | Session: (unassigned)")
			}
			result.WriteString("\n")
		}
	}

	// Print assignment index
	result.WriteString("\nAssignment Index (session -> partitions):\n")
	if len(p.assignmentIndex) == 0 {
		result.WriteString("  (none)\n")
	} else {
		for session, partitions := range p.assignmentIndex {
			result.WriteString(fmt.Sprintf("  %s: [", session.ConnectionID.String()[:8]))
			partitionIDs := make([]int64, 0, len(partitions))
			for id := range partitions {
				partitionIDs = append(partitionIDs, id)
			}
			sort.Slice(partitionIDs, func(i, j int) bool {
				return partitionIDs[i] < partitionIDs[j]
			})
			for i, id := range partitionIDs {
				if i > 0 {
					result.WriteString(", ")
				}
				result.WriteString(fmt.Sprintf("%d", id))
			}
			result.WriteString("]\n")
		}
	}

	return result.String()
}

func (p *partitionCache) Init(partitions []Partition) {
	p.partitions = make(map[int64]Partition)
	p.assignmentIndex = make(map[Session]map[int64]struct{})

	for _, partition := range partitions {
		p.addPartition(partition)
	}
}

func (p *partitionCache) Update(partitions map[int64]Partition) {
	// TODO(queuefeed): When we introduce rangefeeds we probably need to add mvcc
	// version to Partition to make sure updates from sql statements are kept
	// coherent with updates from the rangefeed.
	for id, newPartition := range partitions {
		oldPartition := p.partitions[id]
		switch {
		case newPartition.Empty():
			p.removePartition(id)
		case oldPartition.Empty():
			p.addPartition(newPartition)
		default:
			p.updatePartition(oldPartition, newPartition)
		}
	}
}

func (p *partitionCache) removePartition(partitionID int64) {
	partition, exists := p.partitions[partitionID]
	if !exists {
		return
	}

	delete(p.partitions, partitionID)

	// Remove from session index
	if !partition.Session.Empty() {
		if sessions, ok := p.assignmentIndex[partition.Session]; ok {
			delete(sessions, partitionID)
			if len(sessions) == 0 {
				delete(p.assignmentIndex, partition.Session)
			}
		}
	}
}

func (p *partitionCache) addPartition(partition Partition) {
	// Add to main partition map
	p.partitions[partition.ID] = partition

	// Add to session index and partition index for assigned session
	if !partition.Session.Empty() {
		if _, ok := p.assignmentIndex[partition.Session]; !ok {
			p.assignmentIndex[partition.Session] = make(map[int64]struct{})
		}
		p.assignmentIndex[partition.Session][partition.ID] = struct{}{}
	}

}

func (p *partitionCache) updatePartition(oldPartition, newPartition Partition) {
	// Update main partition map
	p.partitions[newPartition.ID] = newPartition

	// Remove old session assignments
	if !oldPartition.Session.Empty() {
		if sessions, ok := p.assignmentIndex[oldPartition.Session]; ok {
			delete(sessions, oldPartition.ID)
			if len(sessions) == 0 {
				delete(p.assignmentIndex, oldPartition.Session)
			}
		}
	}

	// Add new session assignments
	if !newPartition.Session.Empty() {
		if _, ok := p.assignmentIndex[newPartition.Session]; !ok {
			p.assignmentIndex[newPartition.Session] = make(map[int64]struct{})
		}
		p.assignmentIndex[newPartition.Session][newPartition.ID] = struct{}{}
	}
}

func (p *partitionCache) isStale(assignment *Assignment) bool {
	cachedAssignment := p.assignmentIndex[assignment.Session]
	if len(assignment.Partitions) != len(cachedAssignment) {
		return true
	}
	for _, partition := range assignment.Partitions {
		if _, ok := cachedAssignment[partition.ID]; !ok {
			return true
		}
	}
	return false
}

func (p *partitionCache) constructAssignment(session Session) *Assignment {
	assignment := &Assignment{
		Session:    session,
		Partitions: make([]Partition, 0, len(p.assignmentIndex[session])),
	}
	for partitionID := range p.assignmentIndex[session] {
		assignment.Partitions = append(assignment.Partitions, p.partitions[partitionID])
	}
	sort.Slice(assignment.Partitions, func(i, j int) bool {
		return assignment.Partitions[i].ID < assignment.Partitions[j].ID
	})
	return assignment
}

func (p *partitionCache) planRegister(
	session Session, cache partitionCache,
) (tryClaim Partition, trySteal Partition) {
	// Check to see if there is an an unassigned partition that can be claimed.
	for _, partition := range cache.partitions {
		if partition.Session.Empty() {
			return partition, Partition{}
		}
	}
	maxPartitions := (len(p.partitions) + len(p.assignmentIndex) - 1) / len(p.assignmentIndex)
	return Partition{}, p.planTheft(1, maxPartitions)
}

func (p *partitionCache) planAssignment(
	session Session, caughtUp bool, cache partitionCache,
) (tryRelease []Partition, tryClaim Partition, trySteal Partition) {

	for partitionId := range p.assignmentIndex[session] {
		partition := p.partitions[partitionId]
		if !partition.Successor.Empty() {
			tryRelease = append(tryRelease, partition)
		}
	}
	if len(tryRelease) != 0 {
		return tryRelease, Partition{}, Partition{}
	}

	// If we aren't caught up, we should not try to claim any new partitions.
	if !caughtUp {
		return nil, Partition{}, Partition{}
	}

	// Check to see if there is an an unassigned partition that can be claimed.
	for _, partition := range p.partitions {
		// TODO(jeffswenson): we should really try to claim a random partition to
		// avoid contention.
		if partition.Session.Empty() {
			return nil, partition, Partition{}
		}
	}

	// maxPartitions is the maximum number of partitions we would expect to be
	// assigned to this session.
	maxPartitions := len(p.partitions)
	if len(p.assignmentIndex) != 0 {
		maxPartitions = (len(p.partitions) + len(p.assignmentIndex) - 1) / len(p.assignmentIndex)
	}
	assignedPartitions := len(p.assignmentIndex[session])
	if maxPartitions <= assignedPartitions {
		return nil, Partition{}, Partition{}
	}

	// NOTE: planTheft may return an empty partition. E.g. consider the case
	// where there are two sessions and three partitions. In that case the
	// maximum partition assignment is 2, but one partition will end up with
	// only 1 assignment. It will consider stealing even though the partitions
	// are balanced.
	//
	// We prioritize stealing sessions from any client that has more than the
	// maximum expected number of partitions. But we are willing to steal from
	// any client that has two more partitions than this client currently has.
	// Stealing from someone with less than the maximum expected number of is
	// needed to handle distributions like:
	// a -> 3 partitions
	// b -> 3 partitions
	// c -> 1 partition
	return nil, Partition{}, p.planTheft(assignedPartitions+1, maxPartitions)
}

// planTheft selects a partition from a session that has more partitions
// assigned to it than the richTreshold. E.g. `richTreshold` is 1, any session
// with 2 or more partitions is a candidate for work stealing.
func (p *partitionCache) planTheft(minumExpected, maximumExpected int) Partition {
	richCandidates, eligibleCandidates := []Partition{}, []Partition{}
	for _, session := range p.assignmentIndex {
		assignedPartitions := len(session)
		if maximumExpected < assignedPartitions {
			for partitionID := range session {
				richCandidates = append(richCandidates, p.partitions[partitionID])
			}
		}
		if minumExpected < assignedPartitions {
			for partitionID := range session {
				eligibleCandidates = append(eligibleCandidates, p.partitions[partitionID])
			}
		}
	}

	if len(richCandidates) != 0 {
		return richCandidates[rand.Intn(len(richCandidates))]
	}
	if len(eligibleCandidates) != 0 {
		return eligibleCandidates[rand.Intn(len(eligibleCandidates))]
	}
	return Partition{}
}
