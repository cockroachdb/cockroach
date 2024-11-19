// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

import (
	"sort"
	"strings"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// CurrentState is a TargetState decorated with the current status of the
// elements in the target state.
type CurrentState struct {
	TargetState

	// Current holds the current statuses of the elements in the TargetState.
	// Initial is like Current, except in the statement transaction, in which
	// it instead holds the statuses at the beginning of the transaction.
	Initial, Current []Status

	// InRollback captures whether the job is currently rolling back.
	// This is important to ensure that the job can be moved to the proper
	// failed state upon restore.
	//
	// Note, if this value is true, the targets have had their directions
	// flipped already.
	//
	InRollback bool

	// Revertible captures whether the schema change, in the current state
	// can be reverted (i.e. enter rollback). In general, InRollback implies
	// that Revertible is false. Note that the value here maps to the
	// NonCancelable property of the job; if a schema change is no longer
	// Revertible, the job must be NonCancelable.
	Revertible bool
}

// ByteSize returns an estimated memory allocation for a schema change state `s`.
// We created this function since the protobuf `s.Size()` method is the size of
// `s` when it's serialized, and often differs greatly from the actual size in
// memory.
//
// TODO (xiang): gogoproto `oneof` directive, as used today for scpb.ElementProto
// is quite memory-inefficient and is the biggest contribution to the memory diff.
// Change it to use protobuf native `oneof`.
func (s CurrentState) ByteSize() (ret int64) {
	ret += int64(unsafe.Sizeof(Target{})+2*unsafe.Sizeof(Status(0))) * int64(cap(s.TargetState.Targets))
	for _, s := range s.TargetState.Statements {
		ret += int64(s.Size())
	}
	ret += int64(s.TargetState.Authorization.Size())
	ret += int64(cap(s.Initial)+cap(s.Current)) * int64(unsafe.Sizeof(Status(0)))
	ret += int64(unsafe.Sizeof(false))
	return ret
}

// WithCurrentStatuses returns a shallow copy of the current state
// with a new set of current statuses.
func (s CurrentState) WithCurrentStatuses(current []Status) CurrentState {
	ret := s
	ret.Current = current
	return ret
}

// DeepCopy returns a deep copy of the receiver.
func (s CurrentState) DeepCopy() CurrentState {
	return CurrentState{
		TargetState: *protoutil.Clone(&s.TargetState).(*TargetState),
		Initial:     append(make([]Status, 0, len(s.Initial)), s.Initial...),
		Current:     append(make([]Status, 0, len(s.Current)), s.Current...),
		InRollback:  s.InRollback,
		Revertible:  s.Revertible,
	}
}

// Rollback idempotently marks the current state as InRollback. If the
// CurrentState was not previously marked as InRollback, it reverses the
// directions of all the targets.
func (s *CurrentState) Rollback() {
	if s.InRollback {
		return
	}
	for i := range s.Targets {
		t := &s.Targets[i]
		// If the metadata is not populated this element
		// only usd for tracking.
		if !t.IsLinkedToSchemaChange() {
			continue
		}
		switch t.TargetStatus {
		case Status_ABSENT:
			t.TargetStatus = Status_PUBLIC
		default:
			t.TargetStatus = Status_ABSENT
		}
	}
	s.InRollback = true
}

// StatementTags returns the concatenated statement tags in the current state.
func (s CurrentState) StatementTags() string {
	var sb strings.Builder
	for i, stmt := range s.Statements {
		if i > 0 {
			sb.WriteString("; ")
		}
		sb.WriteString(stmt.StatementTag)
	}
	return sb.String()
}

// NumStatus is the number of values which Status may take on.
var NumStatus = len(Status_name)

// Element represents a logical component of a catalog entry's schema (e.g., an
// index or column in a table).
type Element interface {
	protoutil.Message

	// element is intended to be implemented only by the members of ElementProto.
	// Code generation implements this method.
	element()
}

type ElementGetter interface {
	Element() Element
}

//go:generate go run element_generator.go --in elements.proto --out elements_generated.go
//go:generate go run element_uml_generator.go --out uml/table.puml

// Element returns an Element from its wrapper for serialization.
func (e *ElementProto) Element() Element {
	return e.GetElementOneOf().(ElementGetter).Element()
}

type ZoneConfigElement interface {
	Element
	GetSeqNum() uint32
	GetTargetID() catid.DescID
}

var _ ZoneConfigElement = &NamedRangeZoneConfig{}
var _ ZoneConfigElement = &DatabaseZoneConfig{}
var _ ZoneConfigElement = &TableZoneConfig{}
var _ ZoneConfigElement = &IndexZoneConfig{}
var _ ZoneConfigElement = &PartitionZoneConfig{}

func (e *NamedRangeZoneConfig) GetSeqNum() uint32 {
	return e.SeqNum
}

func (e *NamedRangeZoneConfig) GetTargetID() catid.DescID {
	return e.RangeID
}

func (e *DatabaseZoneConfig) GetSeqNum() uint32 {
	return e.SeqNum
}

func (e *DatabaseZoneConfig) GetTargetID() catid.DescID {
	return e.DatabaseID
}

func (e *TableZoneConfig) GetSeqNum() uint32 {
	return e.SeqNum
}

func (e *TableZoneConfig) GetTargetID() catid.DescID {
	return e.TableID
}

func (e *IndexZoneConfig) GetSeqNum() uint32 {
	return e.SeqNum
}

func (e *IndexZoneConfig) GetTargetID() catid.DescID {
	return e.TableID
}

func (e *PartitionZoneConfig) GetSeqNum() uint32 {
	return e.SeqNum
}

func (e *PartitionZoneConfig) GetTargetID() catid.DescID {
	return e.TableID
}

// IsLinkedToSchemaChange return if a Target is linked to a schema change.
func (t *Target) IsLinkedToSchemaChange() bool {
	return t.Metadata.IsLinkedToSchemaChange()
}

// IsLinkedToSchemaChange return if a TargetMetadata is linked to a schema change.
func (t *TargetMetadata) IsLinkedToSchemaChange() bool {
	return t.Size() > 0
}

// MakeTarget constructs a new Target. The passed elem must be one of the oneOf
// members of Element. If not, this call will panic.
func MakeTarget(status TargetStatus, elem Element, metadata *TargetMetadata) Target {
	t := Target{
		TargetStatus: status.Status(),
	}
	if metadata != nil {
		t.Metadata = *protoutil.Clone(metadata).(*TargetMetadata)
	}
	t.SetElement(elem)
	return t
}

// SourceElementID elements ID's for identifying parent elements.
// This ID is dynamically allocated when any parent element is
// created and has no relation to the descriptor ID.
type SourceElementID uint32

// Clone will make a deep copy of the DescriptorState.
func (m *DescriptorState) Clone() *DescriptorState {
	if m == nil {
		return nil
	}
	return protoutil.Clone(m).(*DescriptorState)
}

// MakeCurrentStateFromDescriptors constructs a CurrentState object from a
// slice of DescriptorState object from which the current state has been
// decomposed.
func MakeCurrentStateFromDescriptors(descriptorStates []*DescriptorState) (CurrentState, error) {
	var s CurrentState
	var targetRanks []uint32
	var rollback, revertible bool
	maxRank := 0
	stmts := make(map[uint32]Statement)
	for i, cs := range descriptorStates {
		if i == 0 {
			rollback = cs.InRollback
			revertible = cs.Revertible
		} else if rollback != cs.InRollback {
			return CurrentState{}, errors.AssertionFailedf(
				"job %d: conflicting rollback statuses between descriptors",
				cs.JobID,
			)
		} else if revertible != cs.Revertible {
			return CurrentState{}, errors.AssertionFailedf(
				"job %d: conflicting revertability statuses between descriptors",
				cs.JobID,
			)
		}
		s.Initial = append(s.Initial, cs.CurrentStatuses...)
		s.Current = append(s.Current, cs.CurrentStatuses...)
		s.Targets = append(s.Targets, cs.Targets...)
		targetRanks = append(targetRanks, cs.TargetRanks...)
		for _, stmt := range cs.RelevantStatements {
			if existing, ok := stmts[stmt.StatementRank]; ok {
				if existing.Statement != stmt.Statement.Statement {
					return CurrentState{}, errors.AssertionFailedf(
						"job %d: statement %q does not match %q for rank %d",
						cs.JobID,
						existing.Statement,
						stmt.Statement,
						stmt.StatementRank,
					)
				}
			}
			if int(stmt.StatementRank) > maxRank {
				maxRank = int(stmt.StatementRank)
			}
			stmts[stmt.StatementRank] = stmt.Statement
		}
		s.Authorization = cs.Authorization
		if cs.NameMapping.ID != catid.InvalidDescID {
			s.NameMappings = append(s.NameMappings, *protoutil.Clone(&cs.NameMapping).(*NameMapping))
		}
	}
	sort.Sort(NameMappings(s.NameMappings))
	sort.Sort(&stateAndRanks{CurrentState: &s, ranks: targetRanks})
	// Statements will always be indexed by ranks, during
	// restore cases this array could become sparse. But execution
	// relies on it being indexable by rank.
	// Note: In the sparse case some statements will be left empty,
	// but these will never be accessed during execution.
	s.Statements = make([]Statement, maxRank+1)
	for rank, stmt := range stmts {
		s.Statements[rank] = stmt
	}
	s.InRollback = rollback
	s.Revertible = revertible
	return s, nil
}

type stateAndRanks struct {
	*CurrentState
	ranks []uint32
}

var _ sort.Interface = (*stateAndRanks)(nil)

func (s *stateAndRanks) Len() int           { return len(s.Targets) }
func (s *stateAndRanks) Less(i, j int) bool { return s.ranks[i] < s.ranks[j] }
func (s *stateAndRanks) Swap(i, j int) {
	s.ranks[i], s.ranks[j] = s.ranks[j], s.ranks[i]
	s.Targets[i], s.Targets[j] = s.Targets[j], s.Targets[i]
	s.Initial[i], s.Initial[j] = s.Initial[j], s.Initial[i]
	s.Current[i], s.Current[j] = s.Current[j], s.Current[i]
}
