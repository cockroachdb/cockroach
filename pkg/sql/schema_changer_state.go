package sql

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"

// SchemaChangerState is state associated with the new schema changer.
type SchemaChangerState struct {
	inUse               bool
	targetStates        []scpb.TargetState
	targetStatesChanged bool
}

func (s *SchemaChangerState) setTargetStates(updated []scpb.TargetState) {
	s.targetStates = updated
	s.targetStatesChanged = true
}
