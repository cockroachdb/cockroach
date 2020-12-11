package sql

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"

// SchemaChangerState is state associated with the new schema changer.
type SchemaChangerState struct {
	inUse               bool
	targetStates        []targets.TargetState
	targetStatesChanged bool
}

func (s *SchemaChangerState) setTargetStates(updated []targets.TargetState) {
	s.targetStates = updated
	s.targetStatesChanged = true
}
