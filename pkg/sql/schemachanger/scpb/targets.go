package scpb

import "gopkg.in/yaml.v2"

var NumStates = len(State_name)

var _ yaml.Marshaler = (*State)(nil)
var _ yaml.Unmarshaler = (*State)(nil)

// MarshalYAML implements yaml.Marshaler.
// Used for testing.
func (s State) MarshalYAML() (interface{}, error) {
	return s.String(), nil
}

// UnmarshalYAML implements yaml.Unmarshaler.
// Used for testing.
func (s *State) UnmarshalYAML(fn func(interface{}) error) error {
	var str string
	if err := fn(&str); err != nil {
		return err
	}
	*s = State(State_value[str])
	return nil
}

type TargetState struct {
	Target Target
	State  State
}

func (s TargetState) Transition(to State) TargetState {
	return TargetState{Target: s.Target, State: to}
}

type Target interface {
	target()
}

func (*AddCheckConstraint) target()            {}
func (*AddCheckConstraintUnvalidated) target() {}
func (*AddColumn) target()                     {}
func (*AddColumnFamily) target()               {}
func (*AddIndex) target()                      {}
func (*AddPrimaryIndex) target()               {}
func (*AddSequenceDependency) target()         {}
func (*AddUniqueConstraint) target()           {}
func (*DropCheckConstraint) target()           {}
func (*DropColumn) target()                    {}
func (*DropIndex) target()                     {}
func (*DropPrimaryIndex) target()              {}
func (*DropUniqueConstraint) target()          {}
