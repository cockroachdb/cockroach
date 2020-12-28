package targets

var NumStates = len(State_name)

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
