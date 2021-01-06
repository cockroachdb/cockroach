package schemachanger

import "github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"

type Stage interface {
	Plan() Plan
	NextStates() []scpb.State
}

type Plan interface {
	Targets() []*scpb.Target

	ForwardStages() []Stage
	RevertStages() []Stage

	InitialStates() []scpb.State
	TerminalStates() []scpb.State
	CurrentStates() []scpb.State
}
