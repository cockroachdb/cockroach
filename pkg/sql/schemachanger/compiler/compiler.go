package compiler

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
)

type Stage struct {
	Ops         []ops.Op
	NextTargets []targets.TargetState
}

type ExecutionPhase int

const (
	PostStatementPhase ExecutionPhase = iota
	PreCommitPhase
	PostCommitPhase
)

type compileFlags struct {
	ExecutionPhase       ExecutionPhase
	CreatedDescriptorIDs []descpb.ID
}

func compile(targets []targets.TargetState, flags compileFlags) ([]Stage, error) {

}
