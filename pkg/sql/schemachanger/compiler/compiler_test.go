package compiler

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/stretchr/testify/require"
)

func TestCompiler(t *testing.T) {
	for _, tc := range []struct {
		ts    []*targets.TargetState
		flags compileFlags
	}{
		{
			[]*targets.TargetState{
				{
					&targets.DropColumn{
						TableID:  1,
						ColumnID: 2,
					},
					targets.StatePublic,
				},
				{
					&targets.AddIndex{
						TableID:        1,
						IndexID:        2,
						PrimaryIndex:   1,
						ReplacementFor: 1,
						ColumnIDs:      []descpb.ColumnID{1},
						Primary:        true,
					},
					targets.StateAbsent,
				},
				{
					&targets.DropIndex{
						TableID:    1,
						IndexID:    1,
						ReplacedBy: 2,
						ColumnIDs:  []descpb.ColumnID{1, 2},
					},
					targets.StatePublic,
				},
			},
			compileFlags{
				ExecutionPhase: PostCommitPhase,
			},
		},
	} {
		func() {
			g, err := buildGraph(tc.ts, tc.flags)
			require.NoError(t, err)
			t.Log("\n" + g.String())
		}()
	}
}
