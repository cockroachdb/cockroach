package scplan

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/emicklei/dot"
	"github.com/stretchr/testify/require"
)

// TODO (lucy): Update the tests in this file with our new index targets (or
// figure out a better way to specify expected results).

func TestCompiler(t *testing.T) {
	for _, tc := range []struct {
		ts    []scpb.TargetState
		flags CompileFlags
	}{
		{
			[]scpb.TargetState{
				{
					&scpb.DropColumn{
						TableID: 1,
						Column: descpb.ColumnDescriptor{
							Name: "foo",
							ID:   2,
							Type: types.IntArray,
						},
					},
					scpb.State_PUBLIC,
				},
				{
					&scpb.AddIndex{
						TableID: 1,
						Index: descpb.IndexDescriptor{
							ID:        2,
							ColumnIDs: []descpb.ColumnID{1},
						},
						PrimaryIndex:   1,
						ReplacementFor: 1,
					},
					scpb.State_ABSENT,
				},
				{
					&scpb.DropIndex{
						TableID:    1,
						IndexID:    1,
						ReplacedBy: 2,
						ColumnIDs:  []descpb.ColumnID{1, 2},
					},
					scpb.State_PUBLIC,
				},
			},
			CompileFlags{
				ExecutionPhase: PostCommitPhase,
			},
		},
	} {
		func() {
			g, err := buildGraph(tc.ts, tc.flags)
			require.NoError(t, err)
			dg, err := g.drawStages()
			require.NoError(t, err)
			t.Log("\n" + dg.String())
		}()
	}
}

func TestCompile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	type compileIteration struct {
		// Must be set for the first Statement. If nil, use the most previously
		// generated targets.
		initial  []scpb.TargetState
		flags    CompileFlags
		expected []Stage
	}

	type testCase struct {
		name              string
		compileIterations []compileIteration
	}

	var testCases []testCase

	// ALTER TABLE ... ADD COLUMN;
	{
		const (
			tableID  = descpb.ID(51)
			newColID = descpb.ColumnID(2)
		)
		addColTarget := scpb.AddColumn{
			TableID: tableID,
			Column: descpb.ColumnDescriptor{
				ID: newColID,
			},
			// More column metadata
		}

		testCases = append(testCases, testCase{
			name: "add column without backfill",
			compileIterations: []compileIteration{
				{
					initial: []scpb.TargetState{
						{&addColTarget, scpb.State_ABSENT},
					},
					flags: CompileFlags{
						ExecutionPhase: PostStatementPhase,
					},
					expected: []Stage{},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PreCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.MakeAddedColumnDescriptorDeleteOnly{
									TableID: tableID,
									Column: descpb.ColumnDescriptor{
										ID: newColID,
									},
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_DELETE_ONLY},
							},
						},
					},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PostCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
							},
						},
						{
							[]scop.Op{
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: scpb.State_PUBLIC,
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_PUBLIC},
							},
						},
					},
				},
			},
		})
	}

	// CREATE INDEX ... ON ...;
	{
		const (
			tableID  = descpb.ID(51)
			newIdxID = descpb.IndexID(3)
		)
		addIdxTarget := scpb.AddIndex{
			TableID: tableID,
			Index: descpb.IndexDescriptor{
				ID:             newIdxID,
				ColumnIDs:      []descpb.ColumnID{2},
				ExtraColumnIDs: []descpb.ColumnID{1},
			},
		}

		testCases = append(testCases, testCase{
			name: "add non-unique index",
			compileIterations: []compileIteration{
				{
					initial: []scpb.TargetState{
						{&addIdxTarget, scpb.State_ABSENT},
					},
					flags: CompileFlags{
						ExecutionPhase: PostStatementPhase,
					},
					expected: []Stage{},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PreCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.MakeAddedNonPrimaryIndexDeleteOnly{
									TableID: tableID,
									Index: descpb.IndexDescriptor{
										ID:             newIdxID,
										ColumnIDs:      []descpb.ColumnID{2},
										ExtraColumnIDs: []descpb.ColumnID{1},
									},
								},
							},
							[]scpb.TargetState{
								{&addIdxTarget, scpb.State_DELETE_ONLY},
							},
						},
					},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PostCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newIdxID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addIdxTarget, scpb.State_DELETE_AND_WRITE_ONLY},
							},
						},
						{
							[]scop.Op{
								scop.IndexBackfill{
									TableID: tableID,
									IndexID: newIdxID,
								},
							},
							[]scpb.TargetState{
								{&addIdxTarget, scpb.State_VALIDATED},
							},
						},
						{
							[]scop.Op{
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newIdxID,
									NextState: scpb.State_PUBLIC,
								},
							},
							[]scpb.TargetState{
								{&addIdxTarget, scpb.State_PUBLIC},
							},
						},
					},
				},
			},
		})
	}

	// ALTER TABLE ... ADD COLUMN ... DEFAULT ...;
	{
		const (
			tableID         = descpb.ID(51)
			newColID        = descpb.ColumnID(2)
			newPrimaryIdxID = descpb.IndexID(3)
			oldPrimaryIdxID = descpb.IndexID(1)
		)
		addColTarget := scpb.AddColumn{
			TableID: tableID,
			Column: descpb.ColumnDescriptor{
				ID: newColID,
			},
		}
		addIdxTarget := scpb.AddIndex{
			TableID: tableID,
			Index: descpb.IndexDescriptor{
				ID:        newPrimaryIdxID,
				ColumnIDs: []descpb.ColumnID{newColID},
				Unique:    true,
			},
			ReplacementFor: oldPrimaryIdxID,
		}
		dropIdxTarget := scpb.DropIndex{
			TableID:    tableID,
			IndexID:    oldPrimaryIdxID,
			ReplacedBy: newPrimaryIdxID,
		}

		testCases = append(testCases, testCase{
			name: "add column with default value",
			compileIterations: []compileIteration{
				{
					initial: []scpb.TargetState{
						{&addColTarget, scpb.State_ABSENT},
						{&addIdxTarget, scpb.State_ABSENT},
						{&dropIdxTarget, scpb.State_PUBLIC},
					},
					flags: CompileFlags{
						ExecutionPhase: PostStatementPhase,
					},
					expected: []Stage{},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PreCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.MakeAddedColumnDescriptorDeleteOnly{
									TableID: tableID,
									Column: descpb.ColumnDescriptor{
										ID: newColID,
									},
								},
								scop.MakeAddedNonPrimaryIndexDeleteOnly{
									TableID: tableID,
									Index: descpb.IndexDescriptor{
										ID:        newPrimaryIdxID,
										ColumnIDs: []descpb.ColumnID{newColID},
										Unique:    true,
									},
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_DELETE_ONLY},
								{&addIdxTarget, scpb.State_DELETE_ONLY},
								{&dropIdxTarget, scpb.State_PUBLIC},
							},
						},
					},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PostCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addIdxTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&dropIdxTarget, scpb.State_PUBLIC},
							},
						},
						{
							[]scop.Op{
								scop.IndexBackfill{
									TableID: tableID,
									IndexID: newPrimaryIdxID,
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addIdxTarget, scpb.State_BACKFILLED},
								{&dropIdxTarget, scpb.State_PUBLIC},
							},
						},
						// The validation step isn't actually necessary because the new PK
						// has the same set of unique columns as the old PK. I think we can
						// eliminate it in general when adding/dropping columns.
						{
							[]scop.Op{
								scop.UniqueIndexValidation{
									TableID: tableID,
									IndexID: newPrimaryIdxID,
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addIdxTarget, scpb.State_VALIDATED},
								{&dropIdxTarget, scpb.State_PUBLIC},
							},
						},
						{
							[]scop.Op{
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: scpb.State_PUBLIC,
								},
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: scpb.State_PUBLIC,
								},
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_PUBLIC},
								{&addIdxTarget, scpb.State_PUBLIC},
								{&dropIdxTarget, scpb.State_DELETE_AND_WRITE_ONLY},
							},
						},
						{
							[]scop.Op{
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: scpb.State_DELETE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_PUBLIC},
								{&addIdxTarget, scpb.State_PUBLIC},
								{&dropIdxTarget, scpb.State_DELETE_ONLY},
							},
						},
						{
							[]scop.Op{
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: scpb.State_ABSENT,
								},
							},
							[]scpb.TargetState{
								{&addColTarget, scpb.State_PUBLIC},
								{&addIdxTarget, scpb.State_PUBLIC},
								{&dropIdxTarget, scpb.State_ABSENT},
							},
						},
					},
				},
			},
		})
	}

	// This is the example in compiler.md.
	//
	// BEGIN;
	// ALTER TABLE ... DROP COLUMN ...; -- (1)
	// ALTER TABLE ... ADD COLUMN ... UNIQUE DEFAULT ...; -- (2)
	// COMMIT;
	{
		const (
			tableID         = descpb.ID(51)
			oldColID        = descpb.ColumnID(1)
			newPrimaryIdxID = descpb.IndexID(3)
			oldPrimaryIdxID = descpb.IndexID(1)
			newColID        = descpb.ColumnID(2)
			newUniqueIdxID  = descpb.IndexID(4)
		)
		dropColTarget := scpb.AddColumn{
			TableID: tableID,
			Column: descpb.ColumnDescriptor{
				ID: newColID,
			},
		}
		addPrimaryIdxTargetStmt1 := scpb.AddIndex{
			TableID: tableID,
			Index: descpb.IndexDescriptor{
				ID:     newPrimaryIdxID,
				Unique: true,
				// TODO: ColumnIDs....
			},
			ReplacementFor: oldPrimaryIdxID,
		}
		dropPrimaryIdxTargetStmt1 := scpb.DropIndex{
			TableID:    tableID,
			IndexID:    oldPrimaryIdxID,
			ReplacedBy: newPrimaryIdxID,
		}
		addPrimaryIdxTarget := scpb.AddIndex{
			TableID: tableID,
			Index: descpb.IndexDescriptor{
				ID:     newPrimaryIdxID,
				Unique: true,
				// TODO: ColumnIDs....
			},
			ReplacementFor: oldPrimaryIdxID,
		}
		dropPrimaryIdxTarget := scpb.DropIndex{
			TableID:    tableID,
			IndexID:    oldPrimaryIdxID,
			ReplacedBy: newPrimaryIdxID,
		}
		addColTarget := scpb.AddColumn{
			TableID: tableID,
			Column: descpb.ColumnDescriptor{
				ID: newColID,
			},
		}
		addUniqueIdxTarget := scpb.DropIndex{
			TableID:   tableID,
			IndexID:   newUniqueIdxID,
			ColumnIDs: []descpb.ColumnID{newColID},
		}
		addUniqueConstraintTarget := scpb.AddUniqueConstraint{
			TableID:   tableID,
			IndexID:   newUniqueIdxID,
			ColumnIDs: []descpb.ColumnID{newColID},
		}

		testCases = append(testCases, testCase{
			name: "drop column and add column with unique index and default value",
			compileIterations: []compileIteration{
				{
					initial: []scpb.TargetState{
						{&addPrimaryIdxTargetStmt1, scpb.State_ABSENT},
						{&dropPrimaryIdxTargetStmt1, scpb.State_PUBLIC},
						{&dropColTarget, scpb.State_PUBLIC},
					},
					flags: CompileFlags{
						ExecutionPhase: PostStatementPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  oldColID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTargetStmt1, scpb.State_ABSENT},
								{&dropPrimaryIdxTargetStmt1, scpb.State_PUBLIC},
								{&dropColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
							},
						},
					},
				},
				{
					initial: []scpb.TargetState{
						{&addPrimaryIdxTarget, scpb.State_ABSENT},
						{&dropPrimaryIdxTarget, scpb.State_PUBLIC},
						{&dropColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
						{&addColTarget, scpb.State_ABSENT},
						{&addUniqueIdxTarget, scpb.State_ABSENT},
						{&addUniqueConstraintTarget, scpb.State_ABSENT},
					},
					flags: CompileFlags{
						ExecutionPhase: PostStatementPhase,
					},
					expected: []Stage{},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PreCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.MakeAddedColumnDescriptorDeleteOnly{
									TableID: tableID,
									Column: descpb.ColumnDescriptor{
										ID: newColID,
									},
								},
								scop.MakeAddedNonPrimaryIndexDeleteOnly{
									TableID: tableID,
									Index: descpb.IndexDescriptor{
										ID:     newPrimaryIdxID,
										Unique: true,
										// TODO: ColumnIDs....
									},
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTarget, scpb.State_DELETE_ONLY},
								{&dropPrimaryIdxTarget, scpb.State_PUBLIC},
								{&dropColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, scpb.State_DELETE_ONLY},
								{&addUniqueIdxTarget, scpb.State_ABSENT},
								{&addUniqueConstraintTarget, scpb.State_ABSENT},
							},
						},
					},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PostCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&dropPrimaryIdxTarget, scpb.State_PUBLIC},
								{&dropColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addUniqueIdxTarget, scpb.State_ABSENT},
								{&addUniqueConstraintTarget, scpb.State_ABSENT},
							},
						},
						{
							[]scop.Op{
								scop.IndexBackfill{
									TableID: tableID,
									IndexID: newPrimaryIdxID,
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTarget, scpb.State_BACKFILLED},
								{&dropPrimaryIdxTarget, scpb.State_PUBLIC},
								{&dropColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addUniqueIdxTarget, scpb.State_ABSENT},
								{&addUniqueConstraintTarget, scpb.State_ABSENT},
							},
						},
						{
							[]scop.Op{
								// This also moves the unique constraint to State_DELETE_AND_WRITE_ONLY.
								scop.IndexBackfill{
									TableID: tableID,
									IndexID: newUniqueIdxID,
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTarget, scpb.State_BACKFILLED},
								{&dropPrimaryIdxTarget, scpb.State_PUBLIC},
								{&dropColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addUniqueIdxTarget, scpb.State_BACKFILLED},
								{&addUniqueConstraintTarget, scpb.State_DELETE_AND_WRITE_ONLY},
							},
						},
						{
							[]scop.Op{
								scop.UniqueIndexValidation{
									TableID:        tableID,
									PrimaryIndexID: newPrimaryIdxID, // TODO: Do we want the new non-public index?
									IndexID:        newUniqueIdxID,
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTarget, scpb.State_BACKFILLED},
								{&dropPrimaryIdxTarget, scpb.State_PUBLIC},
								{&dropColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addUniqueIdxTarget, scpb.State_BACKFILLED},
								{&addUniqueConstraintTarget, scpb.State_VALIDATED},
							},
						},
						{
							[]scop.Op{
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: scpb.State_PUBLIC,
								},
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: scpb.State_PUBLIC,
								},
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: scpb.State_DELETE_AND_WRITE_ONLY,
								},
								// This also makes the unique constraint public. Eventually we
								// will need a way to update states for index-less unique
								// constraints.
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newUniqueIdxID,
									NextState: scpb.State_PUBLIC,
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTarget, scpb.State_PUBLIC},
								{&dropPrimaryIdxTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&dropColTarget, scpb.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, scpb.State_PUBLIC},
								{&addUniqueIdxTarget, scpb.State_PUBLIC},
								{&addUniqueConstraintTarget, scpb.State_PUBLIC},
							},
						},
						{
							[]scop.Op{
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: scpb.State_DELETE_ONLY,
								},
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  oldColID,
									NextState: scpb.State_DELETE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTarget, scpb.State_PUBLIC},
								{&dropPrimaryIdxTarget, scpb.State_DELETE_ONLY},
								{&dropColTarget, scpb.State_DELETE_ONLY},
								{&addColTarget, scpb.State_PUBLIC},
								{&addUniqueIdxTarget, scpb.State_PUBLIC},
								{&addUniqueConstraintTarget, scpb.State_PUBLIC},
							},
						},
						{
							[]scop.Op{
								scop.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: scpb.State_DELETE_ONLY,
								},
								scop.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  oldColID,
									NextState: scpb.State_DELETE_ONLY,
								},
							},
							[]scpb.TargetState{
								{&addPrimaryIdxTarget, scpb.State_PUBLIC},
								{&dropPrimaryIdxTarget, scpb.State_ABSENT},
								{&dropColTarget, scpb.State_ABSENT},
								{&addColTarget, scpb.State_PUBLIC},
								{&addUniqueIdxTarget, scpb.State_PUBLIC},
								{&addUniqueConstraintTarget, scpb.State_PUBLIC},
							},
						},
					},
				},
			},
		})
	}

	// Proposal for new behavior. Does this work?
	//
	// If all columns are already public, we should be able to put the check
	// constraint in State_DELETE_AND_WRITE_ONLY during statement execution. If those
	// columns are renamed or dropped, we can just internally update or remove the
	// constraint, respectively.
	//
	// ALTER TABLE ... ADD CHECK ...;
	{
		const (
			tableID   = descpb.ID(51)
			columnID  = descpb.ColumnID(1)
			checkName = "check"
			checkExpr = "[expr]"
		)
		addCheckTarget := scpb.AddCheckConstraint{
			TableID:   tableID,
			Name:      checkName,
			Expr:      checkExpr,
			ColumnIDs: []descpb.ColumnID{columnID},
		}

		testCases = append(testCases, testCase{
			name: "add check constraint",
			compileIterations: []compileIteration{
				{
					initial: []scpb.TargetState{
						{&addCheckTarget, scpb.State_ABSENT},
					},
					flags: CompileFlags{
						ExecutionPhase: PostStatementPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.AddCheckConstraint{
									TableID:   tableID,
									Name:      checkName,
									Expr:      checkExpr,
									ColumnIDs: []descpb.ColumnID{columnID},
								},
							},
							[]scpb.TargetState{
								{&addCheckTarget, scpb.State_DELETE_AND_WRITE_ONLY},
							},
						},
					},
				},
				{
					flags: CompileFlags{
						ExecutionPhase: PreCommitPhase,
					},
					expected: []Stage{},
				},
				{
					initial: []scpb.TargetState{
						{&addCheckTarget, scpb.State_DELETE_AND_WRITE_ONLY},
					},
					flags: CompileFlags{
						ExecutionPhase: PostCommitPhase,
					},
					expected: []Stage{
						{
							[]scop.Op{
								scop.ValidateCheckConstraint{
									TableID: tableID,
									Name:    checkName,
								},
							},
							[]scpb.TargetState{
								{&addCheckTarget, scpb.State_VALIDATED},
							},
						},
						{
							[]scop.Op{
								scop.CheckConstraintStateChange{
									TableID:   tableID,
									Name:      checkName,
									NextState: scpb.State_PUBLIC,
								},
							},
							[]scpb.TargetState{
								{&addCheckTarget, scpb.State_PUBLIC},
							},
						},
					},
				},
			},
		})
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var prevTargetStates []scpb.TargetState
			for _, ci := range tc.compileIterations {
				if len(ci.initial) > 0 {
					prevTargetStates = prevTargetStates[:0]
					for _, ts := range ci.initial {
						prevTargetStates = append(prevTargetStates, ts)
					}
				}

				g, err := buildGraph(prevTargetStates, ci.flags)
				require.NoError(t, err)
				dg, err := g.drawStages()
				require.NoError(t, err)
				t.Log("\n" + dg.String())

				sc, err := Compile(prevTargetStates, ci.flags)
				require.NoError(t, err)
				stages := sc.Stages()
				// TODO (lucy): The ordering of ops in each state is currently
				// unspecified, so the comparison should be order-insensitive.
				require.Equal(t, ci.expected, stages)

				if len(stages) > 0 {
					lastTargetStates := stages[len(stages)-1].NextTargets
					prevTargetStates = prevTargetStates[:0]
					for i := range lastTargetStates {
						prevTargetStates = append(prevTargetStates, lastTargetStates[i])
					}
				}
			}
		})
	}
}

func TestDebugScratch(t *testing.T) {
	targetSlice := []scpb.Target{
		&scpb.AddIndex{
			TableID: 10,
			Index: descpb.IndexDescriptor{
				Name:             "primary 2",
				ID:               2,
				ColumnIDs:        []descpb.ColumnID{1},
				ColumnNames:      []string{"i"},
				ColumnDirections: []descpb.IndexDescriptor_Direction{descpb.IndexDescriptor_ASC},
				StoreColumnIDs:   []descpb.ColumnID{2},
				StoreColumnNames: []string{"j"},
				Unique:           true,
				Type:             descpb.IndexDescriptor_FORWARD,
			},
			PrimaryIndex:   1,
			ReplacementFor: 1,
		},
		&scpb.AddColumn{
			TableID:      10,
			ColumnFamily: descpb.FamilyID(1),
			Column: descpb.ColumnDescriptor{
				Name:           "j",
				ID:             2,
				Type:           types.Int,
				Nullable:       true,
				PGAttributeNum: 2,
			},
		},
		&scpb.DropIndex{
			TableID:    10,
			IndexID:    1,
			ReplacedBy: 2,
			ColumnIDs:  []descpb.ColumnID{1},
		},
	}

	targetStates := []scpb.TargetState{
		{
			Target: targetSlice[0],
			State:  scpb.State_DELETE_ONLY,
		},
		{
			Target: targetSlice[1],
			State:  scpb.State_DELETE_ONLY,
		},
		{
			Target: targetSlice[2],
			State:  scpb.State_PUBLIC,
		},
	}

	draw := func(t *testing.T, flag ExecutionPhase, f func(g *SchemaChange) (*dot.Graph, error)) {
		g, err := buildGraph(targetStates, CompileFlags{
			ExecutionPhase: flag,
		})
		require.NoError(t, err)
		d, err := f(g)
		require.NoError(t, err)
		t.Log("\n", d)
	}
	t.Run("deps", func(t *testing.T) {
		t.Run("PostStatement", func(t *testing.T) {
			draw(t, PostStatementPhase, (*SchemaChange).drawDeps)
		})
		t.Run("PreCommit", func(t *testing.T) {
			draw(t, PreCommitPhase, (*SchemaChange).drawDeps)
		})
		t.Run("PostCommit", func(t *testing.T) {
			draw(t, PostCommitPhase, (*SchemaChange).drawDeps)
		})
	})
	t.Run("stages", func(t *testing.T) {
		t.Run("PostStatement", func(t *testing.T) {
			draw(t, PostStatementPhase, (*SchemaChange).drawStages)
		})
		t.Run("PreCommit", func(t *testing.T) {
			draw(t, PreCommitPhase, (*SchemaChange).drawStages)
		})
		t.Run("PostCommit", func(t *testing.T) {
			draw(t, PostCommitPhase, (*SchemaChange).drawStages)
		})
	})

}
