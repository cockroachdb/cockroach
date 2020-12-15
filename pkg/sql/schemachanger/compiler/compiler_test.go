package compiler

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/ops"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/targets"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/emicklei/dot"
	"github.com/stretchr/testify/require"
)

func TestCompiler(t *testing.T) {
	for _, tc := range []struct {
		ts    []targets.TargetState
		flags CompileFlags
	}{
		{
			[]targets.TargetState{
				{
					&targets.DropColumn{
						TableID:  1,
						ColumnID: 2,
					},
					targets.State_PUBLIC,
				},
				{
					&targets.AddIndex{
						TableID: 1,
						Index: descpb.IndexDescriptor{
							ID:        2,
							ColumnIDs: []descpb.ColumnID{1},
						},
						PrimaryIndex:   1,
						ReplacementFor: 1,
						Primary:        true,
					},
					targets.State_ABSENT,
				},
				{
					&targets.DropIndex{
						TableID:    1,
						IndexID:    1,
						ReplacedBy: 2,
						ColumnIDs:  []descpb.ColumnID{1, 2},
					},
					targets.State_PUBLIC,
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
		initial  []targets.TargetState
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
		addColTarget := targets.AddColumn{
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
					initial: []targets.TargetState{
						{&addColTarget, targets.State_ABSENT},
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
							[]ops.Op{
								ops.AddColumnDescriptor{
									TableID: tableID,
									Column: descpb.ColumnDescriptor{
										ID: newColID,
									},
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_DELETE_ONLY},
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
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_DELETE_AND_WRITE_ONLY},
							},
						},
						{
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.State_PUBLIC,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_PUBLIC},
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
		addIdxTarget := targets.AddIndex{
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
					initial: []targets.TargetState{
						{&addIdxTarget, targets.State_ABSENT},
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
							[]ops.Op{
								ops.AddIndexDescriptor{
									TableID: tableID,
									Index: descpb.IndexDescriptor{
										ID:             newIdxID,
										ColumnIDs:      []descpb.ColumnID{2},
										ExtraColumnIDs: []descpb.ColumnID{1},
									},
								},
							},
							[]targets.TargetState{
								{&addIdxTarget, targets.State_DELETE_ONLY},
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
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newIdxID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addIdxTarget, targets.State_DELETE_AND_WRITE_ONLY},
							},
						},
						{
							[]ops.Op{
								ops.IndexBackfill{
									TableID: tableID,
									IndexID: newIdxID,
								},
							},
							[]targets.TargetState{
								{&addIdxTarget, targets.State_VALIDATED},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newIdxID,
									NextState: targets.State_PUBLIC,
								},
							},
							[]targets.TargetState{
								{&addIdxTarget, targets.State_PUBLIC},
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
		addColTarget := targets.AddColumn{
			TableID: tableID,
			Column: descpb.ColumnDescriptor{
				ID: newColID,
			},
		}
		addIdxTarget := targets.AddIndex{
			TableID: tableID,
			Index: descpb.IndexDescriptor{
				ID:        newPrimaryIdxID,
				ColumnIDs: []descpb.ColumnID{newColID},
				Unique:    true,
			},
			Primary:        true,
			ReplacementFor: oldPrimaryIdxID,
		}
		dropIdxTarget := targets.DropIndex{
			TableID:    tableID,
			IndexID:    oldPrimaryIdxID,
			ReplacedBy: newPrimaryIdxID,
		}

		testCases = append(testCases, testCase{
			name: "add column with default value",
			compileIterations: []compileIteration{
				{
					initial: []targets.TargetState{
						{&addColTarget, targets.State_ABSENT},
						{&addIdxTarget, targets.State_ABSENT},
						{&dropIdxTarget, targets.State_PUBLIC},
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
							[]ops.Op{
								ops.AddColumnDescriptor{
									TableID: tableID,
									Column: descpb.ColumnDescriptor{
										ID: newColID,
									},
								},
								ops.AddIndexDescriptor{
									TableID: tableID,
									Index: descpb.IndexDescriptor{
										ID:        newPrimaryIdxID,
										ColumnIDs: []descpb.ColumnID{newColID},
										Unique:    true,
									},
									Primary: true,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_DELETE_ONLY},
								{&addIdxTarget, targets.State_DELETE_ONLY},
								{&dropIdxTarget, targets.State_PUBLIC},
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
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addIdxTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&dropIdxTarget, targets.State_PUBLIC},
							},
						},
						{
							[]ops.Op{
								ops.IndexBackfill{
									TableID: tableID,
									IndexID: newPrimaryIdxID,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addIdxTarget, targets.State_BACKFILLED},
								{&dropIdxTarget, targets.State_PUBLIC},
							},
						},
						// The validation step isn't actually necessary because the new PK
						// has the same set of unique columns as the old PK. I think we can
						// eliminate it in general when adding/dropping columns.
						{
							[]ops.Op{
								ops.UniqueIndexValidation{
									TableID: tableID,
									IndexID: newPrimaryIdxID,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addIdxTarget, targets.State_VALIDATED},
								{&dropIdxTarget, targets.State_PUBLIC},
							},
						},
						{
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.State_PUBLIC,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: targets.State_PUBLIC,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_PUBLIC},
								{&addIdxTarget, targets.State_PUBLIC},
								{&dropIdxTarget, targets.State_DELETE_AND_WRITE_ONLY},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.State_DELETE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_PUBLIC},
								{&addIdxTarget, targets.State_PUBLIC},
								{&dropIdxTarget, targets.State_DELETE_ONLY},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.State_ABSENT,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.State_PUBLIC},
								{&addIdxTarget, targets.State_PUBLIC},
								{&dropIdxTarget, targets.State_ABSENT},
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
		dropColTarget := targets.AddColumn{
			TableID: tableID,
			Column: descpb.ColumnDescriptor{
				ID: newColID,
			},
		}
		addPrimaryIdxTargetStmt1 := targets.AddIndex{
			TableID: tableID,
			Index: descpb.IndexDescriptor{
				ID:     newPrimaryIdxID,
				Unique: true,
				// TODO: ColumnIDs....
			},
			ReplacementFor: oldPrimaryIdxID,
			Primary:        true,
		}
		dropPrimaryIdxTargetStmt1 := targets.DropIndex{
			TableID:    tableID,
			IndexID:    oldPrimaryIdxID,
			ReplacedBy: newPrimaryIdxID,
		}
		addPrimaryIdxTarget := targets.AddIndex{
			TableID: tableID,
			Index: descpb.IndexDescriptor{
				ID:     newPrimaryIdxID,
				Unique: true,
				// TODO: ColumnIDs....
			},
			ReplacementFor: oldPrimaryIdxID,
			Primary:        true,
		}
		dropPrimaryIdxTarget := targets.DropIndex{
			TableID:    tableID,
			IndexID:    oldPrimaryIdxID,
			ReplacedBy: newPrimaryIdxID,
		}
		addColTarget := targets.AddColumn{
			TableID: tableID,
			Column: descpb.ColumnDescriptor{
				ID: newColID,
			},
		}
		addUniqueIdxTarget := targets.DropIndex{
			TableID:   tableID,
			IndexID:   newUniqueIdxID,
			ColumnIDs: []descpb.ColumnID{newColID},
		}
		addUniqueConstraintTarget := targets.AddUniqueConstraint{
			TableID:   tableID,
			IndexID:   newUniqueIdxID,
			ColumnIDs: []descpb.ColumnID{newColID},
		}

		testCases = append(testCases, testCase{
			name: "drop column and add column with unique index and default value",
			compileIterations: []compileIteration{
				{
					initial: []targets.TargetState{
						{&addPrimaryIdxTargetStmt1, targets.State_ABSENT},
						{&dropPrimaryIdxTargetStmt1, targets.State_PUBLIC},
						{&dropColTarget, targets.State_PUBLIC},
					},
					flags: CompileFlags{
						ExecutionPhase: PostStatementPhase,
					},
					expected: []Stage{
						{
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  oldColID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTargetStmt1, targets.State_ABSENT},
								{&dropPrimaryIdxTargetStmt1, targets.State_PUBLIC},
								{&dropColTarget, targets.State_DELETE_AND_WRITE_ONLY},
							},
						},
					},
				},
				{
					initial: []targets.TargetState{
						{&addPrimaryIdxTarget, targets.State_ABSENT},
						{&dropPrimaryIdxTarget, targets.State_PUBLIC},
						{&dropColTarget, targets.State_DELETE_AND_WRITE_ONLY},
						{&addColTarget, targets.State_ABSENT},
						{&addUniqueIdxTarget, targets.State_ABSENT},
						{&addUniqueConstraintTarget, targets.State_ABSENT},
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
							[]ops.Op{
								ops.AddColumnDescriptor{
									TableID: tableID,
									Column: descpb.ColumnDescriptor{
										ID: newColID,
									},
								},
								ops.AddIndexDescriptor{
									TableID: tableID,
									Index: descpb.IndexDescriptor{
										ID:     newPrimaryIdxID,
										Unique: true,
										// TODO: ColumnIDs....
									},
									Primary: true,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.State_DELETE_ONLY},
								{&dropPrimaryIdxTarget, targets.State_PUBLIC},
								{&dropColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, targets.State_DELETE_ONLY},
								{&addUniqueIdxTarget, targets.State_ABSENT},
								{&addUniqueConstraintTarget, targets.State_ABSENT},
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
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&dropPrimaryIdxTarget, targets.State_PUBLIC},
								{&dropColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addUniqueIdxTarget, targets.State_ABSENT},
								{&addUniqueConstraintTarget, targets.State_ABSENT},
							},
						},
						{
							[]ops.Op{
								ops.IndexBackfill{
									TableID: tableID,
									IndexID: newPrimaryIdxID,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.State_BACKFILLED},
								{&dropPrimaryIdxTarget, targets.State_PUBLIC},
								{&dropColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addUniqueIdxTarget, targets.State_ABSENT},
								{&addUniqueConstraintTarget, targets.State_ABSENT},
							},
						},
						{
							[]ops.Op{
								// This also moves the unique constraint to State_DELETE_AND_WRITE_ONLY.
								ops.IndexBackfill{
									TableID: tableID,
									IndexID: newUniqueIdxID,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.State_BACKFILLED},
								{&dropPrimaryIdxTarget, targets.State_PUBLIC},
								{&dropColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addUniqueIdxTarget, targets.State_BACKFILLED},
								{&addUniqueConstraintTarget, targets.State_DELETE_AND_WRITE_ONLY},
							},
						},
						{
							[]ops.Op{
								ops.UniqueIndexValidation{
									TableID:        tableID,
									PrimaryIndexID: newPrimaryIdxID, // TODO: Do we want the new non-public index?
									IndexID:        newUniqueIdxID,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.State_BACKFILLED},
								{&dropPrimaryIdxTarget, targets.State_PUBLIC},
								{&dropColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addUniqueIdxTarget, targets.State_BACKFILLED},
								{&addUniqueConstraintTarget, targets.State_VALIDATED},
							},
						},
						{
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.State_PUBLIC,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: targets.State_PUBLIC,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.State_DELETE_AND_WRITE_ONLY,
								},
								// This also makes the unique constraint public. Eventually we
								// will need a way to update states for index-less unique
								// constraints.
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newUniqueIdxID,
									NextState: targets.State_PUBLIC,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.State_PUBLIC},
								{&dropPrimaryIdxTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&dropColTarget, targets.State_DELETE_AND_WRITE_ONLY},
								{&addColTarget, targets.State_PUBLIC},
								{&addUniqueIdxTarget, targets.State_PUBLIC},
								{&addUniqueConstraintTarget, targets.State_PUBLIC},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.State_DELETE_ONLY,
								},
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  oldColID,
									NextState: targets.State_DELETE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.State_PUBLIC},
								{&dropPrimaryIdxTarget, targets.State_DELETE_ONLY},
								{&dropColTarget, targets.State_DELETE_ONLY},
								{&addColTarget, targets.State_PUBLIC},
								{&addUniqueIdxTarget, targets.State_PUBLIC},
								{&addUniqueConstraintTarget, targets.State_PUBLIC},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.State_DELETE_ONLY,
								},
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  oldColID,
									NextState: targets.State_DELETE_ONLY,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.State_PUBLIC},
								{&dropPrimaryIdxTarget, targets.State_ABSENT},
								{&dropColTarget, targets.State_ABSENT},
								{&addColTarget, targets.State_PUBLIC},
								{&addUniqueIdxTarget, targets.State_PUBLIC},
								{&addUniqueConstraintTarget, targets.State_PUBLIC},
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
		addCheckTarget := targets.AddCheckConstraint{
			TableID:   tableID,
			Name:      checkName,
			Expr:      checkExpr,
			ColumnIDs: []descpb.ColumnID{columnID},
		}

		testCases = append(testCases, testCase{
			name: "add check constraint",
			compileIterations: []compileIteration{
				{
					initial: []targets.TargetState{
						{&addCheckTarget, targets.State_ABSENT},
					},
					flags: CompileFlags{
						ExecutionPhase: PostStatementPhase,
					},
					expected: []Stage{
						{
							[]ops.Op{
								ops.AddCheckConstraint{
									TableID:   tableID,
									Name:      checkName,
									Expr:      checkExpr,
									ColumnIDs: []descpb.ColumnID{columnID},
								},
							},
							[]targets.TargetState{
								{&addCheckTarget, targets.State_DELETE_AND_WRITE_ONLY},
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
					initial: []targets.TargetState{
						{&addCheckTarget, targets.State_DELETE_AND_WRITE_ONLY},
					},
					flags: CompileFlags{
						ExecutionPhase: PostCommitPhase,
					},
					expected: []Stage{
						{
							[]ops.Op{
								ops.ValidateCheckConstraint{
									TableID: tableID,
									Name:    checkName,
								},
							},
							[]targets.TargetState{
								{&addCheckTarget, targets.State_VALIDATED},
							},
						},
						{
							[]ops.Op{
								ops.CheckConstraintStateChange{
									TableID:   tableID,
									Name:      checkName,
									NextState: targets.State_PUBLIC,
								},
							},
							[]targets.TargetState{
								{&addCheckTarget, targets.State_PUBLIC},
							},
						},
					},
				},
			},
		})
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var prevTargetStates []targets.TargetState
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
	targetSlice := []targets.Target{
		&targets.AddIndex{
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
			Primary:        true,
		},
		&targets.AddColumn{
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
		&targets.DropIndex{
			TableID:    10,
			IndexID:    1,
			ReplacedBy: 2,
			ColumnIDs:  []descpb.ColumnID{1},
		},
	}

	targetStates := []targets.TargetState{
		{
			Target: targetSlice[0],
			State:  targets.State_DELETE_ONLY,
		},
		{
			Target: targetSlice[1],
			State:  targets.State_DELETE_ONLY,
		},
		{
			Target: targetSlice[2],
			State:  targets.State_PUBLIC,
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
