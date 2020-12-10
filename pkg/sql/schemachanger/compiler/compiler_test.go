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
					targets.StatePublic,
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
						{&addColTarget, targets.StateAbsent},
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
								{&addColTarget, targets.StateDeleteOnly},
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
									NextState: targets.StateDeleteAndWriteOnly,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.StateDeleteAndWriteOnly},
							},
						},
						{
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.StatePublic,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.StatePublic},
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
						{&addIdxTarget, targets.StateAbsent},
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
								{&addIdxTarget, targets.StateDeleteOnly},
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
									NextState: targets.StateDeleteAndWriteOnly,
								},
							},
							[]targets.TargetState{
								{&addIdxTarget, targets.StateDeleteAndWriteOnly},
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
								{&addIdxTarget, targets.StateValidated},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newIdxID,
									NextState: targets.StatePublic,
								},
							},
							[]targets.TargetState{
								{&addIdxTarget, targets.StatePublic},
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
						{&addColTarget, targets.StateAbsent},
						{&addIdxTarget, targets.StateAbsent},
						{&dropIdxTarget, targets.StatePublic},
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
								{&addColTarget, targets.StateDeleteOnly},
								{&addIdxTarget, targets.StateDeleteOnly},
								{&dropIdxTarget, targets.StatePublic},
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
									NextState: targets.StateDeleteAndWriteOnly,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: targets.StateDeleteAndWriteOnly,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.StateDeleteAndWriteOnly},
								{&addIdxTarget, targets.StateDeleteAndWriteOnly},
								{&dropIdxTarget, targets.StatePublic},
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
								{&addColTarget, targets.StateDeleteAndWriteOnly},
								{&addIdxTarget, targets.StateBackfilled},
								{&dropIdxTarget, targets.StatePublic},
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
								{&addColTarget, targets.StateDeleteAndWriteOnly},
								{&addIdxTarget, targets.StateValidated},
								{&dropIdxTarget, targets.StatePublic},
							},
						},
						{
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.StatePublic,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: targets.StatePublic,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.StateDeleteAndWriteOnly,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.StatePublic},
								{&addIdxTarget, targets.StatePublic},
								{&dropIdxTarget, targets.StateDeleteAndWriteOnly},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.StateDeleteOnly,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.StatePublic},
								{&addIdxTarget, targets.StatePublic},
								{&dropIdxTarget, targets.StateDeleteOnly},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.StateAbsent,
								},
							},
							[]targets.TargetState{
								{&addColTarget, targets.StatePublic},
								{&addIdxTarget, targets.StatePublic},
								{&dropIdxTarget, targets.StateAbsent},
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
						{&addPrimaryIdxTargetStmt1, targets.StateAbsent},
						{&dropPrimaryIdxTargetStmt1, targets.StatePublic},
						{&dropColTarget, targets.StatePublic},
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
									NextState: targets.StateDeleteAndWriteOnly,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTargetStmt1, targets.StateAbsent},
								{&dropPrimaryIdxTargetStmt1, targets.StatePublic},
								{&dropColTarget, targets.StateDeleteAndWriteOnly},
							},
						},
					},
				},
				{
					initial: []targets.TargetState{
						{&addPrimaryIdxTarget, targets.StateAbsent},
						{&dropPrimaryIdxTarget, targets.StatePublic},
						{&dropColTarget, targets.StateDeleteAndWriteOnly},
						{&addColTarget, targets.StateAbsent},
						{&addUniqueIdxTarget, targets.StateAbsent},
						{&addUniqueConstraintTarget, targets.StateAbsent},
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
								{&addPrimaryIdxTarget, targets.StateDeleteOnly},
								{&dropPrimaryIdxTarget, targets.StatePublic},
								{&dropColTarget, targets.StateDeleteAndWriteOnly},
								{&addColTarget, targets.StateDeleteOnly},
								{&addUniqueIdxTarget, targets.StateAbsent},
								{&addUniqueConstraintTarget, targets.StateAbsent},
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
									NextState: targets.StateDeleteAndWriteOnly,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: targets.StateDeleteAndWriteOnly,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.StateDeleteAndWriteOnly},
								{&dropPrimaryIdxTarget, targets.StatePublic},
								{&dropColTarget, targets.StateDeleteAndWriteOnly},
								{&addColTarget, targets.StateDeleteAndWriteOnly},
								{&addUniqueIdxTarget, targets.StateAbsent},
								{&addUniqueConstraintTarget, targets.StateAbsent},
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
								{&addPrimaryIdxTarget, targets.StateBackfilled},
								{&dropPrimaryIdxTarget, targets.StatePublic},
								{&dropColTarget, targets.StateDeleteAndWriteOnly},
								{&addColTarget, targets.StateDeleteAndWriteOnly},
								{&addUniqueIdxTarget, targets.StateAbsent},
								{&addUniqueConstraintTarget, targets.StateAbsent},
							},
						},
						{
							[]ops.Op{
								// This also moves the unique constraint to StateDeleteAndWriteOnly.
								ops.IndexBackfill{
									TableID: tableID,
									IndexID: newUniqueIdxID,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.StateBackfilled},
								{&dropPrimaryIdxTarget, targets.StatePublic},
								{&dropColTarget, targets.StateDeleteAndWriteOnly},
								{&addColTarget, targets.StateDeleteAndWriteOnly},
								{&addUniqueIdxTarget, targets.StateBackfilled},
								{&addUniqueConstraintTarget, targets.StateDeleteAndWriteOnly},
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
								{&addPrimaryIdxTarget, targets.StateBackfilled},
								{&dropPrimaryIdxTarget, targets.StatePublic},
								{&dropColTarget, targets.StateDeleteAndWriteOnly},
								{&addColTarget, targets.StateDeleteAndWriteOnly},
								{&addUniqueIdxTarget, targets.StateBackfilled},
								{&addUniqueConstraintTarget, targets.StateValidated},
							},
						},
						{
							[]ops.Op{
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  newColID,
									NextState: targets.StatePublic,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newPrimaryIdxID,
									NextState: targets.StatePublic,
								},
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.StateDeleteAndWriteOnly,
								},
								// This also makes the unique constraint public. Eventually we
								// will need a way to update states for index-less unique
								// constraints.
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   newUniqueIdxID,
									NextState: targets.StatePublic,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.StatePublic},
								{&dropPrimaryIdxTarget, targets.StateDeleteAndWriteOnly},
								{&dropColTarget, targets.StateDeleteAndWriteOnly},
								{&addColTarget, targets.StatePublic},
								{&addUniqueIdxTarget, targets.StatePublic},
								{&addUniqueConstraintTarget, targets.StatePublic},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.StateDeleteOnly,
								},
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  oldColID,
									NextState: targets.StateDeleteOnly,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.StatePublic},
								{&dropPrimaryIdxTarget, targets.StateDeleteOnly},
								{&dropColTarget, targets.StateDeleteOnly},
								{&addColTarget, targets.StatePublic},
								{&addUniqueIdxTarget, targets.StatePublic},
								{&addUniqueConstraintTarget, targets.StatePublic},
							},
						},
						{
							[]ops.Op{
								ops.IndexDescriptorStateChange{
									TableID:   tableID,
									IndexID:   oldPrimaryIdxID,
									NextState: targets.StateDeleteOnly,
								},
								ops.ColumnDescriptorStateChange{
									TableID:   tableID,
									ColumnID:  oldColID,
									NextState: targets.StateDeleteOnly,
								},
							},
							[]targets.TargetState{
								{&addPrimaryIdxTarget, targets.StatePublic},
								{&dropPrimaryIdxTarget, targets.StateAbsent},
								{&dropColTarget, targets.StateAbsent},
								{&addColTarget, targets.StatePublic},
								{&addUniqueIdxTarget, targets.StatePublic},
								{&addUniqueConstraintTarget, targets.StatePublic},
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
	// constraint in StateDeleteAndWriteOnly during statement execution. If those
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
						{&addCheckTarget, targets.StateAbsent},
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
								{&addCheckTarget, targets.StateDeleteAndWriteOnly},
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
						{&addCheckTarget, targets.StateDeleteAndWriteOnly},
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
								{&addCheckTarget, targets.StateValidated},
							},
						},
						{
							[]ops.Op{
								ops.CheckConstraintStateChange{
									TableID:   tableID,
									Name:      checkName,
									NextState: targets.StatePublic,
								},
							},
							[]targets.TargetState{
								{&addCheckTarget, targets.StatePublic},
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

				stages, err := Compile(prevTargetStates, ci.flags)
				require.NoError(t, err)
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
			State:  targets.StateDeleteOnly,
		},
		{
			Target: targetSlice[1],
			State:  targets.StateDeleteOnly,
		},
		{
			Target: targetSlice[2],
			State:  targets.StatePublic,
		},
	}

	draw := func(t *testing.T, flag ExecutionPhase, f func(g *targetStateGraph) (*dot.Graph, error)) {
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
			draw(t, PostStatementPhase, (*targetStateGraph).drawDeps)
		})
		t.Run("PreCommit", func(t *testing.T) {
			draw(t, PreCommitPhase, (*targetStateGraph).drawDeps)
		})
		t.Run("PostCommit", func(t *testing.T) {
			draw(t, PostCommitPhase, (*targetStateGraph).drawDeps)
		})
	})
	t.Run("stages", func(t *testing.T) {
		t.Run("PostStatement", func(t *testing.T) {
			draw(t, PostStatementPhase, (*targetStateGraph).drawStages)
		})
		t.Run("PreCommit", func(t *testing.T) {
			draw(t, PreCommitPhase, (*targetStateGraph).drawStages)
		})
		t.Run("PostCommit", func(t *testing.T) {
			draw(t, PostCommitPhase, (*targetStateGraph).drawStages)
		})
	})

}
