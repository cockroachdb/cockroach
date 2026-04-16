// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tabledesc_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descutil"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parserutils"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/plpgsqltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/require"
)

// randomTableDesc builds a table descriptor with randomly generated IDs
// and a random number of FKs, dependencies, columns, triggers, etc.
//
// N.B. We are mostly just trying to validate that our Rewrite()
// implementation visits every ID properly, i.e. it's okay to construct
// nonsensical table descriptors.
func randomTableDesc(rng *rand.Rand) *descpb.TableDescriptor {
	randID := func() descpb.ID { return descpb.ID(rng.Intn(200) + 50) }
	randN := func(max int) int { return rng.Intn(max + 1) }
	randTypeOID := func() oid.Oid { return catid.TypeIDToOID(randID()) }
	randTypeCastExpr := func() catpb.Expression {
		return catpb.Expression(fmt.Sprintf("NULL:::@%d", randTypeOID()))
	}

	tableID := randID()
	desc := &descpb.TableDescriptor{
		ID:                      tableID,
		Name:                    "test_table",
		ParentID:                randID(),
		UnexposedParentSchemaID: randID(),
		Version:                 5,
	}

	var constraintID descpb.ConstraintID
	nextConstraintID := func() descpb.ConstraintID {
		constraintID++
		return constraintID
	}

	for j := 0; j < randN(3); j++ {
		desc.OutboundFKs = append(desc.OutboundFKs, descpb.ForeignKeyConstraint{
			Name:              fmt.Sprintf("fk_out_%d", j),
			OriginTableID:     randID(),
			ReferencedTableID: randID(),
			ConstraintID:      nextConstraintID(),
		})
	}
	for j := 0; j < randN(3); j++ {
		desc.InboundFKs = append(desc.InboundFKs, descpb.ForeignKeyConstraint{
			Name:              fmt.Sprintf("fk_in_%d", j),
			OriginTableID:     randID(),
			ReferencedTableID: randID(),
			ConstraintID:      nextConstraintID(),
		})
	}
	for j := 0; j < randN(3); j++ {
		desc.DependsOn = append(desc.DependsOn, randID())
	}
	for j := 0; j < randN(3); j++ {
		desc.DependsOnTypes = append(desc.DependsOnTypes, randID())
	}
	for j := 0; j < randN(3); j++ {
		desc.DependsOnFunctions = append(desc.DependsOnFunctions, randID())
	}
	for j := 0; j < randN(3); j++ {
		desc.DependedOnBy = append(desc.DependedOnBy, descpb.TableDescriptor_Reference{
			ID: randID(),
		})
	}

	randCol := func(id descpb.ColumnID, name string) descpb.ColumnDescriptor {
		col := descpb.ColumnDescriptor{
			ID:   id,
			Name: name,
			Type: types.Int,
		}
		if rng.Intn(2) == 0 {
			col.Type = types.MakeEnum(randTypeOID(), randTypeOID())
		}
		if rng.Intn(2) == 0 {
			expr := randTypeCastExpr()
			col.DefaultExpr = &expr
		}
		if rng.Intn(2) == 0 {
			expr := randTypeCastExpr()
			col.ComputeExpr = &expr
		}
		if rng.Intn(2) == 0 {
			expr := randTypeCastExpr()
			col.OnUpdateExpr = &expr
		}
		for k := 0; k < randN(2); k++ {
			col.UsesSequenceIds = append(col.UsesSequenceIds, randID())
		}
		for k := 0; k < randN(2); k++ {
			col.OwnsSequenceIds = append(col.OwnsSequenceIds, randID())
		}
		for k := 0; k < randN(2); k++ {
			col.UsesFunctionIds = append(col.UsesFunctionIds, randID())
		}
		return col
	}

	for j := 0; j < 1+randN(2); j++ {
		desc.Columns = append(desc.Columns,
			randCol(descpb.ColumnID(j+1), fmt.Sprintf("col_%d", j)))
	}

	// Secondary indexes with partial index predicates.
	for j := 0; j < randN(2); j++ {
		desc.Indexes = append(desc.Indexes, descpb.IndexDescriptor{
			ID:        descpb.IndexID(j + 2),
			Name:      fmt.Sprintf("idx_%d", j),
			Predicate: randTypeCastExpr() + " IS NOT NULL",
		})
	}

	// View query with type OID references.
	desc.ViewQuery = descpb.Statement(fmt.Sprintf("SELECT %s", randTypeCastExpr()))

	// Check constraints with type OID references in expressions.
	for j := 0; j < randN(3); j++ {
		desc.Checks = append(desc.Checks, &descpb.TableDescriptor_CheckConstraint{
			Name:         fmt.Sprintf("ck_%d", j),
			Expr:         randTypeCastExpr() + " IS NOT NULL",
			ConstraintID: nextConstraintID(),
		})
	}

	for j := 0; j < randN(3); j++ {
		desc.UniqueWithoutIndexConstraints = append(desc.UniqueWithoutIndexConstraints,
			descpb.UniqueWithoutIndexConstraint{
				TableID:      randID(),
				ConstraintID: nextConstraintID(),
				Predicate:    randTypeCastExpr() + " IS NOT NULL",
			})
	}

	// Mutations: FK constraints, columns, and UWI constraints.
	for j := 0; j < randN(2); j++ {
		desc.Mutations = append(desc.Mutations, descpb.DescriptorMutation{
			Descriptor_: &descpb.DescriptorMutation_Constraint{
				Constraint: &descpb.ConstraintToUpdate{
					ConstraintType: descpb.ConstraintToUpdate_FOREIGN_KEY,
					ForeignKey: descpb.ForeignKeyConstraint{
						Name:              fmt.Sprintf("fk_mut_%d", j),
						OriginTableID:     tableID,
						ReferencedTableID: randID(),
						ConstraintID:      nextConstraintID(),
					},
				},
			},
		})
	}
	for j := 0; j < randN(2); j++ {
		mutCol := randCol(descpb.ColumnID(100+j), fmt.Sprintf("mut_col_%d", j))
		desc.Mutations = append(desc.Mutations, descpb.DescriptorMutation{
			Descriptor_: &descpb.DescriptorMutation_Column{Column: &mutCol},
		})
	}
	for j := 0; j < randN(2); j++ {
		desc.Mutations = append(desc.Mutations, descpb.DescriptorMutation{
			Descriptor_: &descpb.DescriptorMutation_Constraint{
				Constraint: &descpb.ConstraintToUpdate{
					ConstraintType: descpb.ConstraintToUpdate_UNIQUE_WITHOUT_INDEX,
					UniqueWithoutIndexConstraint: descpb.UniqueWithoutIndexConstraint{
						TableID:      randID(),
						ConstraintID: nextConstraintID(),
					},
				},
			},
		})
	}

	for j := 0; j < randN(3); j++ {
		trig := descpb.TriggerDescriptor{
			ID:       descpb.TriggerID(j + 1),
			Name:     fmt.Sprintf("trg_%d", j),
			FuncID:   randID(),
			FuncBody: descpb.RoutineBody(fmt.Sprintf("BEGIN\nRETURN %s;\nEND;", randTypeCastExpr())),
			WhenExpr: randTypeCastExpr() + " IS NOT NULL",
		}
		for k := 0; k < randN(2); k++ {
			trig.DependsOn = append(trig.DependsOn, randID())
		}
		for k := 0; k < randN(2); k++ {
			trig.DependsOnTypes = append(trig.DependsOnTypes, randID())
		}
		for k := 0; k < randN(2); k++ {
			trig.DependsOnRoutines = append(trig.DependsOnRoutines, randID())
		}
		desc.Triggers = append(desc.Triggers, trig)
	}

	for j := 0; j < randN(3); j++ {
		pol := descpb.PolicyDescriptor{
			ID:            descpb.PolicyID(j + 1),
			Name:          fmt.Sprintf("pol_%d", j),
			UsingExpr:     randTypeCastExpr() + " IS NOT NULL",
			WithCheckExpr: randTypeCastExpr() + " IS NOT NULL",
		}
		for k := 0; k < randN(2); k++ {
			pol.DependsOnFunctions = append(pol.DependsOnFunctions, randID())
		}
		for k := 0; k < randN(2); k++ {
			pol.DependsOnTypes = append(pol.DependsOnTypes, randID())
		}
		for k := 0; k < randN(2); k++ {
			pol.DependsOnRelations = append(pol.DependsOnRelations, randID())
		}
		desc.Policies = append(desc.Policies, pol)
	}

	desc.SequenceOpts = &descpb.TableDescriptor_SequenceOpts{
		Increment: 1,
		SequenceOwner: descpb.TableDescriptor_SequenceOpts_SequenceOwner{
			OwnerTableID:  randID(),
			OwnerColumnID: descpb.ColumnID(1),
		},
	}

	desc.DeclarativeSchemaChangerState = &scpb.DescriptorState{
		Targets: []scpb.Target{
			{
				ElementProto: scpb.ElementProto{
					ElementOneOf: &scpb.ElementProto_Column{
						Column: &scpb.Column{
							TableID:  randID(),
							ColumnID: descpb.ColumnID(rng.Intn(10) + 1),
						},
					},
				},
				TargetStatus: scpb.Status_PUBLIC,
			},
		},
		CurrentStatuses: []scpb.Status{scpb.Status_ABSENT},
		TargetRanks:     []uint32{0},
	}

	return desc
}

// getAllIDs returns all descriptor IDs referenced by the table descriptor.
// Note that since this is using the same reflection based walk as the Rewrite
// implementation, it's possible that we extend descriptors in the future with
// a _new_ type that is not properly handled in both walks. This is a testing
// gap, but ideally we should add more targeted tests that would catch this
// higher up in the LDR/restore/etc. flow for whatever new field we are adding.
func getAllIDs(t *testing.T, desc *descpb.TableDescriptor) catalog.DescriptorIDSet {
	t.Helper()
	var ids catalog.DescriptorIDSet

	require.NoError(t, descutil.WalkDescIDs(desc, func(id *catid.DescID) error {
		ids.Add(*id)
		return nil
	}))

	require.NoError(t, descutil.WalkTypes(desc, func(typ *types.T) error {
		if !typ.UserDefined() {
			return nil
		}
		ids.Add(catid.UserDefinedOIDToID(typ.Oid()))
		ids.Add(catid.UserDefinedOIDToID(typ.UserDefinedArrayOID()))
		if typ.Family() == types.ArrayFamily {
			ids.Add(catid.UserDefinedOIDToID(typ.ArrayContents().Oid()))
			ids.Add(catid.UserDefinedOIDToID(typ.ArrayContents().UserDefinedArrayOID()))
		}
		return nil
	}))

	visitor := &tree.TypeCollectorVisitor{OIDs: make(map[oid.Oid]struct{})}
	require.NoError(t, descutil.WalkExpressions(desc, func(expr *catpb.Expression) error {
		if *expr == "" {
			return nil
		}
		parsed, err := parserutils.ParseExpr(string(*expr))
		require.NoError(t, err)
		tree.WalkExpr(visitor, parsed)
		return nil
	}))
	require.NoError(t, descutil.WalkStatements(desc, func(stmt *catpb.Statement) error {
		if *stmt == "" {
			return nil
		}
		parsed, err := parserutils.ParseOne(string(*stmt))
		require.NoError(t, err)
		tree.WalkStmt(visitor, parsed.AST)
		return nil
	}))
	require.NoError(t, descutil.WalkRoutineBodies(desc, func(body *catpb.RoutineBody) error {
		if *body == "" {
			return nil
		}
		parsed, err := parserutils.PLpgSQLParse(string(*body))
		require.NoError(t, err)
		v := plpgsqltree.SQLStmtVisitor{Visitor: visitor}
		plpgsqltree.Walk(&v, parsed.AST)
		return nil
	}))
	for o := range visitor.OIDs {
		if !types.IsOIDUserDefinedType(o) {
			continue
		}
		ids.Add(catid.UserDefinedOIDToID(o))
	}

	return ids
}

// TestRewrite builds a table descriptor with random IDs populated and
// picks a random subset of them to rewrite. This is a randomized test
// because we want to test Rewrite handling cases where one ID is used
// in multiple places, e.g. FK IDs. Running multiple iterations lets us
// stress this without needing a descriptor generator that is smart enough
// to know how to construct valid descriptors or hardcoding something
// reasonable.
func TestRewrite(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const iterations = 100
	rng, _ := randutil.NewPseudoRand()

	for i := 0; i < iterations; i++ {
		desc := randomTableDesc(rng)
		mut := tabledesc.NewBuilder(desc).BuildCreatedMutableTable()

		origIDs := getAllIDs(t, mut.TableDesc())

		idMap := make(map[descpb.ID]descpb.ID)
		origIDs.ForEach(func(id descpb.ID) {
			if rng.Intn(2) == 0 {
				idMap[id] = descpb.ID(rng.Intn(200) + 300)
			}
		})

		rewriter := func(id descpb.ID) (descpb.ID, error) {
			if newID, ok := idMap[id]; ok {
				return newID, nil
			}
			return id, nil
		}

		require.NoError(t, mut.Rewrite(rewriter))

		var expectedIDs catalog.DescriptorIDSet
		origIDs.ForEach(func(id descpb.ID) {
			newID, _ := rewriter(id)
			expectedIDs.Add(newID)
		})

		actualIDs := getAllIDs(t, mut.TableDesc())

		require.Equal(t, expectedIDs, actualIDs)
	}
}
