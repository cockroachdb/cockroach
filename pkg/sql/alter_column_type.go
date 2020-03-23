// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachange"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

// AlterColumnType takes an AlterTableAlterColumnType, determines
// which conversion to use and applies the type conversion.
func AlterColumnType(
	tableDesc *sqlbase.MutableTableDescriptor,
	col *sqlbase.ColumnDescriptor,
	t *tree.AlterTableAlterColumnType,
) error {
	typ := t.ToType

	// Special handling for STRING COLLATE xy to verify that we recognize the language.
	if t.Collation != "" {
		if types.IsStringType(typ) {
			typ = types.MakeCollatedString(typ, t.Collation)
		} else {
			return pgerror.New(pgcode.Syntax, "COLLATE can only be used with string types")
		}
	}

	err := sqlbase.ValidateColumnDefType(typ)
	if err != nil {
		return err
	}

	// No-op if the types are Identical.  We don't use Equivalent here because
	// the user may be trying to change the type of the column without changing
	// the type family.
	if col.Type.Identical(typ) {
		return nil
	}

	kind, err := schemachange.ClassifyConversion(&col.Type, typ)
	if err != nil {
		return err
	}

	switch kind {
	case schemachange.ColumnConversionDangerous, schemachange.ColumnConversionImpossible:
		// We're not going to make it impossible for the user to perform
		// this conversion, but we do want them to explicit about
		// what they're going for.
		return pgerror.Newf(pgcode.CannotCoerce,
			"the requested type conversion (%s -> %s) requires an explicit USING expression",
			col.Type.SQLString(), typ.SQLString())
	case schemachange.ColumnConversionTrivial:
		col.Type = *typ
	case schemachange.ColumnConversionGeneral:
		currentMutationID := tableDesc.ClusterVersion.NextMutationID
		for i := range tableDesc.Mutations {
			mut := &tableDesc.Mutations[i]
			if mut.MutationID < currentMutationID {
				return unimplemented.NewWithIssuef(
					47137, "table %s is currently undergoing a schema change", tableDesc.Name)
			}
		}

		nameExists := func(name string) bool {
			_, _, err := tableDesc.FindColumnByName(tree.Name(name))
			return err == nil
		}

		shadowColName := sqlbase.GenerateUniqueConstraintName(
			col.Name,
			nameExists,
		)

		// Compute Expr.
		unresolvedName := func(name string) *tree.UnresolvedName {
			return &tree.UnresolvedName{
				NumParts: 1,
				Parts:    tree.NameParts{name},
			}
		}

		newComputedExpr := tree.CastExpr{Expr: unresolvedName(col.Name), Type: t.ToType, SyntaxMode: tree.CastShort}
		s := tree.Serialize(&newComputedExpr)
		newColComputeExpr := &s

		// May run into runtime error if DEFAULT value of column cannot be casted.
		// In this case right now, the schema change is not rolled back.
		hasDefault := col.HasDefault()
		var newColDefaultExpr *string
		if hasDefault {
			if col.HasNullDefault() {
				s := tree.Serialize(tree.DNull)
				newColDefaultExpr = &s
			} else {
				expr, err := parser.ParseExpr(col.DefaultExprStr())
				if err != nil {
					return err
				}
				newDefaultComputedExpr := tree.CastExpr{Expr: expr, Type: t.ToType, SyntaxMode: tree.CastShort}
				s := tree.Serialize(&newDefaultComputedExpr)
				newColDefaultExpr = &s
			}
		}

		id := tableDesc.NextColumnID
		tableDesc.NextColumnID++

		newCol := sqlbase.ColumnDescriptor{
			Name:            shadowColName,
			ID:              id,
			Type:            *t.ToType,
			Nullable:        col.Nullable,
			DefaultExpr:     newColDefaultExpr,
			UsesSequenceIds: col.UsesSequenceIds,
			OwnsSequenceIds: col.OwnsSequenceIds,
			ComputeExpr:     newColComputeExpr,
		}

		tableDesc.AddColumnMutation(&newCol, sqlbase.DescriptorMutation_ADD)

		swapArgs := &sqlbase.ComputedColumnSwap{
			OldColumnId: col.ID,
			NewColumnId: newCol.ID,
		}

		tableDesc.AddComputedColumnSwapMutation(swapArgs)
	default:
		return fmt.Errorf("unknown conversion for %s -> %s",
			col.Type.SQLString(), typ.SQLString())
	}

	return nil
}
