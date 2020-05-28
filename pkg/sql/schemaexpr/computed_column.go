// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/errors"
)

// ComputedColumnValidator validates that an expression is a valid computed
// column. See Validate for more details.
type ComputedColumnValidator struct {
	desc    *sqlbase.MutableTableDescriptor
	semaCtx *tree.SemaContext
}

// NewComputedColumnValidator returns an ComputedColumnValidator struct that can
// be used to validate computed columns. See Validate for more details.
func NewComputedColumnValidator(
	desc *sqlbase.MutableTableDescriptor, semaCtx *tree.SemaContext,
) ComputedColumnValidator {
	return ComputedColumnValidator{
		desc:    desc,
		semaCtx: semaCtx,
	}
}

// Validate verifies that an expression is a valid computed column expression.
// If it is not valid, an error is returned.
//
// A computed column expression is valid if all of the following are true:
//
//   - It does not have a default value.
//   - It does not reference other computed columns.
//
func (v *ComputedColumnValidator) Validate(d *tree.ColumnTableDef) error {
	if d.HasDefaultExpr() {
		return pgerror.New(
			pgcode.InvalidTableDefinition,
			"computed columns cannot have default values",
		)
	}

	// TODO(mgartner): Use util.FastIntSet here instead.
	dependencies := make(map[sqlbase.ColumnID]struct{})
	// First, check that no column in the expression is a computed column.
	err := iterColDescriptors(v.desc, d.Computed.Expr, func(c *sqlbase.ColumnDescriptor) error {
		if c.IsComputed() {
			return pgerror.New(pgcode.InvalidTableDefinition,
				"computed columns cannot reference other computed columns")
		}
		dependencies[c.ID] = struct{}{}

		return nil
	})
	if err != nil {
		return err
	}

	// TODO(justin,bram): allow depending on columns like this. We disallow it
	// for now because cascading changes must hook into the computed column
	// update path.
	for i := range v.desc.OutboundFKs {
		fk := &v.desc.OutboundFKs[i]
		for _, id := range fk.OriginColumnIDs {
			if _, ok := dependencies[id]; !ok {
				// We don't depend on this column.
				continue
			}
			for _, action := range []sqlbase.ForeignKeyReference_Action{
				fk.OnDelete,
				fk.OnUpdate,
			} {
				switch action {
				case sqlbase.ForeignKeyReference_CASCADE,
					sqlbase.ForeignKeyReference_SET_NULL,
					sqlbase.ForeignKeyReference_SET_DEFAULT:
					return pgerror.New(pgcode.InvalidTableDefinition,
						"computed columns cannot reference non-restricted FK columns")
				}
			}
		}
	}

	// Replace the column variables with dummyColumns so that they can be
	// type-checked.
	replacedExpr, _, err := replaceVars(v.desc, d.Computed.Expr)
	if err != nil {
		return err
	}

	// Resolve the type of the computed column expression.
	defType, err := tree.ResolveType(d.Type, v.semaCtx.GetTypeResolver())
	if err != nil {
		return err
	}

	// Check that the type of the expression is of type defType and that there
	// are no variable expressions (besides dummyColumnItems) and no impure
	// functions.
	_, err = sqlbase.SanitizeVarFreeExpr(
		replacedExpr,
		defType,
		"computed column",
		v.semaCtx,
		false /* allowImpure */)
	if err != nil {
		return err
	}

	return nil
}

// ValidateNoDependents verifies that the input column is not dependent on a
// computed column. The function errs if any existing computed columns or
// computed columns being added reference the given column.
func (v *ComputedColumnValidator) ValidateNoDependents(col *sqlbase.ColumnDescriptor) error {
	checkComputed := func(c *sqlbase.ColumnDescriptor) error {
		if !c.IsComputed() {
			return nil
		}

		expr, err := parser.ParseExpr(*c.ComputeExpr)
		if err != nil {
			// At this point, we should be able to parse the computed expression.
			return errors.WithAssertionFailure(err)
		}

		return iterColDescriptors(v.desc, expr, func(colVar *sqlbase.ColumnDescriptor) error {
			if colVar.ID == col.ID {
				return pgerror.Newf(
					pgcode.InvalidColumnReference,
					"column %q is referenced by computed column %q",
					col.Name,
					c.Name,
				)
			}
			return nil
		})
	}

	for i := range v.desc.Columns {
		if err := checkComputed(&v.desc.Columns[i]); err != nil {
			return err
		}
	}

	for i := range v.desc.Mutations {
		mut := &v.desc.Mutations[i]
		mutCol := mut.GetColumn()
		if mut.Direction == sqlbase.DescriptorMutation_ADD && mutCol != nil {
			if err := checkComputed(mutCol); err != nil {
				return err
			}
		}
	}

	return nil
}
