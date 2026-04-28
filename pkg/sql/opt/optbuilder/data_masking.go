// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

// addDataMaskingProjections replaces column references with masking expressions
// for users who are not exempt from data masking. This is called after RLS
// filtering and virtual column projection in the scan-building path.
func (b *Builder) addDataMaskingProjections(tabMeta *opt.TableMeta, outScope *scope) {
	// Check if user is exempt from masking.
	if b.isExemptFromDataMasking(tabMeta) {
		return
	}

	tab := tabMeta.Table

	// Track which scope columns need masking projections.
	var maskedCols intsets.Fast
	for i := range outScope.cols {
		ord := outScope.cols[i].tableOrdinal
		col := tab.Column(ord)
		if col.HasMaskingExpr() {
			maskedCols.Add(i)
		}
	}
	if maskedCols.Empty() {
		return
	}

	// Build a table scope for resolving column references in masking
	// expressions. We lazily allocate it because most tables won't have masking
	// policies.
	var tableScope *scope

	// Build the masking projections. We construct a Project that replaces the
	// masked columns with the result of their masking expressions while passing
	// through all unmasked columns. Each masked column gets a NEW column ID
	// because the masking expression references the original column — projecting
	// back into the same ID would create a self-referencing synthesized column,
	// which the optimizer's functional dependency tracking rejects.
	proj := make(memo.ProjectionsExpr, 0, maskedCols.Len())
	var passthrough opt.ColSet
	for i := range outScope.cols {
		if !maskedCols.Contains(i) {
			passthrough.Add(outScope.cols[i].id)
			continue
		}

		ord := outScope.cols[i].tableOrdinal
		col := tab.Column(ord)
		maskExprStr := col.MaskingExprStr()

		parsedExpr, err := parser.ParseExpr(maskExprStr)
		if err != nil {
			panic(err)
		}

		if tableScope == nil {
			tableScope = b.allocScope()
			tableScope.expr = outScope.expr
			b.appendOrdinaryColumnsFromTable(tableScope, tabMeta, &tabMeta.Alias)
		}

		colType := col.DatumType()
		texpr := tableScope.resolveAndRequireType(parsedExpr, colType)
		if texpr == nil {
			// If the masking expression doesn't type-check to the column type,
			// try resolving as AnyElement and let the optimizer handle type
			// coercion.
			texpr = tableScope.resolveType(parsedExpr, types.AnyElement)
		}

		scalar := b.buildScalar(texpr, tableScope, nil, nil, nil)

		// Allocate a new column ID for the masked result. The masking expression
		// references the original column ID, so we cannot reuse it.
		newColID := b.factory.Metadata().AddColumn(
			outScope.cols[i].name.MetadataName(), colType,
		)
		proj = append(proj, b.factory.ConstructProjectionsItem(scalar, newColID))

		// Update the scope column to reference the new masked column ID. The
		// rest of the query plan will see this ID instead of the original.
		outScope.cols[i].id = newColID
	}

	outScope.expr = b.factory.ConstructProject(outScope.expr, proj, passthrough)
}

// isExemptFromDataMasking checks if the current user is exempt from dynamic
// data masking. Admins, table owners, and users with the UNMASK privilege see
// raw data.
func (b *Builder) isExemptFromDataMasking(tabMeta *opt.TableMeta) bool {
	isAdmin, err := b.catalog.UserHasAdminRole(b.ctx, b.checkPrivilegeUser())
	if err != nil {
		panic(err)
	}
	if isAdmin {
		return true
	}

	// Check if the user is the table owner.
	isOwner, err := b.catalog.IsOwner(b.ctx, tabMeta.Table, b.checkPrivilegeUser())
	if err != nil {
		panic(err)
	}
	if isOwner {
		return true
	}

	// Check if the user has the UNMASK privilege.
	hasUnmask, err := b.catalog.UserHasGlobalPrivilegeOrRoleOption(
		b.ctx, privilege.UNMASK, b.checkPrivilegeUser(),
	)
	if err != nil {
		panic(err)
	}
	return hasUnmask
}
