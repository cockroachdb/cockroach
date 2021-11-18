// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/lib/pq/oid"
)

func removeTypeBackRefDeps(b BuildCtx, table catalog.TableDescriptor) {
	// TODO(fqazi): Consider cleaning up refs using GetReferencedDescIDs(),
	// which would include all types of references inside a table descriptor.
	db := b.MustReadDatabase(table.GetParentID())
	typeIDs, _, err := table.GetAllReferencedTypeIDs(db, func(id descpb.ID) (catalog.TypeDescriptor, error) {
		return b.MustReadType(id), nil
	})
	onErrPanic(err)
	// Drop all references to this table/view/sequence
	for _, typeID := range typeIDs {
		b.EnqueueDropIfNotExists(&scpb.TypeReference{
			TypeID: typeID,
			DescID: table.GetID(),
		})
	}
}

// removeColumnTypeBackRefs removes type back references for a given table
// column from default expressions and comptued expressions.
func removeColumnTypeBackRefs(b BuildCtx, table catalog.TableDescriptor, id descpb.ColumnID) {
	visitor := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}
	visitorDeleted := &tree.TypeCollectorVisitor{
		OIDs: make(map[oid.Oid]struct{}),
	}
	// TODO(fqazi): Deal with the case where a column is added
	// in the current statement.

	// Get all available type references and create nodes
	// for dropping these type references.
	for _, col := range table.AllColumns() {
		if !col.HasDefault() || col.ColumnDesc().HasNullDefault() {
			continue
		}
		expr, err := parser.ParseExpr(col.GetDefaultExpr())
		onErrPanic(err)
		if col.GetID() == id {
			tree.WalkExpr(visitorDeleted, expr)
		} else {
			tree.WalkExpr(visitor, expr)
		}
		if col.IsComputed() {
			expr, err := parser.ParseExpr(col.GetComputeExpr())
			onErrPanic(err)
			if col.GetID() == id {
				tree.WalkExpr(visitorDeleted, expr)
			} else {
				tree.WalkExpr(visitor, expr)
			}
		}
	}
	// Remove OID that only exist in the deleted list.
	for oid := range visitorDeleted.OIDs {
		if _, ok := visitor.OIDs[oid]; !ok {
			typeID, err := typedesc.UserDefinedTypeOIDToID(oid)
			onErrPanic(err)
			b.EnqueueDropIfNotExists(&scpb.TypeReference{
				TypeID: typeID,
				DescID: table.GetID(),
			})
		}
	}
}
