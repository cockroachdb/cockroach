// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
)

func qualifiedName(b BuildCtx, id catid.DescID) string {
	_, _, ns := scpb.FindNamespace(b.QueryByID(id))
	_, _, sc := scpb.FindNamespace(b.QueryByID(ns.SchemaID))
	_, _, db := scpb.FindNamespace(b.QueryByID(ns.DatabaseID))
	if db == nil {
		return ns.Name
	}
	if sc == nil {
		return db.Name + "." + ns.Name
	}
	return db.Name + "." + sc.Name + "." + ns.Name
}

func simpleName(b BuildCtx, id catid.DescID) string {
	_, _, ns := scpb.FindNamespace(b.QueryByID(id))
	return ns.Name
}

// dropRestrictDescriptor contains the common logic for dropping something with
// RESTRICT.
func dropRestrictDescriptor(b BuildCtx, id catid.DescID) (hasChanged bool) {
	b.QueryByID(id).ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if target == scpb.ToAbsent {
			return
		}
		b.CheckPrivilege(e, privilege.DROP)
		dropElement(b, e)
		hasChanged = true
	})
	return hasChanged
}

func dropElement(b BuildCtx, e scpb.Element) {
	// TODO(postamar): remove this dirty hack ASAP, see column/index dep rules.
	switch t := e.(type) {
	case *scpb.ColumnType:
		t.IsRelationBeingDropped = true
	case *scpb.SecondaryIndexPartial:
		t.IsRelationBeingDropped = true
	}
	b.Drop(e)
}

// dropCascadeDescriptor contains the common logic for dropping something with
// CASCADE.
func dropCascadeDescriptor(b BuildCtx, id catid.DescID) {
	undropped := b.QueryByID(id).Filter(func(_ scpb.Status, target scpb.TargetStatus, _ scpb.Element) bool {
		return target == scpb.ToPublic
	})
	// Exit early if all elements already have ABSENT targets.
	if undropped.IsEmpty() {
		return
	}
	// Check privileges and decide which actions to take or not.
	var isVirtualSchema bool
	undropped.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Database:
			break
		case *scpb.Schema:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary schema"))
			}
			isVirtualSchema = t.IsVirtual
			// Return early to skip checking privileges on schemas.
			return
		case *scpb.Table:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary table"))
			}
		case *scpb.Sequence:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary sequence"))
			}
		case *scpb.View:
			if t.IsTemporary {
				panic(scerrors.NotImplementedErrorf(nil, "dropping a temporary view"))
			}
		case *scpb.EnumType:
		case *scpb.AliasType:
			break
		default:
			return
		}
		b.CheckPrivilege(e, privilege.DROP)
	})
	// Mark element targets as ABSENT.
	next := b.WithNewSourceElementID()
	undropped.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		if isVirtualSchema {
			// Don't actually drop any elements of virtual schemas.
			return
		}
		dropElement(b, e)
		switch t := e.(type) {
		case *scpb.EnumType:
			dropCascadeDescriptor(next, t.ArrayTypeID)
		case *scpb.SequenceOwner:
			dropCascadeDescriptor(next, t.SequenceID)
		}
	})
	// Recurse on back-referenced elements.
	ub := undroppedBackrefs(b, id)
	ub.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.SchemaParent:
			dropCascadeDescriptor(next, t.SchemaID)
		case *scpb.ObjectParent:
			dropCascadeDescriptor(next, t.ObjectID)
		case *scpb.View:
			dropCascadeDescriptor(next, t.ViewID)
		case *scpb.Sequence:
			dropCascadeDescriptor(next, t.SequenceID)
		case *scpb.AliasType:
			dropCascadeDescriptor(next, t.TypeID)
		case *scpb.EnumType:
			dropCascadeDescriptor(next, t.TypeID)
		case *scpb.Column, *scpb.ColumnType, *scpb.SecondaryIndexPartial:
			// These only have type references.
			break
		case
			*scpb.ColumnDefaultExpression,
			*scpb.ColumnOnUpdateExpression,
			*scpb.CheckConstraint,
			*scpb.ForeignKeyConstraint,
			*scpb.SequenceOwner,
			*scpb.DatabaseRegionConfig:
			dropElement(b, e)
		}
	})
}

func undroppedBackrefs(b BuildCtx, id catid.DescID) ElementResultSet {
	return b.BackReferences(id).Filter(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) bool {
		return target != scpb.ToAbsent && screl.AllDescIDs(e).Contains(id)
	})
}

func descIDs(input ElementResultSet) (ids catalog.DescriptorIDSet) {
	input.ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		ids.Add(screl.GetDescID(e))
	})
	return ids
}
