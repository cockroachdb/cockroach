// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

func alterTypeRename(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeRename,
) {
	newName := string(t.NewName)

	typeElts := b.QueryByID(enumType.TypeID)
	currentNS := typeElts.FilterNamespace().MustGetOneElement()

	if currentNS.Name == newName {
		return
	}

	checkTypeNameConflicts(b, newName, currentNS)

	newNS := &scpb.Namespace{
		DatabaseID:   currentNS.DatabaseID,
		SchemaID:     currentNS.SchemaID,
		DescriptorID: currentNS.DescriptorID,
		Name:         newName,
	}
	b.Drop(currentNS)
	b.Add(newNS)

	// Rename the implicit array type.
	arrayElts := b.QueryByID(enumType.ArrayTypeID)
	arrayNS := arrayElts.FilterNamespace().MustGetOneElement()

	newArrayName := findFreeNameInSchema(b, b.NamePrefix(currentNS), "_"+newName)
	newArrayNS := &scpb.Namespace{
		DatabaseID:   arrayNS.DatabaseID,
		SchemaID:     arrayNS.SchemaID,
		DescriptorID: arrayNS.DescriptorID,
		Name:         newArrayName,
	}
	b.Drop(arrayNS)
	b.Add(newArrayNS)

	newTN := tree.MakeTypeNameWithPrefix(tn.ObjectNamePrefix, newName)

	b.LogEventForExistingPayload(newNS, &eventpb.RenameType{
		TypeName:    tn.FQString(),
		NewTypeName: newTN.FQString(),
	})
}

// checkTypeNameConflicts checks if the new type name conflicts with an
// existing object in the same schema.
func checkTypeNameConflicts(b BuildCtx, newName string, currentNS *scpb.Namespace) {
	tn := tree.MakeTableNameFromPrefix(
		b.NamePrefix(currentNS),
		tree.Name(newName),
	)
	ers := b.ResolveRelation(tn.ToUnresolvedObjectName(),
		ResolveParams{
			IsExistenceOptional: true,
			WithOffline:         true,
			ResolveTypes:        true,
		})
	if ers == nil || ers.IsEmpty() {
		return
	}

	existingNS := ers.FilterNamespace().MustGetZeroOrOneElement()
	if existingNS == nil {
		return
	}

	if existingNS.DescriptorID == currentNS.DescriptorID {
		return
	}

	if !ers.FilterNamespace().ToAbsent().IsEmpty() {
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"object %q being dropped, try again later", newName))
	}

	if !ers.FilterCompositeType().IsEmpty() || !ers.FilterEnumType().IsEmpty() || !ers.FilterDomainType().IsEmpty() {
		panic(sqlerrors.NewTypeAlreadyExistsError(tn.String()))
	}
	panic(sqlerrors.NewRelationAlreadyExistsError(tn.String()))
}

// findFreeNameInSchema prepends underscores to candidateName until it doesn't
// collide with any existing object in the given schema.
func findFreeNameInSchema(b BuildCtx, prefix tree.ObjectNamePrefix, candidateName string) string {
	for {
		tn := tree.MakeTableNameFromPrefix(prefix, tree.Name(candidateName))
		ers := b.ResolveRelation(tn.ToUnresolvedObjectName(),
			ResolveParams{
				IsExistenceOptional: true,
				WithOffline:         true,
				ResolveTypes:        true,
			})
		if ers == nil || ers.IsEmpty() {
			return candidateName
		}
		candidateName = "_" + candidateName
	}
}
