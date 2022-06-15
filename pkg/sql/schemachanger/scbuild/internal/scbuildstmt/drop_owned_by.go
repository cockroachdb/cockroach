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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DropOwnedBy implements DROP OWNED BY.
func DropOwnedBy(b BuildCtx, n *tree.DropOwnedBy) {
	normalizedRoles, err := decodeusername.FromRoleSpecList(
		b.SessionData(), username.PurposeValidation, n.Roles,
	)
	if err != nil {
		panic(err)
	}
	for _, role := range normalizedRoles {
		if role.IsAdminRole() || role.IsRootUser() || role.IsNodeUser() {
			panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
				"cannot drop objects owned by role %q because they are required by the database system", role))
		}
		if role != b.SessionData().User() && !b.IsMemberOf(role) {
			panic(pgerror.New(pgcode.InsufficientPrivilege, "permission denied to drop objects"))
		}
	}

	var objects []descpb.ID
	var toCheckBackrefs []descpb.ID

	// Lookup all objects in the current database.
	_, _, db := scpb.FindDatabase(b.ResolveDatabase(tree.Name(b.SessionData().Database), ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CONNECT,
	}))
	dbRefs := undroppedBackrefs(b, db.DatabaseID)
	scpb.ForEachSchemaParent(dbRefs, func(_ scpb.Status, _ scpb.TargetStatus, sp *scpb.SchemaParent) {
		schemaRefs := undroppedBackrefs(b, sp.SchemaID)
		scpb.ForEachObjectParent(schemaRefs, func(_ scpb.Status, _ scpb.TargetStatus, op *scpb.ObjectParent) {
			objects = append(objects, op.ObjectID)
		})
		objects = append(objects, sp.SchemaID)
	})

	// Drop owned objects and revoke user privileges for the specified roles.
	for _, id := range objects {
		elts := b.QueryByID(id)
		_, _, owner := scpb.FindOwner(elts)
		for _, role := range normalizedRoles {
			if owner.Owner == role.Normalized() {
				if n.DropBehavior == tree.DropCascade {
					// TODO(jasonmchan): implement for #55908
					panic(scerrors.NotImplementedError(n))
				} else {
					if dropRestrictDescriptor(b, id) {
						toCheckBackrefs = append(toCheckBackrefs, id)
					}
				}
				break
			}
			scpb.ForEachUserPrivileges(elts, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.UserPrivileges) {
				if e.UserName == role.Normalized() {
					b.Drop(e)
				}
			})
		}
	}

	// Revoke privileges for the database. The current user shouldn't revoke
	// their own database privileges.
	dbElts := b.QueryByID(db.DatabaseID)
	scpb.ForEachUserPrivileges(dbElts, func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.UserPrivileges) {
		for _, role := range normalizedRoles {
			if e.UserName == role.Normalized() && e.UserName != b.SessionData().User().Normalized() {
				b.Drop(e)
				break
			}
		}
	})

	b.IncrementSubWorkID()
	b.IncrementDropOwnedByCounter()

	// Enforce RESTRICT semantics by checking for backreferences.
	for _, id := range toCheckBackrefs {
		backrefs := undroppedBackrefs(b, id)
		if !backrefs.IsEmpty() {
			panic(pgerror.New(pgcode.DependentObjectsStillExist,
				"cannot drop desired object(s) because other objects depend on them"))
		}
	}
}
