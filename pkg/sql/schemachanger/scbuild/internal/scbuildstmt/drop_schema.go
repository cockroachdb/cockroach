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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DropSchema implements DROP SCHEMA.
func DropSchema(b BuildCtx, n *tree.DropSchema) {
	for _, name := range n.Names {
		elts := b.ResolveSchema(name, ResolveParams{
			IsExistenceOptional: n.IfExists,
			RequiredPrivilege:   privilege.DROP,
		})
		_, _, sc := scpb.FindSchema(elts)
		if sc == nil {
			continue
		}
		if sc.IsVirtual || sc.IsPublic {
			panic(pgerror.Newf(pgcode.InvalidSchemaName,
				"cannot drop schema %q", simpleName(b, sc.SchemaID)))
		}
		if sc.IsTemporary {
			panic(scerrors.NotImplementedErrorf(n, "dropping a temporary schema"))
		}
		if n.DropBehavior == tree.DropCascade {
			dropCascadeDescriptor(b, sc.SchemaID)
		} else {
			dropSchemaRestrict(b, sc.SchemaID)
		}
		b.IncrementSubWorkID()
	}
}

func dropSchemaRestrict(b BuildCtx, id catid.DescID) {
	if !dropRestrict(b, id) {
		return
	}
	backrefs := undroppedBackrefs(b, id)
	if backrefs.IsEmpty() {
		return
	}
	panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
		"schema %q is not empty and CASCADE was not specified", simpleName(b, id)))
}
