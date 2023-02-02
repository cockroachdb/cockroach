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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// DropDatabase implements DROP DATABASE.
func DropDatabase(b BuildCtx, n *tree.DropDatabase) {
	elts := b.ResolveDatabase(n.Name, ResolveParams{
		IsExistenceOptional: n.IfExists,
		RequiredPrivilege:   privilege.DROP,
	})
	_, _, db := scpb.FindDatabase(elts)
	if db == nil {
		return
	}
	if string(n.Name) == b.SessionData().Database && b.SessionData().SafeUpdates {
		panic(pgerror.DangerousStatementf("DROP DATABASE on current database"))
	}
	b.IncrementSchemaChangeDropCounter("database")
	// Perform explicit or implicit DROP DATABASE CASCADE.
	if n.DropBehavior == tree.DropCascade || (n.DropBehavior == tree.DropDefault && !b.SessionData().SafeUpdates) {
		dropCascadeDescriptor(b, db.DatabaseID)
		b.LogEventForExistingTarget(db)
		return
	}
	// Otherwise, perform DROP DATABASE RESTRICT.
	if !dropRestrictDescriptor(b, db.DatabaseID) {
		b.LogEventForExistingTarget(db)
		return
	}
	// Implicitly DROP RESTRICT the public schema as well.
	var publicSchemaID catid.DescID
	b.BackReferences(db.DatabaseID).ForEachElementStatus(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Schema:
			if t.IsPublic {
				publicSchemaID = t.SchemaID
			}
		}
	})
	dropRestrictDescriptor(b, publicSchemaID)
	dbBackrefs := undroppedBackrefs(b, db.DatabaseID)
	publicSchemaBackrefs := undroppedBackrefs(b, publicSchemaID)
	if dbBackrefs.IsEmpty() && publicSchemaBackrefs.IsEmpty() {
		return
	}
	// Block DROP if cascade is not set.
	if n.DropBehavior == tree.DropRestrict {
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"database %q is not empty and RESTRICT was specified", simpleName(b, db.DatabaseID)))
	}
	panic(pgerror.DangerousStatementf(
		"DROP DATABASE on non-empty database without explicit CASCADE"))
}
