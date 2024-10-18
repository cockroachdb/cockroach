// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type supportedAlterSchemaCommand = supportedStatement

// TODO(before merge): I like this pattern. I realize we most likely don't need it everytime, but
// this makes it easy to track everything going on with the DSC in the code. Thoughts?

// supportedAlterSchemaStatements tracks alter schema operations fully supported by
// declarative schema changer. Operations marked as non-fully supported can
// only be with the use_declarative_schema_changer session variable.
var supportedAlterSchemaStatements = map[reflect.Type]supportedAlterSchemaCommand{
	reflect.TypeOf((*tree.AlterSchemaRename)(nil)): {fn: alterSchemaRename, on: true, checks: isV232Active},
	// AlterSchemaOwner
}

// AlterSchema implements ALTER SCHEMA.
func AlterSchema(b BuildCtx, n *tree.AlterSchema) {
	if _, ok := n.Cmd.(*tree.AlterSchemaOwner); ok {
		// TODO(annie): remove this once we allow ALTER SCHEMA ... OWNER TO ...
		panic(scerrors.NotImplementedError(n))
	}
}

func alterSchemaRename(b BuildCtx, n *tree.AlterSchema) {
	schemaElements := b.ResolveSchema(n.Schema, ResolveParams{})

	//if n.Comment == nil {
	//	schemaElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
	//		switch e.(type) {
	//		case *scpb.SchemaComment:
	//			b.Drop(e)
	//			b.LogEventForExistingTarget(e)
	//		}
	//	})
	//} else {
	//	sc := &scpb.SchemaComment{
	//		Comment: *n.Comment,
	//	}
	//	schemaElements.ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e scpb.Element) {
	//		switch t := e.(type) {
	//		case *scpb.Schema:
	//			sc.SchemaID = t.SchemaID
	//		}
	//	})
	//	b.Add(sc)
	//	b.LogEventForExistingTarget(sc)
	//}
}
