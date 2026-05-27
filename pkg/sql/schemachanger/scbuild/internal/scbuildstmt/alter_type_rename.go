// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func alterTypeRename(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeRename,
) {
	renameForTypeDesc(b, tn, enumType, enumType.TypeID, enumType.ArrayTypeID, string(t.NewName))
}
