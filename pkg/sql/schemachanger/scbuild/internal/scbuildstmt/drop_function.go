// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func DropFunction(b BuildCtx, n *tree.DropFunction) {
	if n.DropBehavior == tree.DropCascade {
		// TODO(chengxiong): remove this when we allow UDF usage.
		panic(scerrors.NotImplementedErrorf(n, "cascade dropping functions"))
	}
	for _, f := range n.Functions {
		elts := b.ResolveUDF(&f, ResolveParams{
			IsExistenceOptional: n.IfExists,
		})
		_, _, fn := scpb.FindFunction(elts)
		if fn == nil {
			continue
		}
		f.FuncName.ObjectNamePrefix = b.NamePrefix(fn)
		dropRestrictDescriptor(b, fn.FunctionID)
		b.LogEventForExistingTarget(fn)
		b.IncrementSubWorkID()
		b.IncrementSchemaChangeDropCounter("function")
	}
}
