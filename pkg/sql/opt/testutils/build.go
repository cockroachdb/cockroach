// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testutils

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optbuilder"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// BuildQuery initializes an optimizer and builds the given sql statement.
func BuildQuery(
	t *testing.T, o *xform.Optimizer, catalog cat.Catalog, evalCtx *tree.EvalContext, sql string,
) {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	semaCtx := tree.MakeSemaContext()
	if err := semaCtx.Placeholders.Init(stmt.NumPlaceholders, nil /* typeHints */); err != nil {
		t.Fatal(err)
	}
	semaCtx.Annotations = tree.MakeAnnotations(stmt.NumAnnotations)
	o.Init(evalCtx, catalog)
	err = optbuilder.New(ctx, &semaCtx, evalCtx, catalog, o.Factory(), stmt.AST).Build()
	if err != nil {
		t.Fatal(err)
	}
}
