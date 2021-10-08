// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestUnresolvedObjectName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	testCases := []struct {
		in, out  string
		expanded string
		err      string
	}{
		{`a`, `a`, `""."".a`, ``},
		{`a.b`, `a.b`, `"".a.b`, ``},
		{`a.b.c`, `a.b.c`, `a.b.c`, ``},
		{`a.b.c.d`, ``, ``, `at or near "\.": syntax error`},
		{`a.""`, ``, ``, `invalid table name: a\.""`},
		{`a.b.""`, ``, ``, `invalid table name: a\.b\.""`},
		{`a.b.c.""`, ``, ``, `at or near "\.": syntax error`},
		{`a."".c`, ``, ``, `invalid table name: a\.""\.c`},

		// CockroachDB extension: empty catalog name.
		{`"".b.c`, `"".b.c`, `"".b.c`, ``},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.y`, ``, ``, `syntax error`},
		{`"user".x.y`, `"user".x.y`, `"user".x.y`, ``},
		{`x.user.y`, `x."user".y`, `x."user".y`, ``},
		{`x.user`, `x."user"`, `"".x."user"`, ``},

		{`foo@bar`, ``, ``, `at or near "@": syntax error`},
		{`test.*`, ``, ``, `at or near "\*": syntax error`},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			name, err := parser.ParseTableName(tc.in)
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}
			tn := name.ToTableName()
			if out := tn.String(); tc.out != out {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.out, out)
			}
			tn.ExplicitSchema = true
			tn.ExplicitCatalog = true
			if out := tn.String(); tc.expanded != out {
				t.Fatalf("%s: expected full %s, but found %s", tc.in, tc.expanded, out)
			}
		})
	}
}

// TestUnresolvedNameAnnotation verifies that we use the annotation
// to produce a fully qualified name when required.
func TestUnresolvedNameAnnotation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	aIdx := tree.AnnotationIdx(1)
	u, err := tree.NewUnresolvedObjectName(1, [3]string{"t"}, aIdx)
	if err != nil {
		t.Fatal(err)
	}
	ann := tree.MakeAnnotations(aIdx)

	expect := func(expected string) {
		t.Helper()
		ctx := tree.NewFmtCtx(
			tree.FmtAlwaysQualifyTableNames,
			tree.FmtAnnotations(&ann),
		)
		ctx.FormatNode(u)
		if actual := ctx.CloseAndGetString(); actual != expected {
			t.Errorf("expected: `%s`, got `%s`", expected, actual)
		}
	}

	// No annotation set.
	expect("t")

	name, err := parser.ParseTableName("db.public.t")
	if err != nil {
		t.Fatal(err)
	}
	tn := name.ToTableName()
	u.SetAnnotation(&ann, &tn)

	expect("db.public.t")

	// No annotation.
	u.AnnIdx = 0
	expect("t")
}
