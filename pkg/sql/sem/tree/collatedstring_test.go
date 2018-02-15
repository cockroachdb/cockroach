// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

func TestCastToCollatedString(t *testing.T) {
	cases := []struct {
		typ      coltypes.TCollatedString
		contents string
	}{
		{coltypes.TCollatedString{Locale: "de"}, "test"},
		{coltypes.TCollatedString{Locale: "en"}, "test"},
		{coltypes.TCollatedString{Locale: "en", N: 5}, "test"},
		{coltypes.TCollatedString{Locale: "en", N: 4}, "test"},
		{coltypes.TCollatedString{Locale: "en", N: 3}, "tes"},
	}
	for _, cas := range cases {
		t.Run("", func(t *testing.T) {
			expr := &CastExpr{Expr: NewDString("test"), Type: &cas.typ, SyntaxMode: CastShort}
			typedexpr, err := expr.TypeCheck(&SemaContext{}, types.Any)
			if err != nil {
				t.Fatal(err)
			}
			evalCtx := NewTestingEvalContext(cluster.MakeTestingClusterSettings())
			defer evalCtx.Stop(context.Background())
			val, err := typedexpr.Eval(evalCtx)
			if err != nil {
				t.Fatal(err)
			}
			switch v := val.(type) {
			case *DCollatedString:
				if v.Locale != cas.typ.Locale {
					t.Errorf("expected locale %q but got %q", cas.typ.Locale, v.Locale)
				}
				if v.Contents != cas.contents {
					t.Errorf("expected contents %q but got %q", cas.contents, v.Contents)
				}
			default:
				t.Errorf("expected type *DCollatedString but got %T", v)
			}
		})
	}
}
